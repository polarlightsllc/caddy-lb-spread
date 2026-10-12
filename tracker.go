package lbspread

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// trackerConfig holds the knobs that decide how many upstreams a key spreads over.
type trackerConfig struct {
	minReplicas        int
	maxReplicas        int // 0 = bounded only by available upstreams
	inflightPerReplica int
	cooldown           time.Duration
	keyMinReplicas     map[string]int
}

func (c trackerConfig) minFor(key string) int {
	m := c.minReplicas
	if o, ok := c.keyMinReplicas[key]; ok && o > m {
		m = o
	}
	return m
}

// desiredFor converts in-flight demand into a replica count before any
// upstream-count bound is applied.
func (c trackerConfig) desiredFor(key string, inflight int64) int {
	d := int(math.Ceil(float64(inflight) / float64(c.inflightPerReplica)))
	if m := c.minFor(key); d < m {
		d = m
	}
	return d
}

func (c trackerConfig) limitFor(available int) int {
	limit := available
	if c.maxReplicas > 0 && c.maxReplicas < limit {
		limit = c.maxReplicas
	}
	if limit < 1 {
		limit = 1
	}
	return limit
}

// keyState is the per-key load and replica decision. inflight and lastSeen are
// lock-free on the request path; the replica decision is guarded by mu.
type keyState struct {
	inflight atomic.Int64
	lastSeen atomic.Int64 // unix nanos

	mu           sync.Mutex
	replicas     int
	desired      int // uncapped demand at last evaluation
	limit        int // effective cap at last evaluation
	lastDemandAt time.Time
}

// tracker owns the key table for one routing field. It is shared through a
// caddy.UsagePool so the table survives config reloads.
type tracker struct {
	field string

	cfgMu  sync.RWMutex
	cfg    trackerConfig
	logger *zap.Logger

	mu   sync.RWMutex
	keys map[string]*keyState

	now func() time.Time

	selections       [rankBuckets]atomic.Uint64
	spreadSelections atomic.Uint64
	scaleUps         atomic.Uint64
	scaleDowns       atomic.Uint64

	stop     chan struct{}
	stopOnce sync.Once
}

const (
	rankBuckets   = 9 // ranks 0..7 plus "8+"
	sweepInterval = 10 * time.Second
	maxKeyMetrics = 20
)

func newTracker(field string, cfg trackerConfig, logger *zap.Logger) *tracker {
	return &tracker{
		field:  field,
		cfg:    cfg,
		logger: logger,
		keys:   make(map[string]*keyState),
		now:    time.Now,
		stop:   make(chan struct{}),
	}
}

func (t *tracker) settings() (trackerConfig, *zap.Logger) {
	t.cfgMu.RLock()
	defer t.cfgMu.RUnlock()
	return t.cfg, t.logger
}

func (t *tracker) setConfig(cfg trackerConfig, logger *zap.Logger) {
	t.cfgMu.Lock()
	defer t.cfgMu.Unlock()
	t.cfg = cfg
	if logger != nil {
		t.logger = logger
	}
}

func (t *tracker) state(key string) *keyState {
	t.mu.RLock()
	s := t.keys[key]
	t.mu.RUnlock()
	if s != nil {
		return s
	}
	cfg, _ := t.settings()
	t.mu.Lock()
	defer t.mu.Unlock()
	if s = t.keys[key]; s == nil {
		s = &keyState{replicas: cfg.minFor(key), lastDemandAt: t.now()}
		s.lastSeen.Store(t.now().UnixNano())
		t.keys[key] = s
	}
	return s
}

// begin records one in-flight request for key and returns an idempotent release.
func (t *tracker) begin(key string) (*keyState, func()) {
	s := t.state(key)
	s.inflight.Add(1)
	s.lastSeen.Store(t.now().UnixNano())
	var once sync.Once
	return s, func() { once.Do(func() { s.inflight.Add(-1) }) }
}

// replicasFor evaluates key's demand and returns how many top-ranked upstreams
// it may use while `available` upstreams are selectable.
func (t *tracker) replicasFor(key string, s *keyState, available int) int {
	if s == nil {
		s = t.state(key)
	}
	cfg, logger := t.settings()
	return t.evaluate(key, s, cfg, logger, available, t.now())
}

// evaluate scales up to demand immediately and steps down one replica per
// cooldown during which demand stayed below the current replica count.
func (t *tracker) evaluate(key string, s *keyState, cfg trackerConfig, logger *zap.Logger, available int, now time.Time) int {
	limit := cfg.limitFor(available)
	inflight := s.inflight.Load()
	desired := cfg.desiredFor(key, inflight)
	target := min(desired, limit)
	floor := min(cfg.minFor(key), limit)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.desired = desired
	s.limit = limit
	from := s.replicas
	if s.replicas > limit {
		s.replicas = limit
	}
	switch {
	case target >= s.replicas:
		s.lastDemandAt = now
		s.replicas = target
	case now.Sub(s.lastDemandAt) >= cfg.cooldown:
		s.replicas--
		s.lastDemandAt = now
	}
	if s.replicas < floor {
		s.replicas = floor
	}
	if from != s.replicas {
		up := s.replicas > from
		if up {
			t.scaleUps.Add(1)
		} else {
			t.scaleDowns.Add(1)
		}
		if logger != nil {
			logger.Info("spread replicas changed",
				zap.String("field", t.field),
				zap.String("key", key),
				zap.Int("from", from),
				zap.Int("to", s.replicas),
				zap.Int64("inflight", inflight),
				zap.Int("desired", desired),
				zap.Int("limit", limit),
			)
		}
	}
	return s.replicas
}

func (t *tracker) recordSelection(rank, replicas int) {
	if rank >= rankBuckets-1 {
		rank = rankBuckets - 1
	}
	t.selections[rank].Add(1)
	if replicas > 1 {
		t.spreadSelections.Add(1)
	}
}

// sweep re-evaluates every key so idle keys still step down, then drops keys
// that are idle, at their floor, and unseen for a full cooldown.
func (t *tracker) sweep() {
	cfg, logger := t.settings()
	now := t.now()
	t.mu.RLock()
	keys := make([]string, 0, len(t.keys))
	states := make([]*keyState, 0, len(t.keys))
	for k, s := range t.keys {
		keys = append(keys, k)
		states = append(states, s)
	}
	t.mu.RUnlock()

	var drop []string
	for i, s := range states {
		s.mu.Lock()
		limit := s.limit
		s.mu.Unlock()
		replicas := t.evaluate(keys[i], s, cfg, logger, limit, now)
		idleFor := now.Sub(time.Unix(0, s.lastSeen.Load()))
		if s.inflight.Load() == 0 && replicas <= cfg.minFor(keys[i]) && idleFor >= cfg.cooldown {
			drop = append(drop, keys[i])
		}
	}
	if len(drop) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, k := range drop {
		if s := t.keys[k]; s != nil && s.inflight.Load() == 0 {
			delete(t.keys, k)
		}
	}
}

func (t *tracker) run() {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.sweep()
		case <-t.stop:
			return
		}
	}
}

// Destruct implements caddy.Destructor; it runs when the last loaded config
// using this field is unloaded.
func (t *tracker) Destruct() error {
	t.stopOnce.Do(func() { close(t.stop) })
	return nil
}

type keySnapshot struct {
	key      string
	inflight int64
	replicas int
	desired  int
	limit    int
}

type trackerSnapshot struct {
	tracked   int
	spread    int
	saturated int
	top       []keySnapshot
}

// snapshot reports aggregate key counts plus the hottest spread or saturated
// keys, capped at maxKeyMetrics to bound label cardinality.
func (t *tracker) snapshot() trackerSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	snap := trackerSnapshot{tracked: len(t.keys)}
	var hot []keySnapshot
	for k, s := range t.keys {
		s.mu.Lock()
		ks := keySnapshot{key: k, inflight: s.inflight.Load(), replicas: s.replicas, desired: s.desired, limit: s.limit}
		s.mu.Unlock()
		saturated := ks.limit > 0 && ks.desired > ks.limit
		if ks.replicas > 1 {
			snap.spread++
		}
		if saturated {
			snap.saturated++
		}
		if ks.replicas > 1 || saturated {
			hot = append(hot, ks)
		}
	}
	sort.Slice(hot, func(i, j int) bool {
		if hot[i].inflight != hot[j].inflight {
			return hot[i].inflight > hot[j].inflight
		}
		return hot[i].key < hot[j].key
	})
	if len(hot) > maxKeyMetrics {
		hot = hot[:maxKeyMetrics]
	}
	snap.top = hot
	return snap
}
