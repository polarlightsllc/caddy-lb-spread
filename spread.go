// Package lbspread provides the `spread` reverse_proxy selection policy: a
// rendezvous (HRW) hash on a request field that widens a hot key across the
// top-k ranked upstreams in proportion to its in-flight requests.
//
// With k=1 the selection is identical to Caddy's built-in `header` policy, so
// switching policies does not remap any key.
package lbspread

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/cespare/xxhash/v2"
)

func init() {
	caddy.RegisterModule(SpreadSelection{})
}

const (
	defaultInflightPerReplica = 16
	defaultCooldown           = 5 * time.Minute
	countedVar                = "lb_spread_counted"
)

// trackers holds one key table per routing field, shared across config
// reloads so a reload does not collapse every hot key back to one upstream.
var trackers = caddy.NewUsagePool()

// SpreadSelection hashes Field with rendezvous hashing and routes each key to
// the least-loaded of its top-k ranked available upstreams, where k grows with
// the key's in-flight requests on this proxy.
type SpreadSelection struct {
	// Request header whose value is the routing key. "Host" reads req.Host.
	Field string `json:"field,omitempty"`

	// Replicas every key may use. Default 1 (pure affinity).
	MinReplicas int `json:"min_replicas,omitempty"`

	// Upper bound on replicas per key. 0 means all available upstreams.
	MaxReplicas int `json:"max_replicas,omitempty"`

	// In-flight requests on this proxy per replica before a key widens.
	// Default 16.
	InflightPerReplica int `json:"inflight_per_replica,omitempty"`

	// Time demand must stay below the current replica count before a key
	// narrows by one replica. Default 5m.
	Cooldown caddy.Duration `json:"cooldown,omitempty"`

	// Per-key replica floors, e.g. a known-hot host.
	KeyMinReplicas map[string]int `json:"key_min_replicas,omitempty"`

	// Policy used when the key is empty. Default least_conn.
	FallbackRaw json.RawMessage `json:"fallback,omitempty" caddy:"namespace=http.reverse_proxy.selection_policies inline_key=policy"`

	fallback reverseproxy.Selector
	tracker  *tracker
	load     func(*reverseproxy.Upstream) int
}

// CaddyModule returns the Caddy module information.
func (SpreadSelection) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.reverse_proxy.selection_policies.spread",
		New: func() caddy.Module { return new(SpreadSelection) },
	}
}

// Provision validates the config and attaches the shared tracker.
func (s *SpreadSelection) Provision(ctx caddy.Context) error {
	if s.Field == "" {
		return errors.New("spread: field is required")
	}
	if s.MinReplicas == 0 {
		s.MinReplicas = 1
	}
	if s.MinReplicas < 1 {
		return fmt.Errorf("spread: min_replicas must be >= 1, got %d", s.MinReplicas)
	}
	if s.MaxReplicas < 0 || (s.MaxReplicas > 0 && s.MaxReplicas < s.MinReplicas) {
		return fmt.Errorf("spread: max_replicas must be 0 or >= min_replicas, got %d", s.MaxReplicas)
	}
	if s.InflightPerReplica == 0 {
		s.InflightPerReplica = defaultInflightPerReplica
	}
	if s.InflightPerReplica < 1 {
		return fmt.Errorf("spread: inflight_per_replica must be >= 1, got %d", s.InflightPerReplica)
	}
	if s.Cooldown == 0 {
		s.Cooldown = caddy.Duration(defaultCooldown)
	}
	if s.Cooldown < 0 {
		return errors.New("spread: cooldown must not be negative")
	}
	for k, n := range s.KeyMinReplicas {
		if n < 1 {
			return fmt.Errorf("spread: key_min_replicas for %q must be >= 1, got %d", k, n)
		}
	}

	if s.FallbackRaw == nil {
		s.FallbackRaw = caddyconfig.JSONModuleObject(reverseproxy.LeastConnSelection{}, "policy", "least_conn", nil)
	}
	mod, err := ctx.LoadModule(s, "FallbackRaw")
	if err != nil {
		return fmt.Errorf("spread: loading fallback selection policy: %w", err)
	}
	s.fallback = mod.(reverseproxy.Selector)

	cfg := s.trackerConfig()
	logger := ctx.Logger()
	val, loaded, err := trackers.LoadOrNew(s.Field, func() (caddy.Destructor, error) {
		t := newTracker(s.Field, cfg, logger)
		go t.run()
		return t, nil
	})
	if err != nil {
		return err
	}
	s.tracker = val.(*tracker)
	if loaded {
		s.tracker.setConfig(cfg, logger)
	}
	s.load = func(u *reverseproxy.Upstream) int { return u.NumRequests() }

	return registerMetrics(ctx)
}

// Cleanup releases this config's reference to the shared tracker.
func (s *SpreadSelection) Cleanup() error {
	if s.tracker == nil {
		return nil
	}
	_, err := trackers.Delete(s.Field)
	return err
}

func (s *SpreadSelection) trackerConfig() trackerConfig {
	return trackerConfig{
		minReplicas:        s.MinReplicas,
		maxReplicas:        s.MaxReplicas,
		inflightPerReplica: s.InflightPerReplica,
		cooldown:           time.Duration(s.Cooldown),
		keyMinReplicas:     s.KeyMinReplicas,
	}
}

// Select returns an available upstream, if any.
func (s *SpreadSelection) Select(pool reverseproxy.UpstreamPool, req *http.Request, w http.ResponseWriter) *reverseproxy.Upstream {
	key := s.keyFor(req)
	if key == "" {
		return s.fallback.Select(pool, req, w)
	}
	available := make([]*reverseproxy.Upstream, 0, len(pool))
	for _, up := range pool {
		if up != nil && up.Available() {
			available = append(available, up)
		}
	}
	if len(available) == 0 {
		return nil
	}
	ks := s.count(req, key)
	k := s.tracker.replicasFor(key, ks, len(available))
	up, rank := pickTopK(available, key, k, s.load)
	s.tracker.recordSelection(rank, k)
	return up
}

func (s *SpreadSelection) keyFor(req *http.Request) string {
	if s.Field == "Host" {
		return req.Host
	}
	return req.Header.Get(s.Field)
}

// count adds the request to the key's in-flight total once per request (not
// per retry) and releases it when the request context ends. Upgrades and
// event streams are long-lived and idle, so they never drive widening.
func (s *SpreadSelection) count(req *http.Request, key string) *keyState {
	if !countable(req) {
		return nil
	}
	ctx := req.Context()
	if caddyhttp.GetVar(ctx, countedVar) != nil {
		return nil
	}
	ks, release := s.tracker.begin(key)
	caddyhttp.SetVar(ctx, countedVar, true)
	context.AfterFunc(ctx, release)
	return ks
}

func countable(req *http.Request) bool {
	if req.Header.Get("Upgrade") != "" {
		return false
	}
	return !strings.Contains(req.Header.Get("Accept"), "text/event-stream")
}

type candidate struct {
	up    *reverseproxy.Upstream
	score uint64
}

// pickTopK ranks upstreams by rendezvous score for key, keeps the top k, and
// returns the least-loaded one; ties go to the higher-ranked upstream. The
// score matches Caddy's built-in hash policies, so rank 0 is the upstream the
// `header` policy would pick.
func pickTopK(available []*reverseproxy.Upstream, key string, k int, load func(*reverseproxy.Upstream) int) (*reverseproxy.Upstream, int) {
	if k < 1 {
		k = 1
	}
	if k > len(available) {
		k = len(available)
	}
	top := make([]candidate, 0, k)
	for _, up := range available {
		c := candidate{up: up, score: hrw(up.String(), key)}
		i := len(top)
		for i > 0 && c.score > top[i-1].score {
			i--
		}
		if i >= k {
			continue
		}
		if len(top) < k {
			top = append(top, candidate{})
		}
		copy(top[i+1:], top[i:len(top)-1])
		top[i] = c
	}
	if len(top) == 1 {
		return top[0].up, 0
	}
	best, bestLoad := 0, load(top[0].up)
	for i := 1; i < len(top); i++ {
		if l := load(top[i].up); l < bestLoad {
			best, bestLoad = i, l
		}
	}
	return top[best].up, best
}

func hrw(upstream, key string) uint64 {
	h := xxhash.New()
	_, _ = h.WriteString(upstream)
	_, _ = h.WriteString(key)
	return h.Sum64()
}

// UnmarshalCaddyfile sets up the module from Caddyfile tokens:
//
//	lb_policy spread <field> {
//		min_replicas         <n>
//		max_replicas         <n>
//		inflight_per_replica <n>
//		cooldown             <duration>
//		key_min_replicas     <key> <n>
//		fallback             <policy>
//	}
func (s *SpreadSelection) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // policy name
	if !d.NextArg() {
		return d.ArgErr()
	}
	s.Field = d.Val()
	if d.NextArg() {
		return d.ArgErr()
	}
	intArg := func() (int, error) {
		if !d.NextArg() {
			return 0, d.ArgErr()
		}
		n, err := strconv.Atoi(d.Val())
		if err != nil {
			return 0, d.Errf("invalid integer %q: %v", d.Val(), err)
		}
		return n, nil
	}
	for d.NextBlock(0) {
		var err error
		switch d.Val() {
		case "min_replicas":
			s.MinReplicas, err = intArg()
		case "max_replicas":
			s.MaxReplicas, err = intArg()
		case "inflight_per_replica":
			s.InflightPerReplica, err = intArg()
		case "cooldown":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, perr := caddy.ParseDuration(d.Val())
			if perr != nil {
				return d.Errf("invalid cooldown %q: %v", d.Val(), perr)
			}
			s.Cooldown = caddy.Duration(dur)
		case "key_min_replicas":
			if !d.NextArg() {
				return d.ArgErr()
			}
			key := d.Val()
			n, ierr := intArg()
			if ierr != nil {
				return ierr
			}
			if s.KeyMinReplicas == nil {
				s.KeyMinReplicas = make(map[string]int)
			}
			s.KeyMinReplicas[key] = n
		case "fallback":
			if !d.NextArg() {
				return d.ArgErr()
			}
			if s.FallbackRaw != nil {
				return d.Err("fallback selection policy already specified")
			}
			name := d.Val()
			unm, uerr := caddyfile.UnmarshalModule(d, "http.reverse_proxy.selection_policies."+name)
			if uerr != nil {
				return uerr
			}
			sel, ok := unm.(reverseproxy.Selector)
			if !ok {
				return d.Errf("module %s (%T) is not a reverseproxy.Selector", name, unm)
			}
			s.FallbackRaw = caddyconfig.JSONModuleObject(sel, "policy", name, nil)
		default:
			return d.Errf("unrecognized option '%s'", d.Val())
		}
		if err != nil {
			return err
		}
		if d.NextArg() {
			return d.ArgErr()
		}
	}
	return nil
}

// Interface guards
var (
	_ reverseproxy.Selector = (*SpreadSelection)(nil)
	_ caddy.Provisioner     = (*SpreadSelection)(nil)
	_ caddy.CleanerUpper    = (*SpreadSelection)(nil)
	_ caddyfile.Unmarshaler = (*SpreadSelection)(nil)
)
