package lbspread

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
)

func upstreams(n int) []*reverseproxy.Upstream {
	pool := make([]*reverseproxy.Upstream, n)
	for i := range pool {
		pool[i] = &reverseproxy.Upstream{Host: new(reverseproxy.Host), Dial: fmt.Sprintf("10.0.0.%d:8080", i+1)}
	}
	return pool
}

// builtinHeaderPick mirrors Caddy's hostByHashing (used by lb_policy header).
func builtinHeaderPick(pool []*reverseproxy.Upstream, key string) *reverseproxy.Upstream {
	var best uint64
	var up *reverseproxy.Upstream
	for _, u := range pool {
		if h := hrw(u.String(), key); h > best {
			best, up = h, u
		}
	}
	return up
}

func zeroLoad(*reverseproxy.Upstream) int { return 0 }

func TestPickTopKMatchesBuiltinHeaderPolicyAtK1(t *testing.T) {
	pool := upstreams(7)
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("project-%d.meetploy.app", i)
		got, rank := pickTopK(pool, key, 1, zeroLoad)
		if want := builtinHeaderPick(pool, key); got != want || rank != 0 {
			t.Fatalf("%s: got %s rank %d, want %s", key, got, rank, want)
		}
	}
}

func TestPickTopKPrefersLeastLoadedAmongTopK(t *testing.T) {
	pool := upstreams(6)
	key := "hot.example.com"
	ranked := make([]*reverseproxy.Upstream, 0, 3)
	for k := 1; k <= 3; k++ {
		// The candidate set for k is the k-prefix of the rendezvous ranking.
		load := func(u *reverseproxy.Upstream) int {
			for _, r := range ranked {
				if r == u {
					return 10
				}
			}
			return 1
		}
		got, rank := pickTopK(pool, key, k, load)
		if rank != k-1 {
			t.Fatalf("k=%d: want the only unloaded candidate at rank %d, got rank %d", k, k-1, rank)
		}
		ranked = append(ranked, got)
	}
	if ranked[0] != builtinHeaderPick(pool, key) {
		t.Fatal("rank 0 must be the affinity owner")
	}

	// Equal load stays on the affinity owner.
	got, rank := pickTopK(pool, key, 3, func(*reverseproxy.Upstream) int { return 4 })
	if got != ranked[0] || rank != 0 {
		t.Fatalf("tie must go to rank 0, got rank %d", rank)
	}
}

func TestPickTopKCandidatesAreStableWhenPoolGrows(t *testing.T) {
	// Adding an upstream may insert it into a key's top-k but never reorders
	// the existing members, so a widened key keeps its warm upstreams.
	small := upstreams(5)
	big := append(append([]*reverseproxy.Upstream{}, small...), upstreams(6)[5])
	big[5].Dial = "10.0.0.99:8080"
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("k%d", i)
		before := topK(small, key, 3)
		after := topK(big, key, 4)
		set := map[*reverseproxy.Upstream]bool{}
		for _, u := range after {
			set[u] = true
		}
		for _, u := range before {
			if !set[u] {
				t.Fatalf("%s: %s left the candidate set after scale-up", key, u)
			}
		}
	}
}

func topK(pool []*reverseproxy.Upstream, key string, k int) []*reverseproxy.Upstream {
	var out []*reverseproxy.Upstream
	for r := 0; r < k; r++ {
		load := func(u *reverseproxy.Upstream) int {
			for _, o := range out {
				if o == u {
					return 1
				}
			}
			return 0
		}
		u, _ := pickTopK(pool, key, k, load)
		out = append(out, u)
	}
	return out
}

func newTestTracker(cfg trackerConfig) (*tracker, *time.Time) {
	now := time.Unix(1_700_000_000, 0)
	t := newTracker("X-Key", cfg, nil)
	t.now = func() time.Time { return now }
	return t, &now
}

func TestTrackerScalesUpImmediatelyAndDownOneStepPerCooldown(t *testing.T) {
	tr, now := newTestTracker(trackerConfig{minReplicas: 1, inflightPerReplica: 4, cooldown: time.Minute})
	var releases []func()
	for i := 0; i < 10; i++ {
		_, rel := tr.begin("hot")
		releases = append(releases, rel)
	}
	if got := tr.replicasFor("hot", nil, 6); got != 3 {
		t.Fatalf("10 in-flight / 4 per replica: want 3, got %d", got)
	}
	for _, rel := range releases {
		rel()
		rel() // idempotent
	}
	if got := tr.replicasFor("hot", nil, 6); got != 3 {
		t.Fatalf("must hold during cooldown, got %d", got)
	}
	*now = now.Add(time.Minute)
	if got := tr.replicasFor("hot", nil, 6); got != 2 {
		t.Fatalf("one step down after cooldown, got %d", got)
	}
	if got := tr.replicasFor("hot", nil, 6); got != 2 {
		t.Fatalf("next step needs another cooldown, got %d", got)
	}
	*now = now.Add(time.Minute)
	tr.sweep()
	if got := tr.replicasFor("hot", nil, 6); got != 1 {
		t.Fatalf("sweep steps idle keys down, got %d", got)
	}
}

func TestTrackerRespectsLimitsAndOverrides(t *testing.T) {
	tr, _ := newTestTracker(trackerConfig{
		minReplicas: 1, maxReplicas: 3, inflightPerReplica: 1, cooldown: time.Minute,
		keyMinReplicas: map[string]int{"vip": 2},
	})
	for i := 0; i < 8; i++ {
		tr.begin("hot")
	}
	if got := tr.replicasFor("hot", nil, 10); got != 3 {
		t.Fatalf("max_replicas caps at 3, got %d", got)
	}
	if got := tr.replicasFor("hot", nil, 2); got != 2 {
		t.Fatalf("available upstreams cap at 2, got %d", got)
	}
	if snap := tr.snapshot(); snap.saturated != 1 || snap.top[0].desired != 8 {
		t.Fatalf("hot key must be reported saturated with desired 8: %+v", snap)
	}
	if got := tr.replicasFor("vip", nil, 10); got != 2 {
		t.Fatalf("key_min_replicas floor, got %d", got)
	}
	if got := tr.replicasFor("vip", nil, 1); got != 1 {
		t.Fatalf("floor cannot exceed available upstreams, got %d", got)
	}
}

func TestTrackerSweepDropsIdleKeys(t *testing.T) {
	tr, now := newTestTracker(trackerConfig{minReplicas: 1, inflightPerReplica: 4, cooldown: time.Minute})
	_, rel := tr.begin("a")
	tr.replicasFor("a", nil, 3)
	tr.sweep()
	if tr.snapshot().tracked != 1 {
		t.Fatal("busy key must stay tracked")
	}
	rel()
	*now = now.Add(time.Minute)
	tr.sweep()
	if tr.snapshot().tracked != 0 {
		t.Fatal("idle key at floor must be dropped after cooldown")
	}
}

func provisioned(t *testing.T, s *SpreadSelection) *SpreadSelection {
	t.Helper()
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel)
	if err := s.Provision(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Cleanup() })
	return s
}

func requestWithVars(key string) (*http.Request, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, caddyhttp.VarsCtxKey, map[string]any{})
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	req.Header.Set("X-Spread-Test", key)
	return req, cancel
}

func TestSelectCountsOncePerRequestAndReleasesOnCompletion(t *testing.T) {
	s := provisioned(t, &SpreadSelection{Field: "X-Spread-Test", InflightPerReplica: 2})
	pool := upstreams(4)

	req, done := requestWithVars("busy")
	for i := 0; i < 3; i++ { // initial attempt + two retries
		if s.Select(pool, req, nil) == nil {
			t.Fatal("no upstream")
		}
	}
	ks := s.tracker.state("busy")
	if got := ks.inflight.Load(); got != 1 {
		t.Fatalf("retries must not recount, in-flight %d", got)
	}
	done()
	deadline := time.Now().Add(time.Second)
	for ks.inflight.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("in-flight not released when the request context ended")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSelectSpreadsHotKeyAndKeepsColdKeyPinned(t *testing.T) {
	s := provisioned(t, &SpreadSelection{Field: "X-Spread-Test", InflightPerReplica: 2, MaxReplicas: 3})
	pool := upstreams(6)
	var mu sync.Mutex
	inflight := map[*reverseproxy.Upstream]int{}
	s.load = func(u *reverseproxy.Upstream) int {
		mu.Lock()
		defer mu.Unlock()
		return inflight[u]
	}

	seen := map[*reverseproxy.Upstream]bool{}
	var cancels []context.CancelFunc
	for i := 0; i < 12; i++ {
		req, cancel := requestWithVars("hot")
		cancels = append(cancels, cancel)
		up := s.Select(pool, req, nil)
		mu.Lock()
		inflight[up]++
		mu.Unlock()
		seen[up] = true
	}
	if len(seen) != 3 {
		t.Fatalf("hot key with 12 in-flight / 2 per replica, max 3: want 3 upstreams, got %d", len(seen))
	}

	owner := builtinHeaderPick(pool, "cold")
	for i := 0; i < 20; i++ {
		req, cancel := requestWithVars("cold")
		if got := s.Select(pool, req, nil); got != owner {
			t.Fatalf("cold key left its affinity owner")
		}
		cancel()
	}
	for _, c := range cancels {
		c()
	}
}

func TestSelectIgnoresUpgradesAndEventStreamsForDemand(t *testing.T) {
	s := provisioned(t, &SpreadSelection{Field: "X-Spread-Test", InflightPerReplica: 1})
	pool := upstreams(3)
	for _, h := range [][2]string{{"Upgrade", "websocket"}, {"Accept", "text/event-stream"}} {
		req, cancel := requestWithVars("ws")
		defer cancel()
		req.Header.Set(h[0], h[1])
		s.Select(pool, req, nil)
	}
	if got := s.tracker.state("ws").inflight.Load(); got != 0 {
		t.Fatalf("long-lived requests must not count, in-flight %d", got)
	}
}

func TestSelectFallsBackWithoutKey(t *testing.T) {
	s := provisioned(t, &SpreadSelection{Field: "X-Spread-Test"})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if s.Select(upstreams(2), req, nil) == nil {
		t.Fatal("fallback must select an upstream")
	}
}

func TestProvisionValidation(t *testing.T) {
	for name, s := range map[string]*SpreadSelection{
		"no field":       {},
		"max below min":  {Field: "X", MinReplicas: 3, MaxReplicas: 2},
		"bad inflight":   {Field: "X", InflightPerReplica: -1},
		"bad key floor":  {Field: "X", KeyMinReplicas: map[string]int{"a": 0}},
		"negative floor": {Field: "X", MinReplicas: -1},
	} {
		ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
		if err := s.Provision(ctx); err == nil {
			t.Errorf("%s: want error", name)
		}
		cancel()
	}
}

func TestUnmarshalCaddyfile(t *testing.T) {
	d := caddyfile.NewTestDispenser(`spread X-Shard-Key {
		min_replicas 1
		max_replicas 4
		inflight_per_replica 24
		cooldown 10m
		key_min_replicas hot.example.com 2
		fallback first
	}`)
	var s SpreadSelection
	if err := s.UnmarshalCaddyfile(d); err != nil {
		t.Fatal(err)
	}
	if s.Field != "X-Shard-Key" || s.MinReplicas != 1 || s.MaxReplicas != 4 || s.InflightPerReplica != 24 ||
		time.Duration(s.Cooldown) != 10*time.Minute || s.KeyMinReplicas["hot.example.com"] != 2 ||
		string(s.FallbackRaw) != `{"policy":"first"}` {
		t.Fatalf("unexpected config: %+v fallback=%s", s, s.FallbackRaw)
	}

	for _, bad := range []string{
		`spread`,
		`spread A B`,
		`spread A { max_replicas }`,
		`spread A { max_replicas x }`,
		`spread A { cooldown forever }`,
		`spread A { nope 1 }`,
		`spread A { min_replicas 1 2 }`,
	} {
		var s SpreadSelection
		if err := s.UnmarshalCaddyfile(caddyfile.NewTestDispenser(bad)); err == nil {
			t.Errorf("%q: want error", bad)
		}
	}
}
