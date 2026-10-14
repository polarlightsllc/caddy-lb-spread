package lbspread

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	_ "github.com/caddyserver/caddy/v2/modules/standard"
)

type blockingUpstream struct {
	*httptest.Server
	name    string
	arrived *atomic.Int64
	release chan struct{}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// TestCaddyEndToEnd runs a real Caddy server with the spread policy in front
// of blocking upstreams: a hot key widens across several upstreams, a cold key
// stays on one, and the metrics endpoint exposes the spread state.
func TestCaddyEndToEnd(t *testing.T) {
	var arrived atomic.Int64
	release := make(chan struct{})
	var ups []*blockingUpstream
	for i := 0; i < 5; i++ {
		u := &blockingUpstream{name: fmt.Sprintf("u%d", i), arrived: &arrived, release: release}
		u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Block") != "" {
				arrived.Add(1)
				<-release
			}
			w.Header().Set("X-Upstream", u.name)
		}))
		defer u.Close()
		ups = append(ups, u)
	}

	var upstreamCfg []map[string]any
	for _, u := range ups {
		upstreamCfg = append(upstreamCfg, map[string]any{"dial": strings.TrimPrefix(u.URL, "http://")})
	}
	port := freePort(t)
	cfg := map[string]any{
		"admin": map[string]any{"disabled": true},
		"apps": map[string]any{"http": map[string]any{
			"servers": map[string]any{"s": map[string]any{
				"listen": []string{fmt.Sprintf("127.0.0.1:%d", port)},
				"routes": []any{
					map[string]any{
						"match":  []any{map[string]any{"path": []string{"/metrics"}}},
						"handle": []any{map[string]any{"handler": "metrics"}},
					},
					map[string]any{"handle": []any{map[string]any{
						"handler":   "reverse_proxy",
						"upstreams": upstreamCfg,
						"load_balancing": map[string]any{"selection_policy": map[string]any{
							"policy":               "spread",
							"field":                "X-Shard-Key",
							"inflight_per_replica": 2,
							"max_replicas":         3,
						}},
					}}},
				},
			}},
		}},
	}
	raw, _ := json.Marshal(cfg)
	if err := caddy.Load(raw, true); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = caddy.Stop() }()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	get := func(key string, block bool) string {
		req, _ := http.NewRequest(http.MethodGet, base+"/", nil)
		req.Header.Set("X-Shard-Key", key)
		if block {
			req.Header.Set("X-Block", "1")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
			return ""
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.Header.Get("X-Upstream")
	}

	const hot = 12
	var wg sync.WaitGroup
	var mu sync.Mutex
	hotUpstreams := map[string]int{}
	for i := 0; i < hot; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := get("hot.example.com", true)
			mu.Lock()
			hotUpstreams[name]++
			mu.Unlock()
		}()
		// Serialize arrivals so each selection sees the previous in-flight load.
		deadline := time.Now().Add(5 * time.Second)
		for arrived.Load() < int64(i+1) {
			if time.Now().After(deadline) {
				t.Fatalf("request %d never reached an upstream", i)
			}
			time.Sleep(time.Millisecond)
		}
	}

	metrics := func() string {
		resp, err := http.Get(base + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}
	m := metrics()
	for _, want := range []string{
		`caddy_lb_spread_spread_keys{field="X-Shard-Key"} 1`,
		`caddy_lb_spread_key_replicas{field="X-Shard-Key",key="hot.example.com"} 3`,
		`caddy_lb_spread_key_inflight{field="X-Shard-Key",key="hot.example.com"} 12`,
		`caddy_lb_spread_saturated_keys{field="X-Shard-Key"} 1`,
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}

	close(release)
	wg.Wait()
	if len(hotUpstreams) != 3 {
		t.Fatalf("hot key: want 3 upstreams (max_replicas), got %v", hotUpstreams)
	}

	cold := map[string]int{}
	for i := 0; i < 20; i++ {
		cold[get("cold.example.com", false)]++
	}
	if len(cold) != 1 {
		t.Fatalf("cold key must stay on one upstream, got %v", cold)
	}
	if !strings.Contains(metrics(), `caddy_lb_spread_selections_total{field="X-Shard-Key",rank="1"}`) {
		t.Error("selections_total by rank missing")
	}
}
