# caddy-lb-spread

A Caddy `reverse_proxy` selection policy that keeps each key, such as a Host,
on one upstream until that key gets hot. Once it does, the policy spreads the
key over the next few upstreams in that key's own rendezvous ranking.

```caddyfile
request_header X-Shard-Key {host}
reverse_proxy {
	dynamic a backend-headless.default.svc.cluster.local 8080
	lb_policy spread X-Shard-Key {
		max_replicas         4
		inflight_per_replica 16
		cooldown             10m
	}
}
```

## Why

`lb_policy header` (rendezvous hashing) gives a key cache affinity: one
upstream holds that key's warm state. The downside is that a single hot key can
never use more than one upstream, and adding upstreams doesn't help it. Round-robin
fixes the hot key, but then every upstream has to keep every key warm.

`spread` sits between the two:

- Each key `K` ranks upstreams by `xxhash(upstream + K)`. This is the same score
  Caddy's built-in hash policies use.
- `K` may use the top `k` upstreams in that ranking. Each request goes to the
  least-busy of them (by Caddy's in-flight count per upstream). A tie goes to the
  higher-ranked one.
- `k = ceil(inflight(K) / inflight_per_replica)`. It is bounded below by
  `min_replicas` (or that key's `key_min_replicas` value) and above by
  `max_replicas` and the number of available upstreams.
- `k` rises immediately. It falls by one step per `cooldown` while demand stays
  lower, so short dips don't make the key's extra upstreams go cold.

With `k = 1` the policy picks exactly what `lb_policy header <field>` picks.
Switching an existing config to `spread` therefore remaps nothing. Because the
ranking is stable, a widened key keeps reusing the same few warm upstreams, and
adding an upstream never reorders a key's existing candidates.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `<field>` | required | Request header used as the key. `Host` reads `req.Host`. |
| `min_replicas` | `1` | Replicas every key may use. |
| `max_replicas` | `0` (all) | Cap per key. |
| `inflight_per_replica` | `16` | In-flight requests **on this proxy** per replica before a key widens. |
| `cooldown` | `5m` | How long demand must stay lower before a key narrows by one replica. |
| `key_min_replicas <key> <n>` | – | Per-key floor. Can be repeated. |
| `fallback <policy>` | `least_conn` | Used when the key is empty. |

Notes:

- Demand is counted per proxy instance, once per request (retries don't
  count again). It is released when the request finishes. WebSocket upgrades
  and `text/event-stream` requests stay open while idle, so they don't count
  toward demand.
- Each proxy decides `k` on its own. The rankings agree across proxies, so
  every proxy widens a key onto the same upstreams.
- Key state is shared by `field` across sites and survives config reloads. If
  several sites use the same field with different options, the most recently
  provisioned options apply.

## Metrics

These are exported on Caddy's metrics registry when `metrics` is enabled:

| Metric | Type | Labels |
| --- | --- | --- |
| `caddy_lb_spread_tracked_keys` | gauge | `field` |
| `caddy_lb_spread_spread_keys` | gauge | `field` (keys using more than one replica) |
| `caddy_lb_spread_saturated_keys` | gauge | `field` (keys whose demand exceeds `max_replicas` or the available upstreams) |
| `caddy_lb_spread_key_replicas` | gauge | `field`, `key` |
| `caddy_lb_spread_key_desired_replicas` | gauge | `field`, `key` (demand before caps) |
| `caddy_lb_spread_key_inflight` | gauge | `field`, `key` |
| `caddy_lb_spread_selections_total` | counter | `field`, `rank` (`0` is the affinity owner, then `1`–`7`, then `8+`) |
| `caddy_lb_spread_spread_selections_total` | counter | `field` |
| `caddy_lb_spread_scale_events_total` | counter | `field`, `direction` |

The `key_*` series cover only the 20 keys with the most in-flight requests
among those that are spread or saturated, so label cardinality stays bounded.
Every change to a key's replica count is also logged at info level as
`spread replicas changed`.

## Build

```sh
xcaddy build --with github.com/polarlightsllc/caddy-lb-spread
```
