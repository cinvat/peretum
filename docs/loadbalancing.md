---
label: Load balancing
icon: arrow-switch
order: 600
---

# Load balancing

Every target has its own load balancer over its `upstreams` list. The balancer is
per target, not global: two targets pointing at the same origin balance
independently.

## One pick, no retry

The balancer is consulted **once** per request, and the request goes to whichever
upstream it returns. There is no second attempt if that upstream fails.

This is the most important thing to know about the subsystem. When an origin goes
away, recovery depends entirely on health marking rather than on a retry: the
failure marks the upstream unhealthy, the next request is routed elsewhere, and
the upstream is re-selected once it proves healthy again. A single request in
flight during a failure is failed rather than re-sent.

Set `upstreams` with at least two entries if you need tolerance to a backend
going away.

## Choosing an algorithm

Set `lb_algorithm` on the target. An unrecognised value silently falls back to
`round_robin` — there is no warning, so a typo is invisible until you read the
source.

| Value | Behavior |
| --- | --- |
| `round_robin` / `rr` / *(empty)* | Equal distribution. Default. |
| `weighted_rr` / `wrr` | Respects `weight`. |
| `maglev` | Consistent hashing, so a backend change reshuffles few keys. |
| `least_conn` / `least_connections` | Fewest active requests. **See the caveat below.** |

## How each algorithm behaves

### `round_robin`

A single atomic counter advances once per pick and the slot it lands on is used
if healthy. Unhealthy upstreams are skipped by scanning forward up to the pool
size, so a burst of failures does not skip the whole pool.

### `weighted_rr`

Each upstream carries a counter initialised to its `weight`, decremented on every
pick. When any counter reaches zero, **all healthy upstreams are refilled** to
their weights; unhealthy ones are left alone, which is what keeps them out of the
rotation. A `weight` of `0` or less is treated as `1`.

Because the refill is triggered by whichever counter empties, the distribution
approximates the weight ratio rather than tracking it exactly.

### `maglev`

Builds a 65537-slot lookup table and maps a request to a slot with
`CRC32(path + rawQuery)`.

Two details are easy to get wrong:

- The key is the **request URI**, not the upstream set, so a single target routes
  by path. Two different keys can land on the same backend, and one key always
  lands on the same backend — that is the point of the algorithm.
- The table is **rebuilt on every health change**, which is 65537 slots of work on
  the goroutine that observed the change. On the request path that is every
  health flip. It is not currently a problem in practice, but it is the reason
  health flips are not free under `maglev`.

If the slot's upstream is unhealthy, the first healthy upstream is used instead.
If every upstream is unhealthy, the previous table is kept and selection falls
back to the shared behaviour below.

### `least_conn`

**Caveat: this algorithm does not currently balance anything.** It compares
per-upstream active-request counters, but nothing on the request path updates
those counters, so they all stay at zero and every request goes to the first
healthy upstream.

To spread load across backends today, use `weighted_rr` for proportional
splitting or `maglev` for key-stable splitting.

## Health tracking

### Passive (always on)

An upstream is marked **unhealthy** when a request to it fails at the transport
level, and **healthy** again as soon as one succeeds.

Two behaviours worth planning around:

- **Any completed response counts as healthy, including `5xx`.** An origin
  returning errors is considered reachable, so it stays in rotation. Passive
  tracking answers "is the backend up", not "is it serving correctly".
- **A response larger than `max_body_size` marks the upstream unhealthy.** That
  is a client-side limit, not an origin fault, but it is reported as a transport
  failure. If you raise `max_body_size`, previously-poisoned upstreams recover on
  their next success.

Health is **per load balancer**, so it is reset when the target is rebuilt — a
config reload or a cluster update marks every upstream healthy again. A reload is
therefore a way to force re-probing.

### When everything is unhealthy

Rather than fail closed, the balancer returns the upstream whose health has gone
unconfirmed for the longest time. A dead upstream keeps being retried by
subsequent requests, so it can return to rotation the moment it recovers instead
of staying excluded until the process restarts.

This only applies when the target has at least one upstream configured. A target
with an empty `upstreams` list returns no upstream, and the request fails with
`no healthy upstreams`.

### Active health checks

Enable per-upstream HTTP probes to recover failed upstreams without waiting for a
user request to hit them:

```yaml
upstreams:
  - url: "http://backend1:8080"
    weight: 1
    health_check:
      path: "/healthz"          # required to enable
      interval: "30s"           # default 10s
      timeout: "5s"             # default 3s
      expected_status: 200      # default 200
      headers:
        X-Custom-Probe: "true"
```

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `path` | string | — | HTTP path to probe (required). |
| `interval` | duration | `10s` | How often to probe unhealthy upstreams. |
| `timeout` | duration | `3s` | Maximum time to wait for probe response. |
| `expected_status` | int | `200` | Expected HTTP status code. |
| `headers` | map[string]string | — | Additional headers to send with probe. |

Only **unhealthy** upstreams are probed. Healthy ones are left alone, because
there is nothing to recover; this keeps probe traffic proportional to the size of
the failure rather than to the size of the pool.

Because probes are scheduled on a fixed interval with no jitter, a pool that
failed together is also probed together. Give `interval` a value comfortably
larger than your backend's probe cost, or stagger it per upstream.

## Lazy mode

On a CDN-scale edge (`cluster.lazy`), a target's load balancer only exists while
its config is resident in the handler LRU. When a cold target is evicted, its
balancer and its health state go with it, and the next request rebuilds both from
the store with every upstream marked healthy.

This means a target that is being evicted and reloaded repeatedly can keep losing
its health state. Size `cluster.lru_size` to hold your hot targets. See
[Cluster mode](configuration/cluster.md).
