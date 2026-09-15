# Routing

Peretum routes requests in two steps: **host routing** selects a target, and
**location routing** selects the backend within that target.

## Host routing

The router holds a map of host keys → targets. Hosts come from:

- a target's `listen` **host part** (e.g. `api.example.com`);
- a target's `name` — used as the host key when `listen` has no host;
- the legacy `server_name` shorthand.

Requests are matched by the `Host` header (**lowercased**, port stripped).

Targets with no host key act as the **default/fallback handler**; the router
also falls back to a target whose first matching location is `path: "/"`.

## Location routing

Inside a target, the first location whose `path` matches (ordered by
priority, not definition order) wins:

| `match_type` | Priority | Notes |
| --- | --- | --- |
| `exact` | 1000 | `path == request path` |
| `regex` | 500 | full Go regex against the path |
| `prefix` (default) | length of trimmed path | longest prefix wins; `"/"` matches everything |

```yaml
locations:
  - path: "/api/"
    match_type: "prefix"
    upstream: "http://localhost:8082"
```

## Load balancing

An LB algorithm picks an upstream from the target's `upstreams` list:

| Value | Behavior |
| --- | --- |
| `round_robin` / `rr` / (empty) | Equal distribution. |
| `weighted_rr` / `wrr` | Respects `weight` (smooth). |
| `maglev` | consistent hashing over 65537 slots, keyed by `CRC32(path + rawQuery)`. |
| `least_conn` | upstream with the fewest active requests. |

All algorithms skip unhealthy upstreams; a whole-target failure returns
`503`, and the LB pool is repaired by the health tracker (see
[Targets](configuration/targets.md)).

## gRPC routing

gRPC streams use the same host/location router, so `grpc://` backends and
mixed HTTP/gRPC targets share one route table.
