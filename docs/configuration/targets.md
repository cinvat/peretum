---
label: Targets & locations (config.d)
order: 200
---

# Targets (`config.d/*.yaml`)

One YAML file per target:

```yaml
name: "api"                        # host key, or use `listen:` with a host
listen: "api.example.com"          # host key (a bare ":port" binds nothing)
lb_algorithm: "maglev"
upstreams:
  - url: "https://api1.example.com"
    weight: 3
  - url: "https://api2.example.com"
    weight: 2
tls:                             # optional per-target certificate
  cert_file: "./certs/api.example.com.crt"
  key_file: "./certs/api.example.com.key"
locations:
  - path: "/api/"
    match_type: "prefix"
    cache: true
    cache_ttl: "1h"
    ...
```

## Target fields

| Key | Type | Description |
| --- | --- | --- |
| `name` | string | Target name; also the router host key unless `listen` overrides it. |
| `listen` | string | Host key for this target. A host-only value (e.g. `api.example.com`) becomes the router key; `":port"`-only values do not create a host key. |
| `upstreams` | []object | `url` (required), `weight` (`<= 0` treated as `1`), `health_check`. |
| `lb_algorithm` | string | See [load balancing](#load-balancing-algorithms). |
| `locations` | []object | Ordered location list; first match wins (see priorities below). |
| `tls` | object | `cert_file`, `key_file` — per-target certificate presented via SNI. |

Targets with no host key act as the **default/fallback** handler; the router
also falls back to a target whose first matching location is `path: "/"`.

## Locations

A location is matched by `path` (first match wins, ordered by priority):

| `match_type` | Priority | Notes |
| --- | --- | --- |
| `exact` | 1000 | `path == request path` |
| `regex` | 500 | full Go regex against the path |
| `prefix` (default) | length of trimmed path | longest prefix wins; `"/"` matches everything |

### Location fields

```yaml
- path: "/api/"
  match_type: "prefix"
  cache: true
  cache_ttl: "1h"            # per-entry TTL, overrides global max_cache_age
  proxy:
    pass_host_header: true
    websocket: true
    connect_timeout: "5s"
    read_timeout: "30s"
    send_timeout: "30s"
    upstream: "http://special-backend:8080"   # per-location upstream override
    buffering: true
    buffer_size: "64k"
  cors: {...}                # plugin blocks below
  headers: {...}
  rewrite: {...}
  optimize: {...}
  compression: {...}
  waf: {...}
```

| Key | Type | Description |
| --- | --- | --- |
| `path` | string | Match pattern. |
| `match_type` | string | `prefix` (default), `exact`, `regex`. |
| `cache` | bool | Enable the disk cache for this location. |
| `cache_ttl` | duration | Overrides `max_cache_age` for entries of this location. |
| `proxy` | object | `websocket`, `pass_host_header`, `upstream` (override), timeouts, `buffering`, `buffer_size`. |
| `cors`, `headers`, `rewrite`, `optimize`, `compression`, `waf` | object | See [Plugins](../plugins/index.md). |

## Load-balancing algorithms

| Value | Behavior |
| --- | --- |
| `round_robin` / `rr` / (empty) | Equal distribution (default). |
| `weighted_rr` / `wrr` | Respects `weight` (smooth weights). |
| `maglev` | Maglev consistent hashing over a 65537-slot table, keyed by `CRC32(path + rawQuery)` — minimal reshuffle across upstream changes. |
| `least_conn` / `least_connections` | Routes to the upstream with the fewest active requests. |

## Health tracking

All algorithms skip unhealthy upstreams. Upstreams are marked **unhealthy** on
transport errors and **healthy** again on successful reads; when every upstream
is unhealthy the least-recently-checked one is probed.