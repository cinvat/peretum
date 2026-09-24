---
label: Targets & locations (config.d)
order: 200
---

# Targets (`config.d/*.yaml`)

One YAML file per target:

```yaml
server_name: "api"                        # hostname key (router match key)
host_header: "api.example.com"            # upstream Host header override (all locations)
lb_algorithm: "maglev"
upstreams:
  - url: "https://api1.example.com"
    weight: 3
    health_check:
      path: "/healthz"
      interval: "10s"
      timeout: "3s"
      expected_status: 200
      headers:
        X-Probe: "peretum"
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
    cache_excludes:
      - ".m3u8"
      - ".ts"
      - "/live/"
    ...
```

## Target fields

| Key | Type | Description |
| --- | --- | --- |
| `server_name` | string | **Required**. Target hostname; used as the router host key and Pebble key. |
| `host_header` | string | Optional upstream `Host` header override sent on every request proxied by this target's locations. Takes precedence over `proxy.pass_host_header`; empty keeps the default behavior. |
| `upstreams` | []object | `url` (required), `weight` (`<= 0` treated as `1`), `health_check` (see [Health tracking](#health-tracking)). |
| `lb_algorithm` | string | See [load balancing](#load-balancing-algorithms). |
| `locations` | []object | Ordered location list; first match wins (see priorities below). |
| `tls` | object | `cert_file`, `key_file` — per-target certificate presented via SNI. |

Targets with no `server_name` act as the **default/fallback** handler; the router
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
| `cache_excludes` | []string | Paths/extensions to bypass cache (e.g., `.m3u8`, `.ts`, `/live/`). Sets `Cache-Control: no-store` and skips cache lookup/write. |
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

## Active Health Checks

Enable per-upstream HTTP health checks to proactively probe and recover failed
upstreams:

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

Active checks run in the background; recovered upstreams are automatically
marked healthy and re-enter the load balancer rotation.

## Cache Excludes (Live Streaming)

For HLS/DASH live streaming, certain paths (manifests, segments) must not be
cached. Use `cache_excludes` on a location:

```yaml
locations:
  - path: "/"
    match_type: "prefix"
    cache: true
    cache_ttl: "5s"
    cache_excludes:
      - ".m3u8"      # HLS playlists
      - ".ts"        # HLS segments
      - "/live/"     # path prefix
```

Behavior for excluded paths:
1. **Cache bypassed** — no cache lookup, no cache write, no request coalescing.
2. **No-cache headers sent to browser:**
   ```
   Cache-Control: no-store, no-cache, must-revalidate, max-age=0
   Pragma: no-cache
   Expires: 0
   ```

Pattern matching:
- `.m3u8` — matches any path ending with `.m3u8` (extension).
- `/live/` — matches any path starting with `/live/` (prefix).
- `/exact/path` — exact match.