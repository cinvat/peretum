# peretum

A lightweight, high-performance HTTP/HTTPS reverse proxy with transparent disk-based response caching and location routing.

## What it does

- **Caches responses** to disk — serves subsequent requests from cache with `X-Cache: HIT`
- **Routes by path** — prefix, exact, or regex matching `location`
- **Routes by host** — per-target host keys plus a default fallback handler
- **Load balances** — round robin, weighted, maglev (consistent hashing), least connections
- **Multiple upstreams per target** — with weights
- **YAML config** — one file per target in `config.d/`, global settings in `config.yaml`
- **Plugins** — compression, CSS/JS+image optimization, CORS, request/response headers, path rewrite, JSON access/error logs, Prometheus metrics, branded error pages, and a Web Application Firewall (WAF)
- **Hot reload** — `SIGHUP` reloads configs and rotates plugin outputs; `SIGINT`/`SIGTERM` triggers a graceful 30s shutdown
- **nginx-style `listeners`** — bind any number of frontends with flags such as
  `ssl`, `h2c`, and `h3`/`quic` (the legacy single `listen` + `http3_addr`
  shorthand still works)
- **TLS** — per-target and global certificates via `GetConfigForClient`; a
  self-signed certificate is generated automatically when TLS/HTTP/3 is
  requested but no valid certificate is configured
- **HTTP/3 (QUIC)** — enabled by default on the listen port whenever TLS is
  active (override with `http3_addr`), so HTTP/1.1, HTTP/2, and HTTP/3 all work
  out of the box
- **gRPC** — bidirectional streaming passthrough (full HTTP/2 h2c serving, h2 ALPN for TLS) never cached or buffered
- **Tunables** — `max_write_workers` (concurrent cache writes, `-1` = unlimited)
  and `max_response_body_size` (largest buffered response, `-1` = unlimited)

## Quick start

```bash
go run .
```

Serves on `:8081` (override with `listen` in `config.yaml`) using `config.yaml` and `config.d/`.

## CLI

`peretum` uses a cobra command-line interface:

```bash
peretum                  # run the proxy (writes a pid file, defaults below)
peretum -t               # check configuration syntax and exit ('config OK')
peretum -r               # reload the running proxy (SIGHUP to the pid file)
```

Flags:

- `-t, --test` — validate `config.yaml` and every `config.d/*.yaml` (including
  `max_cache_size`/`max_cache_age` sizes and durations, upstream URLs,
  `max_response_body_size`, `max_write_workers`, `listeners` directives, and
  per-location `cache_ttl`) without starting the server; prints `config OK`.
- `-r, --reload` — read the running proxy's pid file and send it `SIGHUP` for a
  hot config reload.
- `--config <path>` — proxy config file (default `config.yaml`).
- `--targets <dir>` — per-target config directory (default `config.d`).
- `--pid-file <path>` — pid file written at startup / read by `--reload`
  (default `peretum.pid`).

## Configuration

### config.yaml — global settings

```yaml
# nginx-style multi-listener frontends (takes precedence over `listen`).
# Each entry: address[:port] [flags]; flags: ssl, h2c, h2/http2, h3/http3/quic.
# A bare host or port defaults to 443 for ssl/h3 listeners, 80 otherwise.
listeners:
  - ":8080"            # plaintext; HTTP/1.1 + h2c
  - ":443 ssl h2"      # HTTPS; HTTP/1.1 + HTTP/2 via ALPN
  - ":443 h3"          # HTTP/3 (UDP) on the same port (implies TLS)
  - ":8443 ssl h3"     # HTTPS + HTTP/3 on a dedicated port (UDP + TCP)

# The legacy shorthand is equivalent to a single plaintext `:8081` listener
# (when TLS certs are configured, TLS and HTTP/3 are implied on the same
# port; http3_addr pins the QUIC listener to another UDP port):
# listen: ":8081"
# http3_addr: ":8443"                # HTTP/3 (QUIC/UDP) frontend; when TLS is
#                                   # active and unset, HTTP/3 serves on the
#                                   # listen port by default
cache_dir: "./cache"
max_cache_size: "500MB"
max_cache_age: "24h"
max_write_workers: 8
#   ^ max concurrent cache writes; -1 = unlimited (default 8)
max_response_body_size: "50MB"
#   ^ largest buffered response body; -1 = unlimited (default 50MB)
# tls_cert_file: "./certs/server.crt"   # global HTTPS
# tls_key_file: "./certs/server.key"
metrics_addr: ":9090"                    # enables prometheus_exporter plugin
json_log:
  enabled: true
  access_log: "./logs/access.jsonl"
  error_log: "./logs/error.jsonl"
  stdout: true

waf:
  enabled: true
  # Shared GeoLite databases (Country/ASN/City .mmdb) consulted by every
  # per-location WAF policy (see the target example below).
  geolite_dir: "./plugins/waf/geolite"
```

### config.d/*.yaml — one per target

```yaml
name: "api"
listen: "api.example.com"        # host key; omit for the default fallback target
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
    cors:
      enabled: true
      allow_origins: ["https://app.example.com"]
      allow_methods: ["GET", "POST", "PUT", "DELETE"]
    rewrite:
      pattern: "^/api/v1/(.*)"
      replacement: "/v2/$1"
    headers:
      request_add:
        X-Forwarded-By: "peretum"
      request_remove: ["X-Internal-Token"]
      response_add:
        Cache-Control: "public, max-age=300"
      response_remove: ["Server"]
    proxy:
      pass_host_header: true
      websocket: true
      connect_timeout: "5s"
      read_timeout: "30s"
      send_timeout: "30s"
    optimize:                    # applied before caching
      enabled: true
      minify_css: true
      minify_js: true
      images:
        enabled: true
        max_width: 1600
        max_height: 1200
        quality: 82
    compression:
      enabled: true
      level: 5
      min_length: 1024
      types: ["text/html", "application/json"]
    waf:                       # per-location Web Application Firewall policy
      enabled: true
      rules:
        - id: "allow-localhost"
          name: "Always allow local development traffic"
          enabled: true
          action:
            type: "allow"
          conditions:
            - - param: "ip"
                operator: "in_ip"
                value: "127.0.0.1, ::1"
        - id: "geo-ip"
          name: "Geo + IP enforcement"
          enabled: true
          action:
            type: "deny"
            code: 403
            message: "Geo/IP rule blocked"
          conditions:
            - - param: "country"
                operator: "contains"
                value: "ir"
            - - param: "ip"
                operator: "in_ip"
                value: "8.8.8.0/24, 51.0.0.0/8"
        - id: "sql-select"
          name: "Block obvious SQL injection"
          action:
            type: "deny"
            code: 403
            message: "SQL injection attempt blocked"
          conditions:
            # OR between groups, AND within a group (DNF, mirrors the Lua WAF).
            - - param: "body"
                operator: "contains"
                value: "select"
              - param: "body"
                operator: "contains"
                value: "from"
            - - param: "query"
                operator: "matches"
                value: "union.*select"
        - id: "ua-curl"
          name: "Block curl user agents"
          action:
            type: "deny"
          conditions:
            - - param: "user_agent"
                operator: "contains"
                value: "curl"
  - path: "/health"
    match_type: "exact"
    cache: false
  - path: "/v1/users/[0-9]+"
    match_type: "regex"
    cache: true
    cache_ttl: "5m"
```

**Match types:**
- `prefix` (default) — matches `/api/`, `/api/users`, `/api/users/123`
- `exact` — matches only `/health`
- `regex` — full Go regex, e.g. `/v1/users/[0-9]+`

**LB algorithms:**
- `round_robin` / `rr` — equal distribution
- `weighted_rr` / `wrr` — respects weight field
- `maglev` — consistent hashing, minimal reshuffle on changes
- `least_conn` — routes to upstream with fewest active requests

## WAF (Web Application Firewall)

The `waf` plugin inspects requests before proxying and blocks-or-audits them
against per-location rule sets. Geo filters (country/ASN/city) and IP
containment checks are condition parameters that work the same way as header
or body checks. `contains` conditions are matched with an Aho-Corasick
automaton.

Global settings live in `config.yaml` under `waf:` (`enabled`,
`geolite_dir`); each `location` in a target may declare its own `waf:` policy
with the fields shown in the example above. A location only has WAF coverage
when it declares a `waf:` block — the plugin ignores everything else.

**Enforcement:**
- Rules run in declaration order. A rule with `action.type: "deny"` blocks
  (status from `action.code`, default 403; body from `action.message`).
- `action.type: "allow"` short-circuits — no subsequent rules are checked.
- `action.type: "log"` records the match and continues to the next rule.

**Parameter list** (the value a condition inspects):

`country`, `asn`, `asn_org`, `city`, `ip`, `host`, `user_agent`, `referer`,
`cookie`, `url`, `path`, `query`, `method`, `body`, `arg:<name>`,
`header:<name>`, `param:` + `param_name`.

Geo fields (`country`, `asn`, `asn_org`, `city`) are full condition parameters
and can appear anywhere a rule expects a condition. For the `ip` param,
membership operators (`equals`, `in`, `not_in`, `in_ip`, `not_in_ip`) match
against **IP addresses or CIDR blocks**: `value` is a comma-separated list of
IPs (plain IPs become `/32` or `/128` host routes) and the rule fires on
network containment.

**Operators:** `contains`, `equals`, `startswith`, `endswith`, `matches`
(regex), `In`, `not_in`, `in_ip`, `not_in_ip`, `gt`, `lt`, `exists`,
`not_exists`.

## Test server

`cmd/testserver` provides an upstream for manual and integration testing:

```bash
go run ./cmd/testserver
# serves on 127.0.0.1:8082
```

- `/style.css` — repeated CSS block (~5.6KB) for compression testing
- `/script.js` — repeated JS block (~5.1KB) for compression/optimizer testing
- `/image.png` — minimal PNG header
- anything else — plain-text line echoing the request path

## gRPC proxying

gRPC requests (any `Content-Type` starting with `application/grpc`) are
detected per-request and passed through as raw bidirectional HTTP/2 streams:

- **Pure streaming passthrough** — request/response bodies are never
  buffered, coalesced, cached, rewritten, or compressed; `FlushInterval: -1`.
- **Frontend** — plaintext listeners serve unencrypted HTTP/2 (h2c) natively
  via `net/http` (`Protocols.UnencryptedHTTP2`), so `grpc-go` clients using
  `insecure.NewCredentials()` work out of the box; HTTPS listeners use ALPN
  `h2`.
- **Backend** — plaintext `http://` upstreams are dialed with h2 prior
  knowledge (`x/net/http2` `AllowHTTP` transport); `https://` upstreams use
  the default ALPN transport. No `grpc-gateway`, protoc stubs, or codec
  registration needed — the proxy just relays frames.
- Verified end-to-end with a real `grpc-go` client/server (bidi + streaming
  with custom headers and trailers) directly and through the proxy.

## HTTP/3 (QUIC)

HTTP/3 is served by default on the same port as the TLS frontend whenever a
TLS certificate is active, and can be pinned to another UDP port with
`http3_addr`. With the `listeners:` form, per-listener control is explicit:

- **Configuration** — a listener flag `h3`/`http3`/`quic` opens the QUIC/UDP
  socket on that listener's address (implied TLS). `:443 h3` on its own serves
  HTTP/3 only (UDP); combine it with `:443 ssl` (or one `:443 ssl h3` line) to
  also serve HTTPS over TCP on the same port. In the legacy shorthand, when TLS
  is active and `http3_addr` is unset, the QUIC listener runs on the `listen`
  address (same port, UDP); set `http3_addr: ":8443"` to use a dedicated UDP
  port. TLS requires either `tls_cert_file`/`tls_key_file` (or per-target
  `tls:` blocks); if TLS/HTTP/3 is requested but no valid certificate can be
  loaded, the proxy generates a self-signed certificate automatically (clients
  must trust it explicitly, e.g. `curl -k`), so all HTTP versions — HTTP/1.1,
  HTTP/2, and HTTP/3 — work out of the box.
- **ALPN** — the frontend negotiates `h3` automatically over QUIC; the same
  certificate bundle is used for both the TCP (h1/h2) and UDP (h3) listeners.
- **Runtime** — the QUIC server shares the same host/location router and
  plugins; requests live outside the on-disk cache path (no caching/HIT on
  HTTP/3 responses).
- **Lifecycle** — the QUIC listener is torn down during graceful shutdown
  (`SIGINT`/`SIGTERM`) and its TLS config is refreshed on `SIGHUP` reload.
- Verified with a `quic-go` HTTP/3 client round-tripping through the proxy to an
  upstream (`proto=HTTP/3.0`, 200). `curl --http3` works with a curl build that
  includes HTTP/3 support.

## Project layout

```
peretum/
├── config.yaml              # Global proxy config
├── config.d/                # Target configs (one per file)
│   ├── api.yaml
│   ├── fallback.yaml
│   ├── novin.yaml
│   └── static.yaml
├── main.go                  # Entrypoint: delegates to cmd.Execute()
├── cmd/                     # cobra-cli style command tree
│   ├── root.go              # Root command + flags (-t/-r, --config, ...)
│   ├── run.go               # Default run (pid file + server lifecycle)
│   ├── reload.go            # --reload: SIGHUP to the pid in --pid-file
│   ├── check.go             # --test: config syntax check without starting
│   └── server.go            # Proxy server assembly (routers, TLS, HTTP/3)
├── cmd/testserver/          # Upstream test server
├── internal/
│   ├── cache/disk/          # Disk cache (shards, atomic writes, eviction)
│   ├── config/              # YAML loading + parsing
│   ├── handler/             # Proxy handler, caching, coalescing
│   ├── loadbalancer/        # LB algorithms
│   ├── plugin/manager/      # Plugin registry hook dispatch
│   └── router/              # Host + location routing
└── plugins/
    ├── base/                # Plugin interface + helpers
    ├── compression/
    ├── cors/
    ├── errorpage/           # Branded HTML error pages
    ├── headers/
    ├── jsonlog/             # JSON access/error logs
    ├── optimizer/           # CSS/JS minification + image optimization
    ├── prometheus/          # Metrics exporter
    ├── registry/            # Plugin registration/lifecycle
    ├── rewrite/
    └── waf/                 # Web Application Firewall (rule-based)
```

## Cache storage

Responses stored as two files under 256 shard directories:

```
cache/
  a3/
    f2b8c9...abcd.meta   # status + headers (binary)
    f2b8c9...abcd.body   # raw response body
```

- Key = SHA-256(host + "|" + path) — no path traversal, no length limits
- Atomic writes via temp file + rename
- LRU eviction when `max_cache_size` exceeded
- Background sweep removes expired entries

## Contributing

### Prerequisites

- Go 1.26.2+
- The repo lives at `github.com/cinvat/peretum`

### Developing

```bash
go build ./...                                # compile everything
go run .                                      # run the proxy against config.yaml/config.d/
go run ./cmd/testserver                       # upstream test server on :8082
```

Tests bind fixed ports (`:8081`, `:8082`, `:9090`) — stop any proxy/testserver/
metrics process on those before running the suite, or port-conflict tests skip.

### Adding a target

Drop a new YAML file in `config.d/` (see the config example above), verify it
with `peretum -t`, then `SIGHUP` a running proxy (or use `peretum -r`, or
restart it). No code changes needed.

### Adding a plugin

1. Implement `base.Plugin` (`plugins/base/base.go`) plus any optional hook
   interfaces you need (`RequestHook`, `ResponseBodyHook`, `CacheHook`,
   `MetricsHook`, `ErrorHook`, ...).
2. Register it in `plugins/registry/init.go`.
3. Enable it under the `plugins:` key in `config.yaml`.

### Testing

Every package must keep 100% statement coverage — new code requires matching
tests, never a relaxed bar:

```bash
go test ./... -cover           # suite must stay at 100% per package
go test ./internal/cache/disk/ -v -race
go test ./internal/cache/disk/ -bench=.
```

### PR checklist

- [ ] `go build ./...` and `go vet ./...` are clean
- [ ] `gofmt -l .` prints nothing
- [ ] `go test ./... -cover` stays at 100% statement coverage in every package
- [ ] Config/behavior changes are documented in this README

## Test coverage

All 20 packages (including the `cmd` command tree and the `main` entrypoint)
carry suite-level tests at 100% statement coverage:

```bash
go test ./...                    # all packages (currently 100% statement coverage)
go test ./internal/cache/disk/ -v -race
go test ./internal/cache/disk/ -bench=.
```

## Building

```bash
go build -o peretum .
./peretum
```
