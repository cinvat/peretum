# AGENTS.md — peretum

CDN reverse proxy in Go (HTTP/1.1, HTTP/2, HTTP/3/QUIC, gRPC passthrough, disk
cache, plugins, WAF). Edges hold **10M+ targets** via lazy loading: cold configs
stay in a Pebble store, compiled handlers materialize on first request into a
bounded LRU, config streams in over NATS JetStream.

## Layout

- `cmd/` — cobra CLI **thin adapters only** (`root.go`, `api.go`,
  `controlplane.go`, `run.go`). No proxy logic here.
- `internal/proxyserver/` — proxy assembly: `server.go` (struct), `router_wiring.go`,
  `lazy_cluster.go`, `tls_listeners.go`, `lifecycle.go` (`Run`), `plugin_config.go`,
  `check.go` (`CheckConfig`), `reload.go` (`ReloadProxy`).
- `internal/controlplane/` — `Run`: fsnotify reconciler + JetStream publisher + health HTTP.
- `internal/{config,router,handler,loadbalancer,cache/disk,cluster,plugin/manager}/`
- `plugins/{base,cors,headers,rewrite,compression,optimizer,jsonlog,prometheus,ratelimit,errorpage,waf,registry}/`
- `api/` — target CRUD REST (`/v1alpha1/targets`); `docs/` — Retype site; `examples/cluster/` — compose demo.

## Commands

```bash
GOPROXY=direct go build ./...
GOPROXY=direct go vet ./...
gofmt -l .                                      # must print nothing
GOPROXY=direct go test -count=1 ./...           # full suite
GOPROXY=direct go test -race -count=1 ./cmd/ ./internal/proxyserver/ ./internal/handler/
go test -cover ./...                            # total gate: >= 95%, every function ~100%
npx --yes retype build docs                     # if docs/*.md changed
```

`GOPROXY=direct` is required in this environment (`proxy.golang.org` 403s on
`klauspost/compress`). Tests bind fixed ports (`:8081`, `:8082`, `:9090`) — stop
local proxies first. `TestServeHTTP_CacheTTL` (200ms TTL) is a known flake; rerun
before blaming your change.

## Conventions

- Keep `cmd/` thin: new CLI surface = cobra constructor in `cmd/`, logic in
  `internal/`. Entry points are exported (`Run`, `CheckConfig`, `ReloadProxy`).
- Shared pure helpers live in `internal/config` (e.g. `ParseServerNames`,
  `ParseListener`) so `proxyserver` and `controlplane` don't import each other.
- Rate limiting: single source of truth is `plugins/ratelimit.Store`
  (`Allow(key, rps, burst)`). The WAF `rate_limit` action delegates to it — never
  a second bucket implementation.
- Metrics: new per-target series go in `plugins/prometheus` next to
  `peretum_target_*`; error paths get `RecordError`.
- Maglev: health flips set the dirty flag only; `Next()` rebuilds lazily.
- Test helpers (`writeFile`, `freePort`, `runConfigYAML`, `startTestNATS`) are
  duplicated per test package — Go test files aren't importable. Keep each copy
  minimal; NATS tests boot an in-process server (`startTestNATS`).
- `gopkg.in/yaml.v3` package name is `yaml` — import trimmers that derive the
  name from the path (`v3`) get this wrong.
- One logical change per commit; commit only when asked. Cover new branches,
  especially error paths.
