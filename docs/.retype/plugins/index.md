# Plugins

Peretum loads plugins from a registry and dispatches lifecycle hooks around the
request path. Plugins are configured **globally** in `config.yaml` (global
settings) and per-location in `config.d/*.yaml` (their behavior).

## The registry

Registered plugins:

| Name | Location-scoped | Page |
| --- | --- | --- |
| `cors` | yes | [CORS](cors.md) |
| `headers` | yes | [Headers](headers.md) |
| `rewrite` | yes | [Rewrite](rewrite.md) |
| `compression` | yes | [Compression](compression.md) |
| `optimizer` | yes | [Optimizer](optimizer.md) |
| `jsonlog` | no | [JSON logs](jsonlog.md) |
| `prometheus_exporter` | no | [Prometheus metrics](prometheus.md) |
| `error_page` | no | [Error pages](errorpage.md) |
| `waf` | per target+location | [WAF](waf.md) |

Notes:

- `enabled` is the **universal load gate** — a plugin whose `enabled` key is
  false is not instantiated at all.
- Location-scoped plugins (`cors`, `headers`, `rewrite`, `compression`,
  `optimizer`) produce **one global instance**: `buildPluginConfigs` overwrites
  the config for every configured location, so the last location in load order
  wins and the resulting plugin applies **to every request on every location**.
  The WAF plugin deliberately avoids this by aggregating a `target|location`
  indexed map (see [WAF](waf.md)).
- Reload (`SIGHUP`) re-applies TLS/router/log-rotation but does **not** re-run
  plugin `Init`; behavior changes require a restart.

## Lifecycle hooks

Request handling in `internal/handler/handler.go` dispatches:

- **BeforeProxy** — before the upstream fetch (also before cache lookup): CORS,
  request headers, rewrite, WAF.
- **AfterProxy** — after the upstream response: response headers.
- **ResponseBodyHook / TransformResponseBody** — on the store-to-cache path
  only: optimizer.
- **Cache hooks** — cache-hit/miss/eviction accounting for metrics.
- **RouterWrapper** — `error_page` wraps the whole frontend router.
- **ErrorHook** — `jsonlog` receives proxy/transform/cache errors.

**gRPC requests** (Content-Type `application/grpc*`) bypass caching, rewriting,
CORS preflight handling, optimization, compression and response buffering; they
still pass through `BeforeProxy` (so WAF applies) and the request logger.

## Caveats

CORS, headers and rewrite each have **both** the plugin hook path and a native
per-location path in the handler. When the same location feeds both (which the
wiring does), the transform is applied **twice** (idempotent header sets; rule
rewrites and preflight short-circuits are one-shot).

## Pages

- [Compression](compression.md)
- [Optimizer](optimizer.md)
- [CORS](cors.md)
- [Headers](headers.md)
- [Rewrite](rewrite.md)
- [JSON logs](jsonlog.md)
- [Prometheus metrics](prometheus.md)
- [Error pages](errorpage.md)
- [Web Application Firewall](waf.md)
