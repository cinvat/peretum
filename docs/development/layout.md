---
label: Project layout
order: 400
---

# Project layout

```
peretum/
├── config.yaml              # Global proxy config
├── config.d/                # Target configs (one per file)
│   ├── api.yaml
│   ├── fallback.yaml
│   ├── static.yaml
│   └── test.yaml
├── main.go                  # Entrypoint: delegates to cmd.Execute()
├── cmd/                     # cobra-cli command tree (thin adapters only)
│   ├── root.go              # Root command + flags (-t/-r, --config, ...)
│   ├── run.go               # Default run (pid file + server lifecycle)
│   ├── api.go               # `api` subcommand
│   └── controlplane.go      # `controlplane` subcommand
├── internal/
│   ├── proxyserver/         # Proxy server assembly (routers, TLS, HTTP/3,
│   │                        # lifecycle, plugin config, --test/--reload)
│   ├── controlplane/        # Config publisher (fsnotify -> NATS JetStream)
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
    └── waf/                 # Web Application Firewall (rules + geo filters)
```

## Key entry points

| Concern | Location |
| --- | --- |
| CLI and flags | `cmd/root.go` |
| Server assembly, plugin wiring (`buildPluginConfigs`) | `internal/proxyserver/` |
| Config types and parsers | `internal/config/config.go` |
| Request lifecycle and plugin hook dispatch | `internal/handler/handler.go` |
| Plugin hook implementation | `internal/plugin/manager/manager.go` |
| Host + location routing | `internal/router/router.go` |
| LB algorithms | `internal/loadbalancer/loadbalancer.go` |
| Plugin contracts | `plugins/base/base.go` |
| Plugin registration | `plugins/registry/init.go` |