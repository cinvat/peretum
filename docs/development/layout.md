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
├── cmd/                     # cobra-cli style command tree
│   ├── root.go              # Root command + flags (-t/-r, --config, ...)
│   ├── run.go               # Default run (pid file + server lifecycle)
│   ├── reload.go            # --reload: SIGHUP to the pid in --pid-file
│   ├── check.go             # --test: config syntax check without starting
│   └── server.go            # Proxy server assembly (routers, TLS, HTTP/3)
├── cmd/testserver/          # Upstream test server
├── docs/                    # GitHub Pages documentation (this site)
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
    └── waf/                 # Web Application Firewall (rules + geo filters)
```

## Key entry points

| Concern | Location |
| --- | --- |
| CLI and flags | `cmd/root.go` |
| Server assembly, plugin wiring (`buildPluginConfigs`) | `cmd/server.go` |
| Config types and parsers | `internal/config/config.go` |
| Request lifecycle and plugin hook dispatch | `internal/handler/handler.go` |
| Plugin hook implementation | `internal/plugin/manager/manager.go` |
| Host + location routing | `internal/router/router.go` |
| LB algorithms | `internal/loadbalancer/loadbalancer.go` |
| Plugin contracts | `plugins/base/base.go` |
| Plugin registration | `plugins/registry/init.go` |