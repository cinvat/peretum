# Peretum

[![Go](https://github.com/cinvat/peretum/actions/workflows/go.yml/badge.svg)](https://github.com/cinvat/peretum/actions/workflows/go.yml)
[![Docs](https://github.com/cinvat/peretum/actions/workflows/docs.yml/badge.svg)](https://github.com/cinvat/peretum/actions/workflows/docs.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)](https://github.com/cinvat/peretum/actions)
[![Go Version](https://img.shields.io/badge/go-1.26%2B-blue)](https://go.dev/)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](LICENSE)

**Peretum** is a lightweight, high-performance HTTP/HTTPS reverse proxy with transparent disk-based response caching, host+location routing, plugin system, HTTP/3 (QUIC), gRPC passthrough, and a Web Application Firewall.

[📖 Documentation](https://cinvat.github.io/peretum/) · [Quickstart](https://cinvat.github.io/peretum/quickstart.html) · [Configuration](https://cinvat.github.io/peretum/configuration/global.html) · [Plugins](https://cinvat.github.io/peretum/plugins/index.html)

---

## Features

- **Transparent disk cache** — `X-Cache: HIT/MISS`, LRU eviction, background sweep, atomic writes, request coalescing
- **Routing** — prefix/exact/regex `location` matching, per-host targets with default fallback
- **Load balancing** — round-robin, weighted, Maglev consistent hashing, least-connections; passive health tracking
- **Active health checks** — per-upstream HTTP probes with configurable interval/timeout
- **Cache excludes** — bypass cache for live streaming (`.m3u8`, `.ts`, `/live/`) with `Cache-Control: no-store`
- **nginx-style `listeners`** — multiple frontends with `ssl`, `h2c`, `h3`/`quic` flags
- **TLS** — global/per-target certs, automatic self-signed fallback
- **HTTP/3 (QUIC)** — enabled on TLS ports; same cert bundle for TCP/UDP
- **gRPC passthrough** — bidirectional streaming (h2c/h2), never cached or buffered
- **Plugins** — compression, CSS/JS/image optimization, CORS, headers, rewrite, JSON logs, Prometheus, error pages, WAF
- **Hot reload** — `SIGHUP` reloads configs/plugins; graceful 30s shutdown

---

## Quick Start

```bash
go run .
```

Serves using `config.yaml` and `config.d/*.yaml`.

```bash
peretum          # run (writes pid file)
peretum -t       # validate config (prints "config OK")
peretum -r       # hot reload (SIGHUP)
```

---

## Documentation

Full docs at **[cinvat.github.io/peretum](https://cinvat.github.io/peretum/)**:

- [Configuration →](https://cinvat.github.io/peretum/configuration/global.html) — `config.yaml` + `config.d/*.yaml`
- [Listeners, TLS & HTTP/3 →](https://cinvat.github.io/peretum/configuration/listeners.html)
- [Targets & Locations →](https://cinvat.github.io/peretum/configuration/targets.html) — cache, health checks, cache_excludes
- [Plugins →](https://cinvat.github.io/peretum/plugins/index.html) — compression, CORS, headers, WAF, etc.
- [HTTP/3 →](https://cinvat.github.io/peretum/http3.html) | [gRPC →](https://cinvat.github.io/peretum/grpc.html) | [Cache →](https://cinvat.github.io/peretum/cache.html)

---

## Developing

```bash
# Prerequisites: Go 1.26+
go build ./...
go run .                              # run proxy against config.yaml + config.d/
go run ./cmd/testserver               # upstream test server on :8082

# Tests (100% statement coverage required)
go test ./... -cover                  # all packages (must stay 100%)
go test ./internal/cache/disk/ -v -race
go test ./internal/cache/disk/ -bench=.
```

### PR Checklist

- [ ] `go build ./...` and `go vet ./...` clean
- [ ] `gofmt -l .` prints nothing
- [ ] `go test ./... -cover` stays at **100% statement coverage** in every package
- [ ] Config/behavior changes documented in [docs](https://cinvat.github.io/peretum/)
