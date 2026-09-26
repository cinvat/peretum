# Peretum

[![Go](https://github.com/cinvat/peretum/actions/workflows/go.yml/badge.svg)](https://github.com/cinvat/peretum/actions/workflows/go.yml)
[![Docs](https://github.com/cinvat/peretum/actions/workflows/docs.yml/badge.svg)](https://github.com/cinvat/peretum/actions/workflows/docs.yml)
[![Coverage](https://codecov.io/gh/cinvat/peretum/branch/main/graph/badge.svg)](https://codecov.io/gh/cinvat/peretum)
[![Go Version](https://img.shields.io/badge/go-1.26%2B-blue)](https://go.dev/)
[![License](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](LICENSE)

**Peretum** is a lightweight, high-performance HTTP/HTTPS reverse proxy with transparent disk-based response caching, host+location routing, plugin system, HTTP/3 (QUIC), gRPC passthrough, and a Web Application Firewall.

[📖 Documentation](https://cinvat.github.io/peretum/) · [Quickstart](https://cinvat.github.io/peretum/quickstart.html) · [Configuration](https://cinvat.github.io/peretum/configuration/global.html) · [Plugins](https://cinvat.github.io/peretum/plugins/index.html)

---

## Features

- **Transparent disk cache** — `X-Cache: HIT/MISS`, LRU eviction, background sweep, atomic writes, request coalescing
- **Routing** — prefix/exact/regex `location` matching, per-host targets with default fallback
- **Load balancing** — round-robin, weighted, Maglev consistent hashing, least-connections; passive health tracking
- **Active health checks** — per-upstream HTTP probes with configurable interval/timeout/path
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

## CDN-Scale Cluster Mode

For CDN-scale deployments with 10M+ targets:

- **Lazy target loading** — target configs stay on disk (Pebble); compiled handlers materialize on first request
- **Store-backed routing** — no per-target map; a request costs one Pebble point lookup, so memory is O(1) plus the LRU regardless of target count
- **Bounded LRU** — compiled handlers cached up to `cluster.lru_size` (default 1000)
- **Single-Flight Coalescing** — concurrent first requests for same host compile exactly once, via a shared table that holds only in-flight loads
- **Pebble Store** — atomic batch writes, `NoSync` for throughput, fast restart via prefix scan
- **NATS JetStream State Store** — current state of every target on `config.target.{server_name}`; a new edge replays the stream to build its store
- **Replay-Gated Startup** — the edge finishes applying retained events before it accepts traffic (`cluster.replay_timeout`, default 2m), so it never serves 404s for targets it has not caught up with
- **Delta Reloads** — SIGHUP triggers a config diff; only changed targets rebuild

```yaml
cluster:
  enabled: true
  nats_uri: "nats://nats.example.com:4222"
  lazy: true
  data_dir: "/var/lib/peretum/targetstore"
  lru_size: 1000
  replay_timeout: 2m
```

---

## Quick Start

```bash
# Build
go build .

# Run proxy
./peretum -t                    # validate config
./peretum                       # run proxy

# Control plane
peretum controlplane --nats nats://nats.example.com:4222 --config-dir config.d
```

---

## Developing

```bash
# Prerequisites: Go 1.26+
go build ./...
go run .                              # run proxy against config.yaml + config.d/
go run ./cmd/testserver               # upstream test server on :8082

# Tests (CI enforces >=95% total statement coverage)
go test ./... -cover                  # all packages
go test ./internal/cache/disk/ -v -race
go test ./internal/cache/disk/ -bench=.
```

Coverage is measured in CI and reported to Codecov; the badge above tracks the
real number. CI fails below 95% total and warns on any individual function under
100%, so 100% everywhere is the goal but not yet the gate.

### PR Checklist

- [ ] `go build ./...` and `go vet ./...` clean
- [ ] `gofmt -l .` prints nothing
- [ ] `go test ./... -cover` holds total statement coverage at **95% or better**
- [ ] Config/behavior changes documented in [docs](https://cinvat.github.io/peretum/)