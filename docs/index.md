---
label: Peretum
icon: home
---

# Peretum

A lightweight, high-performance HTTP/HTTPS reverse proxy with transparent
disk-based response caching, host + location routing, a plugin system,
HTTP/3 (QUIC), gRPC passthrough and a Web Application Firewall.

## Features

- **Transparent disk cache** — serves subsequent requests from disk with
  `X-Cache: HIT`; LRU eviction, background sweep, atomic writes and
  request coalescing.
- **Routing** — prefix, exact and regex `location` matching, per-host targets
  with a default fallback handler.
- **Load balancing** — round-robin, weighted, Maglev consistent hashing and
  least-connections, with passive health tracking.
- **nginx-style `listeners`** — bind any number of frontends with `ssl`,
  `h2c` and `h3`/`quic` flags.
- **TLS** — global and per-target certificates, automatic self-signed
  certificate generation.
- **HTTP/3 (QUIC)** — served on the listen port by default; HTTP/1.1,
  HTTP/2 and HTTP/3 all work out of the box.
- **gRPC** — pure bidirectional streaming passthrough (h2c + ALPN h2),
  never cached or buffered.
- **Plugins** — compression, CSS/JS/image optimization, CORS, request and
  response headers, path rewrite, JSON access/error logs, Prometheus metrics,
  branded error pages and a Web Application Firewall.
- **WAF** — per-location signature rules (Aho–Corasick driven `contains`),
  operates on geo/IP fields as ordinary rule conditions.
- **Hot reload** — `SIGHUP` reloads configs and rotates log outputs; graceful
  30&nbsp;s shutdown on `SIGINT`/`SIGTERM`.

## Quick start

```bash
go run .
```

Serves on `:8081` by default using `config.yaml` and `config.d/`. See the
[Quickstart](quickstart.md) for the details.

## Documentation

**Getting started**

- [Quickstart](quickstart.md) — build, run, first config, test server
- [CLI](cli.md) — `peretum`, `peretum -t`, `peretum -r` and flags

**Configuration**

- [Configuration overview](configuration/index.md)
- [Global configuration](configuration/global.md) — `config.yaml`
- [Targets & locations](configuration/targets.md) — `config.d/*.yaml`
- [Listeners, TLS & HTTP/3](configuration/listeners.md)

**Plugins**

- [Plugin index](plugins/index.md)
- [Compression](plugins/compression.md)
- [Optimizer](plugins/optimizer.md)
- [CORS](plugins/cors.md)
- [Headers](plugins/headers.md)
- [Rewrite](plugins/rewrite.md)
- [JSON logs](plugins/jsonlog.md)
- [Prometheus metrics](plugins/prometheus.md)
- [Error pages](plugins/errorpage.md)
- [Web Application Firewall](plugins/waf.md)

**Core subsystems**

- [HTTP/3 (QUIC)](http3.md)
- [gRPC proxying](grpc.md)
- [Disk cache](cache.md)
- [Routing](routing.md)
- [Load balancing](loadbalancing.md)

**Development**

- [Development index](development/index.md)
- [Project layout](development/layout.md)
- [Testing](development/testing.md)
- [Adding a plugin](development/adding-a-plugin.md)
- [Contributing](development/contributing.md)