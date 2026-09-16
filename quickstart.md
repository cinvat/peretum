# Quickstart

## Prerequisites

- **Go 1.26.2+** (the module lives at `github.com/cinvat/peretum`).

## Build and run

```bash
go build -o peretum .
./peretum
```

This starts the proxy using `config.yaml` and the target files in `config.d/`
and writes a pid file (`peretum.pid`) for `--reload`.

To run without building:

```bash
go run .
```

## Verify the installation

```bash
./peretum -t
# config OK
```

`-t` validates `config.yaml` and every `config.d/*.yaml` without starting the
server: size/duration fields, upstream URLs, listener directives and
per-location `cache_ttl`s.

## First request

The default `config.yaml` listens on `:8080` (plain), `:443` and `:8443`
(TLS + HTTP/3). A self-signed certificate is generated automatically when TLS
or HTTP/3 is requested but no certificate is configured, so:

```bash
curl -k https://localhost:8443/
```

With the default `config.d/` files you will reach an upstream target; if the
upstream is not running you get a `502 Bad Gateway`.

## Test server

`cmd/testserver` provides an upstream for manual and integration testing:

```bash
go run ./cmd/testserver
# serves on 127.0.0.1:8082
```

Endpoints:

- `/style.css` — repeated CSS block (~5.6&nbsp;KB) for compression testing
- `/script.js` — repeated JS block (~5.1&nbsp;KB) for compression/optimizer
- `/image.png` — minimal PNG header
- anything else — a plain-text line echoing the request path

## Ready to configure?

See:

- [Global configuration](configuration/global.md) for `config.yaml`
- [Targets & locations](configuration/targets.md) for `config.d/*.yaml`
- [CLI](cli.md) for reload and config checking
