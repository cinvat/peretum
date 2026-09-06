---
label: gRPC proxying
icon: arrow-switch
order: 400
---

# gRPC proxying

gRPC requests are HTTP/2 streams. Peretum supports them as **tunneled
pass-through**: end-to-end transport (HTTP/2) and binary bodies are left
untouched, and no gRPC-specific features (messages, trailers, reflection) are
interpreted.

## How it works

- gRPC traffic is identified by `Content-Type: application/grpc` (or
  `application/grpc+proto` etc.).
- Hooks are bypassed for gRPC requests: **no caching**, **no rewriting**,
  **no CORS preflight handling**, **no optimization**, **no compression** and
  **no response buffering**.
- `BeforeProxy` still runs, so the **WAF** and request-header rewrite
  apply to gRPC requests too (see [Plugins](plugins/index.md)).

## Server requirements

- The upstream must accept **HTTP/2**:
  - h2c — for cleartext (`listen: ":8080"` or `listeners: [":8080 h2c"]`);
  - ALPN h2 — for TLS (`listeners: [":443 ssl h2"]`).
- Bi-directional streaming is supported (the proxy never buffers the request
  or response body): `client streams`, `server streams` and `bidi` all pass
  through.

## Configuration

Provide `http2`-aware frontends via `listeners:`:

```yaml
listeners:
  - ":8080 h2c"      # cleartext gRPC (no TLS)
  - ":443 ssl h2"    # TLS gRPC with ALPN
```

The default `config.yaml` listens on `:8080` (h2c), `:443` (TLS + HTTP/3) and
`:8443` (TLS + HTTP/3), so both cleartext and TLS gRPC clients work out of the
box.

## Manual check

```bash
grpcurl -plaintext -d '{}' localhost:8080 helloworld.Greeter/SayHello
grpcurl -d '{}' localhost:443 helloworld.Greeter/SayHello
```