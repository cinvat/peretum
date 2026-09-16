# Listeners

Peretum can bind any number of frontends with the nginx-style `listeners:`
directive. Each entry is `"address[:port] [flags]"`:

| Flag | Meaning |
| --- | --- |
| `ssl` | Serve TLS on the TCP listener (HTTP/1.1 + HTTP/2 via ALPN). |
| `h2c` | Unencrypted HTTP/2 (conflicts with `ssl`). |
| `h2` / `http2` | Accepted for nginx parity; HTTP/2 is always negotiable (ALPN over TLS, h2c on plaintext). |
| `h3` / `http3` / `quic` | Serve HTTP/3 over QUIC/UDP (implies TLS). |

```yaml
listeners:
  - ":8080"            # plaintext; HTTP/1.1 + h2c
  - ":443 ssl h2"      # HTTPS; HTTP/1.1 + HTTP/2 via ALPN
  - ":443 h3"          # HTTP/3 (UDP) on the same port (implies TLS)
  - ":8443 ssl h3"     # HTTPS + HTTP/3 on a dedicated port (UDP + TCP)
```

Rules:

- When no port is given, it defaults to **443** for `ssl`/`h3` listeners and
  **80** otherwise.
- A wildcard address (`""` or `":"`) binds all interfaces.
- A quic-only directive like `":443 h3"` binds **UDP only**; combine with
  `ssl` (same line or a separate `:443 ssl` line) to also open the TCP
  listener on that port.
- `h2c` and `ssl` on the same directive is a configuration error.
- Duplicate flags on one directive are a configuration error.

The legacy shorthand is equivalent to a single plaintext listener:

```yaml
listen: ":8081"
```

When TLS certificates are configured, TLS and HTTP/3 are implied on the listen
port; `http3_addr: ":8443"` pins the QUIC listener to another UDP port.

## TLS

Certificates can come from three places (in increasing specificity):

1. Global `tls_cert_file` / `tls_key_file` in `config.yaml`.
2. Per-target `tls:` blocks (served via SNI).
3. Self-signed certificate generated automatically whenever TLS or HTTP/3 is
   requested but no valid certificate can be loaded.

Reloading (`SIGHUP`) re-applies global and per-target certificates; HTTP/3
listeners pick up the refreshed bundle immediately.

## HTTP/3 (QUIC)

HTTP/3 is served by default on the same port as the TLS frontend, and can be
pinned to another UDP port:

- `:443 h3` alone serves **HTTP/3 only** (UDP); combine with `:443 ssl` (or
  one `:443 ssl h3` line) to also serve HTTPS over TCP on the same port.
- The frontend negotiates `h3` automatically over QUIC; the same certificate
  bundle serves the TCP (h1/h2) and UDP (h3) listeners.
- HTTP/3 requests share the host/location router and plugins, but live
  **outside the disk cache path** (no caching/HIT on HTTP/3 responses).
- The QUIC listener is torn down during graceful shutdown and its TLS config
  refreshed on `SIGHUP`.

See [HTTP/3 (QUIC)](../http3.md) for the full details.

## TLS certificate resolution

TLS is resolved per connection via `GetConfigForClient`: SNI-based per-target
certificates first, then the global certificate, then the generated
self-signed certificate. Clients must trust the self-signed certificate
explicitly when it is used (e.g. `curl -k`).
