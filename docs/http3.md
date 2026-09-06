---
label: HTTP/3 (QUIC)
icon: broadcast
order: 500
---

# HTTP/3 (QUIC)

HTTP/3 is served by default on the same port as the TLS frontend whenever a
TLS certificate is active, and can be pinned to another UDP port with
`http3_addr`. With the `listeners:` form, per-listener control is explicit.

## Configuration

| Form | Effect |
| --- | --- |
| `listeners: [":443 ssl h3"]` | HTTPS over TCP **and** HTTP/3 over UDP on `:443`. |
| `listeners: [":443 h3"]` | HTTP/3 **only** (UDP socket on `:443`). |
| `listeners: [":443 ssl", ":443 h3"]` | HTTPS (TCP) + HTTP/3 (UDP) on the same port. |
| legacy `listen` + `http3_addr` | TLS implied on `listen` port; QUIC pinned to `http3_addr`. |

In the legacy shorthand, when TLS is active and `http3_addr` is unset, the
QUIC listener runs on the `listen` address (same port, UDP); set
`http3_addr: ":8443"` to use a dedicated UDP port.

## TLS requirement

HTTP/3 implies TLS. TLS requires either global `tls_cert_file`/`tls_key_file`
or per-target `tls:` blocks. If TLS or HTTP/3 is requested but no valid
certificate can be loaded, the proxy **generates a self-signed certificate**
automatically (clients must trust it explicitly, e.g. `curl --http3 -k`), so
HTTP/1.1, HTTP/2, and HTTP/3 all work out of the box.

## Runtime behavior

- **ALPN** — the frontend negotiates `h3` automatically over QUIC; the same
  certificate bundle serves both the TCP (h1/h2) and UDP (h3) listeners.
- **Routing & plugins** — HTTP/3 requests share the same host/location router
  and plugin stack (including the WAF).
- **Caching** — HTTP/3 responses live **outside** the on-disk cache path; no
  caching or `X-Cache: HIT` on HTTP/3.
- **Lifecycle** — the QUIC listener is torn down during graceful shutdown
  (`SIGINT`/`SIGTERM`) and its TLS config is refreshed on `SIGHUP` reload.

## Verification

```bash
curl --http3 -k https://localhost:8443/     # needs a curl build with HTTP/3
```

The suite also round-trips a `quic-go` HTTP/3 client through the proxy
(`proto=HTTP/3.0`).