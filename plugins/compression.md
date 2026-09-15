# Compression

Transparent gzip compression of upstream responses.

Location-scoped config (last location wins; applies proxy-wide):

```yaml
locations:
  - path: "/"
    compression:
      enabled: true
      level: 5             # 1-9 (default 5)
      min_length: 1024     # minimum response body size to compress (default 1024)
      types:               # extra MIME types to compress
        - "application/vnd.api+json"
```

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | — | Master switch. |
| `level` | int | 5 | gzip level, 1–9. |
| `min_length` | int | 1024 | Only compress bodies at least this large. |
| `types` | []string | see below | MIME types treated as compressible, merged over the defaults. |

Setting `enable: true` is required — a location whose `compression` block is
disabled emits `enabled: false` and the plugin is not loaded.

## Built-in compressible types

`text/html`, `text/css`, `text/javascript`, `application/javascript`,
`application/json`, `application/xml`, `text/xml`, `text/plain`,
`application/wasm`, `image/svg+xml`.

Configured `types` are normalized (lowercased, whitespace trimmed, `;charset`
stripped) and merged on top of these.

## Behavior

- Compresses **response bodies** when the client sends `Accept-Encoding: gzip`
  (the upstream request has it stripped), the Content-Type is compressible,
  and the body is `>= min_length`.
- Sets `Content-Encoding: gzip`, `Vary: Accept-Encoding`, and removes
  `Content-Length` (the response is fully buffered before deciding).
- Applies only on the **reverse-proxied, non-gRPC** path; cache hits and gRPC
  streams pass through untouched.
