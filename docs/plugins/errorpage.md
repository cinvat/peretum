---
label: Error pages
order: 200
---

# Error pages

Replaces default error responses with branded HTML pages.

Global config in `config.yaml`:

```yaml
error_page:
  enabled: true
  statuses: [404, 500, 502, 503, 504]   # default: [404, 500, 502, 503, 504]
```

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | false | Master switch. |
| `statuses` | []int | `[404, 500, 502, 503, 504]` | Status codes rendered as branded pages. |

## Behavior

The plugin wraps the **top-level router** (`RouterWrapper`), so it intercepts:

- router-level misses (e.g. `404 No matching target/location`) that never
  reach a target handler;
- every configured 4xx/5xx from any target — including **WAF blocks** (which
  write a `text/plain` body with `X-WAF-Block: true`).

For configured statuses it substitutes an escaped, branded HTML page (embedded
logo, request ID, time, host, method, path, client address), forces
`Content-Type: text/html; charset=utf-8`, removes `Content-Length` /
`Content-Encoding`, sets `X-Request-ID` and `Cache-Control: no-store`, and
suppresses the body for `HEAD`.