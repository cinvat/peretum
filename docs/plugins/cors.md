---
label: CORS
order: 700
---

# CORS

Cross-Origin Resource Sharing response headers.

Location-scoped config (last location wins; applies proxy-wide):

```yaml
locations:
  - path: "/"
    cors:
      enabled: true
      allow_origins:
        - "https://app.example.com"
        - "https://dev.example.com"
      allow_methods: ["GET", "POST", "PUT", "DELETE"]
      allow_headers: ["Authorization", "Content-Type", "X-Request-ID"]
      expose_headers: ["X-Total-Count"]
      allow_credentials: true
      max_age: 3600
```

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | — | Master switch. |
| `allow_origins` | []string | empty | Origin is allowed if an entry is `"*"` or matches exactly. |
| `allow_methods` | []string | `GET, POST, PUT, DELETE, OPTIONS, HEAD` | Emitted in `Access-Control-Allow-Methods`. |
| `allow_headers` | []string | empty | If empty, echoes `Access-Control-Request-Headers`. |
| `expose_headers` | []string | empty | `Access-Control-Expose-Headers`. |
| `allow_credentials` | bool | false | Sets `Access-Control-Allow-Credentials: true`. |
| `max_age` | int | 0 | `Access-Control-Max-Age` when `> 0`. |

## Behavior

- Runs in `BeforeProxy` (before the cache lookup), so it also applies to
  cached responses.
- `Access-Control-Allow-Origin` echoes the requesting origin — `*` is accepted
  in config but never emitted verbatim when credentials are used.
- An `OPTIONS` request from an allowed origin **short-circuits** with `204 No
  Content` and is never proxied or cached.

> **Note:** the handler also runs a native per-location CORS path when
> `location.CORS.Enabled` is set, so the same headers are written twice
> (idempotent overwrites). The plugin instance matches every location once
> enabled.