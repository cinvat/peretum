---
label: Headers
order: 600
---

# Headers

Adds and removes request and response headers.

Location-scoped config (last location wins; applies proxy-wide):

```yaml
locations:
  - path: "/"
    headers:
      request_add:
        X-Forwarded-By: "peretum"
        X-API-Version: "v2"
      request_remove:
        - "X-Internal-Token"
      response_add:
        X-Powered-By: "peretum"
        Cache-Control: "public, max-age=300"
      response_remove:
        - "Server"
```

| Key | Type | Description |
| --- | --- | --- |
| `request_add` | map[string]string | Headers set on the upstream request. |
| `request_remove` | []string | Headers removed from the upstream request. |
| `response_add` | map[string]string | Headers set on the client response. |
| `response_remove` | []string | Headers removed from the client response. |

## Behavior

- `request_*` mutations apply in `BeforeProxy` (before the upstream fetch),
  including on cache-hit responses.
- `response_*` mutations apply in `AfterProxy`, on the non-gRPC proxied path.
- The plugin applies to **every request** once loaded.

> **Note:** the handler also applies the location's `headers` block natively
> (`applyRequestHeaders` / `applyResponseHeaders` on both cache-hit and proxied
> paths), so header mutations can be applied twice when the plugin and the
> location config are both present — idempotent for `add`, additive for
> `remove`.