---
label: Rewrite
order: 500
---

# Rewrite

Regex-based request path rewrites, with optional `$N` capture groups,
`break` and redirect support.

Location-scoped config (last location wins; applies proxy-wide):

```yaml
locations:
  - path: "/"
    rewrite:
      pattern: "^/api/v1/(.*)"
      replacement: "/v2/$1"
      break: false
      redirect: ""          # "", "redirect" (302), or "permanent" (301)
```

| Key | Type | Description |
| --- | --- | --- |
| `pattern` | string | Regex matched against `r.URL.Path` (required). |
| `replacement` | string | Replacement; supports `$1`, `$2`, … (required). |
| `break` | bool | Stop after the first matching rule. |
| `redirect` | string | `""` = rewrite in place; `"redirect"` = 302; `"permanent"` = 301. |

Behavior:

- Rules with an empty pattern/replacement or an un-compilable regex are
  **skipped** with a warning.
- Rewrites `r.URL.Path` in `BeforeProxy`; the rewritten path is what gets
  proxied and cached.
- A configured `redirect` issues the matching status and short-circuits the
  request (never proxied or cached).
- Applies to **every** request once loaded — including locations that did not
  declare a `rewrite` block.

> **Note:** the handler also applies the location's `rewrite` block natively on
> the non-gRPC path, so a single location feeding both can apply the rewrite
> twice (the plugin runs globally).