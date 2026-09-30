---
label: Rate limiting
order: 150
---

# Rate limiting

The `ratelimit` plugin throttles requests with a **token bucket per
target + client IP**. Over-limit requests are rejected with `429 Too Many
Requests` and a `Retry-After: 1` header before they reach the cache or any
upstream.

## Global throttle (`config.yaml`)

```yaml
rate_limit:
  enabled: true
  rps: 100      # sustained requests/second per target+IP (default 100)
  burst: 200    # bucket size, i.e. short-burst allowance (default 200)
```

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | — | Master switch. |
| `rps` | float | `100` | Sustained rate per target+client IP. |
| `burst` | int | `200` | Bucket capacity; absorbs short bursts. |

Buckets are created lazily on first request and idle entries are reaped, so
memory stays proportional to the number of *active* client IPs, not the
target count.

## Per-location throttles (WAF `rate_limit` action)

For limits that apply only to some paths, clients or geographies, use a WAF
rule with `action.type: "rate_limit"` instead of the global throttle. The
rule's conditions select the traffic; `rps`/`burst` tune that slice:

```yaml
locations:
  - path: "/login"
    waf:
      enabled: true
      rules:
        - id: "login-throttle"
          action:
            type: "rate_limit"   # or "ratelimit"
            rps: 5
            burst: 10
            code: 429
            message: "Too Many Requests"
          conditions:
            - - param: "path"
                operator: "startswith"
                value: "/login"
```

Both the plugin and the WAF action delegate to the same
`plugins/ratelimit.Store`, so accounting is shared — but note the bucket
keys differ: the global plugin keys by `target|IP`, the WAF action by
`rule-id|IP`.

## Behavior

- Runs in `BeforeProxy` (before the cache lookup), so throttled floods never
  touch origins or the rule engine behind them.
- Over-limit responses carry `Retry-After: 1`. The WAF variant defaults to
  `429`/`Too Many Requests` when the rule omits `code`/`message`, and the
  [error page plugin](errorpage.md) can brand the page.
- Applies to gRPC requests too (they pass through `BeforeProxy`).
