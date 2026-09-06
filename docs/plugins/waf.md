---
label: Web Application Firewall (WAF)
order: 100
---

# Web Application Firewall (WAF)

The `waf` plugin inspects requests **before proxying** and blocks or audits
them against per-location rule sets. It runs from `BeforeProxy`, so it applies
to proxied, cached and gRPC requests alike.

Unlike the other location-scoped plugins (which collapse to a single global
instance), the WAF aggregates **every** location that declares a `waf:` block
into a `target|location`-indexed policy map, so each location gets exactly its
own rules. Locations without a `waf:` block are not covered by the WAF.

## Global settings (`config.yaml`)

```yaml
waf:
  enabled: true
  geolite_dir: "./plugins/waf/geolite"
```

| Key | Type | Description |
| --- | --- | --- |
| `enabled` | bool | Master switch. |
| `geolite_dir` | string | Directory with `GeoLite2-Country.mmdb` (**required**), `GeoLite2-ASN.mmdb` (optional), `GeoLite2-City.mmdb` (optional). A missing Country DB is fatal; missing ASN/City DBs degrade gracefully (their params just return empty). |

## Per-location policy (`config.d/*.yaml`)

```yaml
locations:
  - path: "/"
    waf:
      enabled: true
      rules:
        # Local IP bypass (replaces the former ip_whitelist gate key).
        - id: "allow-localhost"
          name: "Always allow local development traffic"
          enabled: true
          action:
            type: "allow"
          conditions:
            - - param: "ip"
                operator: "in_ip"
                value: "127.0.0.1, ::1"
        # Geo + IP enforcement — rule-based conditions replace geo gate keys.
        - id: "geo-ip"
          name: "Geo + IP enforcement"
          enabled: true
          action:
            type: "deny"
            code: 403
            message: "Geo/IP rule blocked"
          conditions:
            - - param: "country"
                operator: "contains"    # case-insensitive substring: "ir" matches IR
                value: "ir"
            - - param: "asn"
                operator: "in"
                value: "4134,4837"
            - - param: "city"
                operator: "equals"
                value: "Tehran"
            - - param: "ip"
                operator: "in_ip"
                value: "8.8.8.0/24, 51.0.0.0/8"
        # SQL injection
        - id: "sql-select"
          name: "Block obvious SQL injection"
          enabled: true        # per-rule toggle
          action:
            type: "deny"       # deny | allow | log
            code: 403          # HTTP status for the block page
            message: "SQL injection attempt blocked"   # block page body
          conditions:
            # OR between groups, AND within a group (DNF, mirrors the Lua WAF).
            - - param: "body"
                operator: "contains"
                value: "select"
              - param: "body"
                operator: "contains"
                value: "from"
            - - param: "query"
                operator: "matches"
                value: "union.*select"
```

## Evaluation order

Rules are evaluated in the order they appear in the `rules:` list. A rule
with `action.type: "allow"` short-circuits the evaluation (the request passes
immediately). The first matching deny rule blocks the request. A rule with
`action.type: "log"` records the match and evaluation continues.

A rule matches when **any** group of conditions matches (DNF). Geo and IP
checks (country, asn, asn_org, city, ip) are ordinary condition parameters
and work the same way as header or body checks.

## Conditions

A rule matches when **any** group of conditions matches; within a group, all
conditions must match (DNF). Operators are parsed case-insensitively:

| Operator | Matches when |
| --- | --- |
| `contains` | param value contains `value` (Aho–Corasick automaton) |
| `equals` | exact string equality (for the `ip` param: network containment) |
| `startswith` | value starts with `value` |
| `endswith` | value ends with `value` |
| `matches` | regexp `value` matches |
| `in` / `not_in` | value is/is-not in the comma-separated `value` list (for the `ip` param: member of any listed IP/CIDR) |
| `in_ip` / `not_in_ip` | client IP is/is-not contained in the comma-separated IP/CIDR `value` list |
| `gt` / `lt` | numeric comparison (asn, score-like numeric params) |
| `exists` / `not_exists` | param is present / absent |

For the `ip` parameter, `equals`, `in`, `not_in`, `in_ip` and `not_in_ip`
perform **network containment**: `value` is a comma-separated list of plain
IPs (treated as `/32` or `/128`) and/or CIDR blocks, and the condition fires
when the effective client IP is (or is not) inside any of them.

## Parameters

| Param | Value |
| --- | --- |
| `host` | `Host` header |
| `user_agent` | `User-Agent` header |
| `referer` | `Referer` header |
| `cookie` | `Cookie` header |
| `url` | full request URI |
| `path` | path only |
| `query` | raw query string |
| `method` | HTTP method |
| `ip` | effective client IP (network-containment checks) |
| `country` | ISO country code from GeoLite Country |
| `asn` | AS number |
| `asn_org` | AS organization |
| `city` | city name (en locale of GeoLite2 City) |
| `body` | request body |
| `arg` | query argument named by `param_name` |
| `header` | request header named by `param_name` |

`country`, `asn`, `asn_org` and `city` are ordinary condition parameters. Use
`equals` / `in` / `not_in` for exact geo matches and `gt` / `lt` for numeric
ASN ranges:

```yaml
- param: "country"
  operator: "in"
  value: "IR,CN,RU"
- param: "asn"
  operator: "gt"
  value: "30000"
- param: "city"
  operator: "equals"
  value: "Tehran"
```

Client addresses and network blocks are matched as part of a rule too:

```yaml
- param: "ip"
  operator: "not_in"          # allow every cloud block except these
  value: "1.2.3.0/24, 192.168.0.0/16, 10.10.10.10"
- param: "ip"
  operator: "in_ip"
  value: "8.8.8.0/24"
```

## Performance

All `contains` patterns for a parameter are merged into a **single
Aho–Corasick automaton**, so each request field is scanned once regardless of
how many rules reference it.

## Client IP resolution

`X-Real-IP` > first hop of `X-Forwarded-For` > `RemoteAddr`.

## Blocking

Blocks write a plain-text body with `X-WAF-Block: true` and the status from
the matched rule's `action.code`, default `403`. A rule without `action.code`
or `action.message` uses `403` and `Request blocked by WAF`. The [error page
plugin](errorpage.md) layers its branded page on top.