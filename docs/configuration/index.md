---
label: Configuration
icon: gear
order: 700
---

# Configuration

Peretum uses two kinds of YAML files:

- **`config.yaml`** — global proxy settings: listeners, disk cache, metrics,
  JSON logging, error pages, the shared WAF GeoLite directory, and tunables.
- **`config.d/*.yaml`** — **one file per target**: host key, upstreams, load
  balancing, per-location behavior (cache, plugins, proxy options).

```bash
./peretum -t                 # validate everything before starting
```

## Loading order and precedence

1. The global config is loaded and validated.
2. Every `config.d/*.yaml` target is loaded (sorted); the target named
   `listen`-less entries become fallback/default targets.
3. `listeners:` (global) takes precedence over the legacy `listen` shorthand.
4. Per-location plugin blocks (`compression`, `optimize`, `rewrite`, `cors`,
   `headers`) are **global and last-wins** — the plugin config is overwritten
   for each configured location, so one instance applies proxy-wide (see
   [Plugins](../plugins/index.md) for details and the WAF exception).

## Pages

- [Global configuration (`config.yaml`)](global.md)
- [Targets & locations (`config.d/*.yaml`)](targets.md)
- [Listeners, TLS & HTTP/3](listeners.md)

Reference tables on each page come directly from the configuration types in
`internal/config/config.go`.