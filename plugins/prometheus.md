# Prometheus metrics

Exports an HTTP `/metrics` endpoint on a dedicated listener.

Enabled automatically when `metrics_addr` is set in `config.yaml`:

```yaml
metrics_addr: ":9090"
```

The plugin config is derived from the global settings:

| Key | Source | Default |
| --- | --- | --- |
| `listen` | `metrics_addr` | `:9090` |

## Metrics

| Metric | Type |
| --- | --- |
| `peretum_target_requests_total{target}` | counter |
| `peretum_target_cached_requests_total{target}` | counter |
| `peretum_location_requests_total{location}` | counter |
| `peretum_location_cached_requests_total{location}` | counter |
| `peretum_proxy_requests_total` | counter |
| `peretum_proxy_cached_requests_total` | counter |
| `peretum_proxy_active_requests` | gauge |
| `peretum_proxy_request_duration_seconds` | histogram |
| `peretum_cache_size_bytes` | gauge |
| `peretum_cache_entries` | gauge |
| `peretum_cache_hits_total` | counter |
| `peretum_cache_misses_total` | counter |
| `peretum_cache_evictions_total` | counter |
| `peretum_cache_expirations_total` | counter |

> **Wiring note:** the runtime request/cache path currently calls the
> cache-hit and cache-miss recorders. The remaining plugin hooks
> (`RecordRequest`, active-request gauge, histogram, eviction/expiration
> counters, cache size/entries) implement the full `MetricsHook` surface but are
> not yet invoked from the runtime path — they are exercised in tests.
