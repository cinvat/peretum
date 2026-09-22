---
label: Global configuration (config.yaml)
order: 300
---

# Global configuration (`config.yaml`)

```yaml
# multi-listener frontends (takes precedence over `listen`).
# Each entry: address[:port] [flags]; flags: ssl, h2c, h2/http2, h3/http3/quic.
# A bare host or port defaults to 443 for ssl/h3 listeners, 80 otherwise.
listeners:
  - ":80"            # plaintext; HTTP/1.1 + h2c
  - ":443 ssl h2 h3"      # HTTPS; HTTP/1.1 + HTTP/2 via ALPN + HTTP/3 (UDP) on the same port (implies TLS)
  - ":8443 ssl quic"     # HTTPS + HTTP/3 on the same port
  - ":8443 h3"           # HTTP/3 (UDP) on the same port (implies TLS)

cache_dir: "./cache"
max_cache_size: "500MB"          # size string; 0 = unlimited
max_cache_age: "24h"             # Go duration; 0 = no expiration
max_write_workers: 8             # -1 = unlimited; 0 = default (8)
max_response_body_size: "50MB"   # -1 = unlimited; empty = default (50MB)

# tls_cert_file: "./certs/server.crt"   # global HTTPS certificate
# tls_key_file: "./certs/server.key"

metrics_addr: ":9090"            # enables the prometheus_exporter plugin

json_log:
  enabled: true
  access_log: "./logs/access.jsonl"
  error_log: "./logs/error.jsonl"
  stdout: true

error_page:
  enabled: true
  statuses: [403, 404, 500, 502, 503, 504]   # default: [404, 500, 502, 503, 504]

waf:
  enabled: true
  geolite_dir: "./plugins/waf/geolite"       # shared by every location's WAF
  max_body_size: "1MB"                       # max request body to read for "body" param

cluster:
  enabled: true
  replica_factor: 3                   # replication factor for control plane HA
  control_plane: "control-plane.example.com:9001"  # control plane address (HTTP :9001)
  lazy: true                          # keep target configs on disk, compile on first request
  lru_size: 1000                      # compiled-handler LRU capacity
```

## Reference

| Key | Type | Description |
| --- | --- | --- |
| `listen` | string | Legacy single-address shorthand; ignored while `listeners` is set. |
| `listeners` | []string | nginx-style frontends (see [Listeners](listeners.md)). |
| `http3_addr` | string | Legacy HTTP/3 (QUIC/UDP) address; implied when TLS is active. |
| `cache_dir` | string | Root directory for the disk cache (created if missing). |
| `max_cache_size` | size string | Total cache bytes; suffixes `B/KB/MB/GB` (binary, float prefixes allowed). `0` = unlimited. |
| `max_cache_age` | duration | Entry lifetime. `0` = no time-based expiration. |
| `max_write_workers` | int | Max concurrent cache writes. `-1` = unlimited, `0` → 8, `>0` = cap. When the pool is full, cache writes for a request are skipped (`write pool full`). |
| `max_response_body_size` | size string | Largest response body buffered before proxying. `-1` = unlimited. |
| `tls_cert_file` / `tls_key_file` | string | Global TLS certificate. |
| `metrics_addr` | string | Enables `prometheus_exporter` on this address. |
| `json_log` | object | See [JSON logs](../plugins/jsonlog.md). |
| `error_page` | object | See [Error pages](../plugins/errorpage.md). |
| `waf` | object | Global WAF settings: `enabled`, `geolite_dir`, `max_body_size` (see [WAF](../plugins/waf.md)). |
| `cluster` | object | Cluster settings: `enabled`, `replica_factor`, `control_plane`, `lazy`, `data_dir`, `lru_size` (see [Cluster Mode](../configuration/cluster.md)). |

## Size strings

`max_cache_size` and `max_response_body_size` accept binary suffixes with
optional fractional prefixes, e.g. `"500MB"`, `"1.5GB"`, `"1024KIB"`. The
longer/special suffixes (`GIB`, `MIB`, `KIB`) are checked first so `"B"` never
swallows `"MB"`.

## Durations

`max_cache_age` and per-location `cache_ttl` use Go `time.ParseDuration`
syntax: `30s`, `5m`, `1h30m`, `24h`.