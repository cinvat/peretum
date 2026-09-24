---
label: Cluster Mode
icon: cloud
order: 400
---

# Cluster Mode

Peretum includes **cluster mode** features for high-volume deployments across multiple edge nodes:

- **Lazy target loading** — target configs stay on disk (Pebble) and are compiled **on first request**
- **Delta reloads** — only changed targets are rebuilt on reload
- **Control plane sync** — NATS JetStream control plane with full config snapshots and incremental updates
- **Control plane HA** — multiple control plane nodes via NATS clustering

---

## Configuration

```yaml
cluster:
  enabled: true              # master switch for cluster features
  control_plane: "nats://nats.example.com:4222"   # NATS JetStream URL(s)
  lazy: true                 # materialize target handlers on first request
  data_dir: "/var/lib/peretum/targetstore"        # Pebble store dir (default: <cache_dir>/targetstore)
  lru_size: 1000             # compiled-handler LRU capacity (default: 1000)
```

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `enabled` | bool | yes | Master switch for cluster features |
| `control_plane` | string | no | NATS JetStream URL(s) for config sync |
| `lazy` | bool | no | Keep target configs off-RAM; compile on first request |
| `data_dir` | string | no | Pebble target store directory (default: `<cache_dir>/targetstore` or `.peretum/targetstore`) |
| `lru_size` | int | no | Compiled-handler LRU capacity for lazy mode (default: 1000) |

---

## Lazy Target Loading

For CDN-scale deployments (**up to millions of targets**) it is impractical to keep
every compiled target handler in memory. With `cluster.lazy: true`:

- **Target configs live in a Pebble key-value store on disk** (`data_dir`), so cold
  configs use **zero RAM**.
- On **first request** for a host, the config is read from the store and compiled into
  a target handler. Concurrent first requests are coalesced (single-flight) so the
  config is compiled exactly once.
- Compiled handlers are kept in an **LRU** (`lru_size`); evicted handlers are
  re-materialized from disk on the next request.
- Router stubs are built at startup from a fast disk scan (hostname → version),
  keeping startup time at seconds even with millions of targets.
- A new edge node with an empty store pulls the **full config snapshot** from the
  control plane's `config.snapshot` NATS subject on first launch. If no control plane
  is configured, the edge seeds its store from the local `config.d` directory.
- **Config streaming** updates from the control plane are applied immediately:
  updated targets are persisted, evicted from the LRU, and re-routed.

### Lazy mode notes

- In lazy mode the router host key is the target **`server_name`** (the Host header
  is matched directly against Pebble keys).
- Active health checks are only attached to **currently materialized** targets;
  evicted (cold) targets fall back to passive upstream health detection.
- Write durability: store writes use `pebble.NoSync` for throughput — configs are
  re-fetchable from the control plane, so a crash loses at most the last updates
  that a re-sync will restore.

---

## Recommended: Public CDN Configuration

```yaml
cluster:
  enabled: true
  control_plane: "nats://nats.example.com:4222"
  lazy: true
```

**What this gives you:**
- Full config replication at every edge node
- Target configs kept on disk, compiled on first request (bounded LRU)
- Fast startup even at 10M+ targets
- Full snapshot pull on new edge nodes, then delta streaming updates via NATS

---

## Control Plane

The control plane is a peretum node that watches `config.d/` and publishes config
snapshots and updates to NATS JetStream.

```bash
peretum controlplane \
  --listen :4222 \
  --config-dir config.d \
  --data-dir ./controlplane-data
```

NATS JetStream subjects:

| Subject | Description |
| --- | --- |
| `config.target.updated.{server_name}` | Single target update |
| `config.target.deleted.{server_name}` | Single target delete |
| `config.snapshot` | Full config snapshot request/response |
| `config.hot-targets` | Hot targets list request/response |

JetStream stream configuration (pre-configured by controlplane command):

```yaml
Name:              "config-sync"
Subjects:          ["config.target.>", "config.snapshot", "config.hot-targets"]
Retention:         LimitsPolicy
MaxMsgsPerSubject: 1            # Keep only latest per hostname
MaxAge:            24h
Discard:           DiscardOld
Storage:           FileStorage
Replicas:          1
```

For HA, run multiple control plane nodes with NATS clustering:

```bash
# Control plane 1
peretum controlplane --listen :4222 --cluster nats://cp1:4222,nats://cp2:4222,nats://cp3:4222

# Control plane 2
peretum controlplane --listen :4222 --cluster nats://cp1:4222,nats://cp2:4222,nats://cp3:4222
```

---

## Deployment Architecture

### Public CDN (Full Replication / Lazy)

```
                    ┌─────────────────┐
                    │  Control Plane  │
                    │  (NATS JetStream)│
                    └────────┬────────┘
                             │ config.target.updated.* + config.snapshot
           ┌─────────────────┼─────────────────┐
           ▼                 ▼                 ▼
      ┌─────────┐      ┌─────────┐      ┌─────────┐
      │ Edge 1  │      │ Edge 2  │      │ Edge 3  │
      │ Pebble  │      │ Pebble  │      │ Pebble  │
      │ + LRU   │      │ + LRU   │      │ + LRU   │
      └────┬────┘      └────┬────┘      └────┬────┘
           │                │                │
           └────────────────┴────────────────┘
                             │
                    ┌────────▼────────┐
                    │   Upstream APIs │
                    └─────────────────┘
```

All edges keep full config snapshots on disk and serve cold configs off-RAM.

---

## NATS JetStream Stream Configuration

The control plane creates a JetStream stream with this precise configuration:

```go
StreamConfig{
    Name:              "config-sync",
    Subjects:          []string{"config.target.>", "config.snapshot", "config.hot-targets"},
    Retention:         jetstream.LimitsPolicy,
    MaxMsgs:           -1,
    MaxAge:            24 * time.Hour,
    MaxBytes:          -1,
    MaxMsgsPerSubject: 1,                 // Keep only latest per hostname
    Discard:           jetstream.DiscardOld,
    Storage:           jetstream.FileStorage,
    Replicas:          1,                 // Increase for HA
}
```

**Key settings rationale:**
- `MaxMsgsPerSubject: 1` — Only the latest target config per hostname is kept
- `DiscardOld` — When a new update arrives, the old one is discarded automatically
- `LimitsPolicy` — Enforces the per-subject limit strictly
- `FileStorage` — Persistent storage, survives restarts

---

## Metrics

Prometheus-compatible metrics exposed at `metrics_addr`:

```
# Reload metrics
peretum_reload_total{type="delta|full"}
peretum_reload_duration_ns
peretum_reload_errors_total

# Target metrics
peretum_targets_loaded_total
peretum_targets_changed_total
peretum_targets_deleted_total

# NATS sync metrics
peretum_nats_updates_received_total
peretum_nats_deletes_received_total
peretum_nats_snapshot_requests_total

# Request latency
peretum_request_latency_bucket{le="1ms|5ms|10ms|50ms|100ms|500ms|1s|5s|10s|+Inf"}
```

---

## Best Practices Summary

| Scenario | `cluster.enabled` | `lazy` | `control_plane` |
|----------|-------------------|--------|-----------------|
| Public CDN | `true` | ✅ | ✅ Required |
| Multi-tenant SaaS | `true` | ✅ | ✅ Required |
| Data residency (GDPR) | `true` | optional | ✅ Required |