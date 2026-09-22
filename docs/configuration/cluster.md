---
label: Cluster Mode
icon: cloud
order: 400
---

# Cluster Mode

Peretum includes **cluster mode** features for high-volume deployments across multiple edge nodes:

- **Lazy target loading** — target configs stay on disk (Pebble) and are compiled **on first request**
- **Delta reloads** — only changed targets are rebuilt on reload
- **Control plane** — HTTP control plane with leader election and full config snapshots (`/sync`)
- **Config streaming** — push updates from the control plane to connected edges
- **Control plane HA** — active-standby leader election with automatic failover

---

## Configuration

```yaml
cluster:
  enabled: true              # master switch for cluster features
  replica_factor: 3          # replication factor for control plane HA (default: 3)
  control_plane: "control-plane.example.com:9001"
  lazy: true                 # materialize target handlers on first request
  data_dir: "/var/lib/peretum/targetstore"   # Pebble store dir (default: <cache_dir>/targetstore)
  lru_size: 1000             # compiled-handler LRU capacity (default: 1000)
```

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `enabled` | bool | yes | Master switch for cluster features |
| `replica_factor` | int | no | Replication factor for control plane HA (default: 3) |
| `control_plane` | string | no | Control plane address (`host:port`, comma-separated for HA) |
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
- Router stubs are built at startup from a fast disk scan (target name → version),
  keeping startup time at seconds even with millions of targets.
- A new edge node with an empty store pulls the **full config snapshot** from the
  control plane's `/sync` endpoint on first launch. If no control plane is
  configured, the edge seeds its store from the local `config.d` directory.
- **Config streaming** updates from the control plane are applied immediately:
  updated targets are persisted, evicted from the LRU, and re-routed.

### Lazy mode notes

- In lazy mode the router host key is the target **name** (the `listen` address is
  ignored for un-compiled stubs).
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
  control_plane: "control-plane.example.com:9001"
  lazy: true
```

**What this gives you:**
- Full config replication at every edge node
- Target configs kept on disk, compiled on first request (bounded LRU)
- Fast startup even at 10M+ targets
- Full snapshot pull on new edge nodes, then delta streaming updates
- Control plane HA with automatic failover

---

## Control Plane

The control plane is a peretum node that watches `config.d/` and serves config
snapshots over HTTP, plus a leader election layer.

```bash
peretum controlplane \
  --listen :9001 \
  --config-dir config.d \
  --data-dir ./controlplane-data
```

Endpoints (exposed on `:9001`):

| Endpoint | Description |
| --- | --- |
| `/health` | Liveness probe |
| `/health/leader` | Returns 200 only for the current leader |
| `/leadership/claim` | Attempt to claim leadership |
| `/leadership/resign` | Resign leadership |
| `/sync` | Full config snapshot (JSON; leader only) |
| `/sync/target` | Snapshot for a single target |
| `/config/reload` | Trigger a config reload/diff |
| `/stats` | Snapshot stats |
| `/peer/notify` | Peer change notifications |

For HA, list multiple control plane addresses in `control_plane`:

```yaml
cluster:
  control_plane: "cp1.example.com:9001,cp2.example.com:9001,cp3.example.com:9001"
```

The node members elect a leader via the `/leadership` endpoints; only the leader
serves `/sync` snapshots and streams updates.

---

## Deployment Architecture

### Public CDN (Full Replication / Lazy)

```
                    ┌─────────────────┐
                    │  Control Plane  │
                    │  (config source)│
                    └────────┬────────┘
                             │ /sync snapshot + streaming
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

# Streaming metrics
peretum_stream_connects_total
peretum_stream_disconnects_total
peretum_stream_updates_total
peretum_stream_errors_total

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