---
label: CDN Scale
icon: cloud
order: 400
---

# CDN Scale Features

Peretum includes CDN-scale features for high-volume, multi-tenant deployments
across multiple edge nodes. When enabled, these features provide:

- **Delta reloads** — only changed targets are rebuilt on reload
- **Tenant sharding** — consistent hashing distributes tenants across edge nodes
- **Tiered config storage** — hot/warm/cold tiers with LRU eviction
- **Lazy loading** — cold tenants loaded on first request
- **Config streaming** — xDS-style gRPC config streaming from control plane
- **Metrics** — Prometheus-compatible metrics for all CDN operations

## Enabling CDN Scale

Add the `cdn_scale` section to your `config.yaml`:

```yaml
cdn_scale:
  enabled: true
  node_id: "edge-us-east-1"           # unique ID for this edge node
  total_nodes: 5                      # total number of edge nodes
  replica_factor: 3                   # replication factor for sharding
  control_plane: "control-plane.example.com:9001"  # gRPC control plane address
```

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `enabled` | bool | yes | Master switch for CDN scale features |
| `node_id` | string | yes | Unique ID for this edge node (used for sharding) |
| `total_nodes` | int | yes | Total number of edge nodes in the cluster |
| `replica_factor` | int | no | Replication factor for sharding (default: 3) |
| `control_plane` | string | no | gRPC control plane address for config streaming |

## Features

### Delta Reloads

When CDN scale is enabled, `peretum -r` (or `SIGHUP`) performs a **delta reload**:

1. Loads new config
2. Computes SHA256 hashes for each target
3. Only rebuilds targets whose hash changed
4. Updates router atomically

This avoids rebuilding the entire router when only one target changes.

### Tenant Sharding

Consistent hashing with virtual nodes distributes tenants across edge nodes:

- Each target is assigned to a primary edge node + replicas
- Uses CRC32 with virtual nodes (150 per replica by default)
- `node_id` and `total_nodes` determine placement

```yaml
cdn_scale:
  node_id: "edge-us-east-1"
  total_nodes: 5
  replica_factor: 3
```

### Tiered Config Storage

Three-tier storage for memory efficiency:

| Tier | Storage | Retention | Use Case |
| --- | --- | --- | --- |
| **Hot** | In-memory LRU | Always | Frequently accessed tenants |
| **Warm** | Serialized (compressed) | 5 min idle | Recently accessed |
| **Cold** | Disk/Control plane | Indefinite | Rarely accessed |

- Hot tier: LRU cache (default 10,000 entries)
- Auto-promotion: tenants with >10 accesses promoted to hot
- Auto-eviction: cold tenants evicted after 5 min idle

### Lazy Loading

Cold tenants are loaded on first request:
1. Request arrives for cold tenant
2. Config loaded from warm tier (or control plane)
3. Promoted to hot tier
4. Subsequent requests served from hot tier

### Config Streaming (xDS-style)

Connect to a gRPC control plane for real-time config updates:

```yaml
cdn_scale:
  control_plane: "control-plane.example.com:9001"
```

Features:
- Bidirectional streaming (edge ↔ control plane)
- Full snapshot on connect, then delta updates
- Automatic reconnection with exponential backoff
- Versioned snapshots for consistency

Run the control plane:
```bash
peretum controlplane --listen :9001 --config-dir config.d --data-dir ./controlplane-data
```

### Metrics

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

# Cache tier metrics
peretum_cache_hot_hits_total
peretum_cache_hot_misses_total
peretum_cache_warm_hits_total
peretum_cache_warm_misses_total
peretum_evictions_total

# Streaming metrics
peretum_stream_connects_total
peretum_stream_disconnects_total
peretum_stream_updates_total
peretum_stream_errors_total

# Request latency
peretum_request_latency_bucket{le="1ms|5ms|10ms|50ms|100ms|500ms|1s|5s|10s|+Inf"}
```

## Running the Control Plane

Start the control plane on a dedicated node:

```bash
peretum controlplane \
  --listen :9001 \
  --config-dir config.d \
  --data-dir ./controlplane-data
```

The control plane:
- Watches `config.d/` for changes
- Streams updates to connected edges via gRPC
- Serves full snapshots on new connections
- Maintains client state and version tracking

## Deployment Architecture

```
                    ┌─────────────────┐
                    │  Control Plane  │
                    │  (config source)│
                    └────────┬────────┘
                             │ gRPC streaming
           ┌─────────────────┼─────────────────┐
           ▼                 ▼                 ▼
      ┌─────────┐      ┌─────────┐      ┌─────────┐
      │ Edge 1  │      │ Edge 2  │      │ Edge 3  │
      │ node_id │      │ node_id │      │ node_id │
      └────┬────┘      └────┬────┘      └────┬────┘
           │                │                │
           └────────────────┴────────────────┘
                             │
                    ┌────────▼────────┐
                    │   Upstream APIs │
                    └─────────────────┘
```

Each edge node:
1. Connects to control plane on startup
2. Receives full config snapshot
3. Receives delta updates in real-time
4. Serves traffic for its sharded tenants
5. Reports metrics to Prometheus