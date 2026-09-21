---
label: Cluster Mode
icon: cloud
order: 400
---

# Cluster Mode

Peretum includes **cluster mode** features for high-volume deployments across multiple edge nodes:

- **Delta reloads** — only changed targets are rebuilt on reload
- **Lazy loading** — configs loaded on first request, not at startup
- **Tiered config storage** — hot/warm/cold tiers with LRU eviction
- **Config streaming** — xDS-style gRPC config streaming from control plane
- **Control plane HA** — active-standby leader election with config sync

---

## When to Enable Cluster Mode

| Use Case | `cluster.enabled` |
|----------|-------------------|
| **Public CDN** (full replication) | `false` (default) |
| **Multi-tenant platform** (strict isolation) | `true` |
| **Data residency** (GDPR, China) | `true` |

---

## Configuration

```yaml
cluster:
  enabled: false           # true = enable sharding, false = full replication
  replica_factor: 3           # replication factor for control plane HA (default: 3)
  control_plane: "control-plane.example.com:9001"  # gRPC control plane address
```

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `enabled` | bool | yes | Master switch for cluster features. `false` = full replication (recommended for CDN) |
| `replica_factor` | int | no | Replication factor for control plane HA (default: 3) |
| `control_plane` | string | no | gRPC control plane address for config streaming |

---

## Recommended: Public CDN Configuration

For a public CDN with full replication:

```yaml
cluster:
  enabled: false
  control_plane: "control-plane.example.com:9001"
```

**What this gives you:**
- **Full cache replication** at every edge node
- **Delta reloads** - only changed targets rebuilt on SIGHUP
- **Lazy loading** - configs loaded on first request, not at startup
- **Tiered storage** - hot/warm/cold with LRU eviction
- **Config streaming** - real-time updates from control plane
- **Control plane HA** - active-standby with automatic failover

---

## Core Features (Work With or Without Sharding)

### Delta Reloads

`peretum -r` (or `SIGHUP`) performs a **delta reload**:

1. Loads new config
2. Computes SHA256 hashes for each target
3. Only rebuilds targets whose hash changed
4. Updates router atomically

This avoids rebuilding the entire router when only one target changes.

### Lazy Loading

Configs loaded on **first request**, not at startup:

- Startup time: 30s → <1s (10K+ targets)
- Memory: 2GB → 200MB (only hot configs in RAM)

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

### Config Streaming (xDS-style)

Connect to a gRPC control plane for real-time config updates:

```yaml
cluster:
  control_plane: "control-plane.example.com:9001"
```

Features:
- Full snapshot on connect, then delta updates
- Automatic reconnection with exponential backoff
- Versioned snapshots for consistency

Run the control plane:
```bash
peretum controlplane \
  --listen :9001 \
  --config-dir config.d \
  --data-dir ./controlplane-data
```

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
- Active-standby HA with automatic failover

---

## Deployment Architecture

### Public CDN (Full Replication)

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
      │ Full    │      │ Full    │      │ Full    │
      │ Cache   │      │ Cache   │      │ Cache   │
      └────┬────┘      └────┬────┘      └────┬────┘
           │                │                │
           └────────────────┴────────────────┘
                             │
                    ┌────────▼────────┐
                    │   Upstream APIs │
                    └─────────────────┘
```

All edges receive full config and have full cache. Control plane streams delta updates.

---

## Best Practices Summary

| Scenario | `cluster.enabled` | `control_plane` |
|----------|-------------------|-----------------|
| Public CDN | `false` | ✅ Recommended |
| Multi-tenant SaaS | `true` | ✅ Required |
| Data residency (GDPR) | `true` | ✅ Required |
| Hybrid | `true` (some edges) | ✅ Recommended |

The control plane and streaming updates work **independently of sharding** — you get real-time config updates regardless of `cluster.enabled`.

### Delta Reloads

`peretum -r` (or `SIGHUP`) performs a **delta reload**:

1. Loads new config
2. Computes SHA256 hashes for each target
2. Only rebuilds targets whose hash changed
3. Updates router atomically

### Lazy Loading

Configs loaded on **first request**, not at startup:

- Startup time: 30s → <1s (10K+ targets)
- Memory: 2GB → 200MB (only hot configs in RAM)

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

### Config Streaming (xDS-style)

Connect to a gRPC control plane for real-time config updates:

```yaml
cluster:
  control_plane: "control-plane.example.com:9001"
```

Features:
- Full snapshot on connect, then delta updates
- Automatic reconnection with exponential backoff
- Versioned snapshots for consistency
- Works with or without sharding

Run the control plane:
```bash
peretum controlplane \
  --listen :9001 \
  --config-dir config.d \
  --data-dir ./controlplane-data
```

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

---

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

---

## Deployment Architecture

### Public CDN (Sharding Disabled)

```
                    ┌─────────────────┐
                    │  Control Plane  │
                    │  (config source)│
                    └────────┬────────┘
                             │ gRPC streaming (delta updates)
           ┌─────────────────┼─────────────────┐
           ▼                 ▼                 ▼
      ┌─────────┐      ┌─────────┐      ┌─────────┐
      │ Edge 1  │      │ Edge 2  │      │ Edge 3  │
      │ Full    │      │ Full    │      │ Full    │
      │ Cache   │      │ Cache   │      │ Cache   │
      └────┬────┘      └────┬────┘      └────┬────┘
           │                │                │
           └────────────────┴────────────────┘
                             │
                    ┌────────▼────────┐
                    │   Upstream APIs │
                    └─────────────────┘
```

All edges receive full config and have full cache. Control plane streams delta updates.

---

### Multi-Tenant Platform (Sharding Enabled)

```
                    ┌─────────────────┐
                    │  Control Plane  │
                    └────────┬────────┘
                             │ gRPC streaming
           ┌─────────────────┼─────────────────┐
           ▼                 ▼                 ▼
      ┌─────────┐      ┌─────────┐      ┌─────────┐
      │ Edge 1  │      │ Edge 2  │      │ Edge 3  │
      │ Tenant  │      │ Tenant  │      │ Tenant  │
      │ A, B    │      │ B, C    │      │ C, A    │
      └────┬────┘      └────┬────┘      └────┬────┘
           │                │                │
           └────────────────┴────────────────┘
                             │
                    ┌────────▼────────┐
                    │   Upstream APIs │
                    └─────────────────┘
```

Each edge serves only its assigned tenants. Control plane assigns tenants via consistent hashing.

---

## Best Practices Summary

| Scenario | `cluster.enabled` | `control_plane` |
|----------|-------------------|-----------------|
| Public CDN | `false` | ✅ Recommended |
| Multi-tenant SaaS | `true` | ✅ Required |
| Data residency (GDPR) | `true` | ✅ Required |
| Hybrid | `true` (some edges) | ✅ Recommended |

The control plane and streaming updates work **independently of sharding** — you get real-time config updates regardless of `cluster.enabled`.