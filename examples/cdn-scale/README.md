# CDN Scale Demo

A complete Docker Compose setup to demonstrate Peretum's CDN-scale features on a single machine.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Control Plane                            │
│  (Config Distribution - gRPC :9001)                             │
└──────────────────────────┬──────────────────────────────────────┘
                           │ gRPC streaming
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│  Edge US East │   │ Edge US West  │   │ Edge EU Cent  │
│  :8080/:8443  │   │  :8081/:8444  │   │  :8082/:8445  │
│  Metrics:9090 │   │ Metrics:9091  │   │ Metrics:9092  │
└───────┬───────┘   └───────┬───────┘   └───────┬───────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            ▼
              ┌───────────────────────────┐
              │      Upstream Services    │
              │  api-1:8001  api-2:8002   │
              │  static:8003              │
              └───────────────────────────┘
```

## Quick Start

```bash
cd examples/cdn-scale

# Start the entire cluster
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f control-plane
docker compose logs -f edge-us-east-1
```

## Services

| Service | Ports | Description |
|---------|-------|-------------|
| control-plane | 9001 | gRPC config distribution |
| edge-us-east-1 | 8080/8443/9090 | Edge node 1 (US East) |
| edge-us-west-1 | 8081/8444/9091 | Edge node 2 (US West) |
| edge-eu-central-1 | 8082/8445/9092 | Edge node 3 (EU Central) |
| upstream-api-1 | 8001 | Primary API origin |
| upstream-api-2 | 8002 | Secondary API origin |
| upstream-static | 8003 | Static assets origin |
| prometheus | 9090 | Metrics collection |
| grafana | 3000 | Visualization (admin/admin) |

## Testing

### Test cache hit/miss
```bash
# First request - MISS
curl -I http://localhost:8080/api/users

# Second request - HIT (cached)
curl -I http://localhost:8080/api/users
```

### Test live streaming cache bypass
```bash
# These paths are excluded from cache (cache_excludes)
curl -I http://localhost:8080/live/stream.m3u8
curl -I http://localhost:8080/live/segment_001.ts
# Both return Cache-Control: no-store
```

### Test cache hit header
```bash
curl -I http://localhost:8080/api/users
# X-Cache: HIT
# X-Cache-Status: enabled
```

### Test health checks
```bash
# Upstream health
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

### View metrics
```bash
# Prometheus
open http://localhost:9090

# Grafana (admin/admin)
open http://localhost:3000
# Import dashboard: peretum-overview.json
```

### Test control plane
```bash
# Check connected edges
curl -s http://localhost:9001/stats | jq
```

## Configuration

Key configuration files:

| File | Description |
|------|-------------|
| `config.yaml` | Global proxy config with CDN scale settings |
| `config.d/api.yaml` | API target with load balancing, WAF, cache |
| `config.d/static.yaml` | Static assets with cache_excludes for HLS |
| `config.d/fallback.yaml` | Fallback target (no cache) |

### CDN Scale Config (config.yaml)

```yaml
cdn_scale:
  enabled: true
  node_id: "edge-us-east-1"      # Unique per edge
  total_nodes: 3                  # Cluster size
  replica_factor: 2               # Replication
  control_plane: "control-plane:9001"
```

## Sharding Demo

Each edge node gets different targets based on consistent hashing:

```bash
# Check which targets each edge serves
curl -s http://localhost:9090/metrics | grep peretum_targets_loaded
curl -s http://localhost:9091/metrics | grep peretum_targets_loaded
curl -s http://localhost:9092/metrics | grep peretum_targets_loaded
```

## Cleanup

```bash
# Stop and remove everything
docker compose down -v

# Or just stop
docker compose stop
```