---
label: Cluster Mode
icon: cloud
order: 400
---

# Cluster Mode

Peretum includes **cluster mode** features for high-volume deployments across multiple edge nodes:

- **Lazy target loading** — target configs stay on disk (Pebble) and are compiled **on first request**
- **NATS JetStream event store** — the control plane publishes the current state of every target; edges consume it
- **Replicated state** — a new edge replays the stream to build its store, so no snapshot endpoint is needed

There is no gRPC control channel and no HTTP snapshot API. NATS JetStream is the only
transport, and it doubles as both the update feed and the state snapshot.

---

## Configuration

```yaml
cluster:
  enabled: true              # master switch for cluster features
  nats_uri: "nats://nats.example.com:4222"   # NATS JetStream URL(s)
  lazy: true                 # materialize target handlers on first request
  data_dir: "/var/lib/peretum/targetstore"   # Pebble store dir (default: <cache_dir>/targetstore)
  lru_size: 1000             # compiled-handler LRU capacity (default: 1000)
```

| Key | Type | Required | Description |
| --- | --- | --- | --- |
| `enabled` | bool | yes | Master switch for cluster features |
| `nats_uri` | string | no | NATS JetStream URL(s) for config sync |
| `lazy` | bool | no | Keep target configs off-RAM; compile on first request |
| `data_dir` | string | no | Pebble target store directory (default: `<cache_dir>/targetstore` or `.peretum/targetstore`) |
| `lru_size` | int | no | Compiled-handler LRU capacity for lazy mode (default: 1000) |

When `nats_uri` is set the edge **ignores its local `config.d`** entirely and takes
its state from JetStream. When it is unset the edge seeds itself from `config.d`
and does not run a consumer.

---

## Target Events

The control plane publishes one message per target to a single subject per hostname:

| Subject | Payload |
| --- | --- |
| `config.target.{server_name}` | `{"server_name": "...", "deleted": false, "config": {...}}` |
| `config.target.{server_name}` | `{"server_name": "...", "deleted": true, "config": null}` |

Upserts and deletes share the same subject, distinguished by the `deleted` field.
The event carries no version or hash: the retained message on the subject *is* the
current state, so an edge never has to reconcile an ordering counter.

```go
type TargetEvent struct {
    ServerName string          `json:"server_name"`
    Deleted    bool            `json:"deleted"`
    Config     json.RawMessage `json:"config,omitempty"`
    Timestamp  time.Time       `json:"timestamp"`
}
```

### Stream configuration

Created by both the control plane and each edge via `CreateOrUpdateStream`, so the
first process to connect establishes it:

```go
jetstream.StreamConfig{
    Name:              "config-sync",
    Subjects:          []string{"config.target.>"},
    Retention:         jetstream.LimitsPolicy,
    MaxMsgs:           -1,
    MaxAge:            0,   // never expire current state
    MaxBytes:          -1,
    MaxMsgsPerSubject: 1,   // keep only the newest event per target
    Discard:           jetstream.DiscardOld,
    Storage:           jetstream.FileStorage,
    Replicas:          1,   // raise for HA
}
```

**Why `MaxMsgsPerSubject: 1` is the whole design:**

- The stream is a **key-value state store**, not a log. History has no value to an
  edge that only wants the latest config.
- A **new edge replays from the beginning** (`DeliverAllPolicy`) and ends up with the
  current state of every target after a single pass. This replaces the old
  `config.snapshot` subject and the `/sync` HTTP pull.
- Growth is bounded by the number of targets, not by the number of changes, so the
  stream does not grow without limit.
- `MaxAge: 0` matters: a state store must not lose a target just because it has been
  quiet for a day. A delete is represented by a retained tombstone, not by expiry.

---

## Edge Consumers

Each edge creates one **durable** consumer and processes every target event:

```go
DeliverPolicy: jetstream.DeliverAllPolicy,
AckPolicy:     jetstream.AckExplicitPolicy,
Durable:       ConsumerName(),
```

- `DeliverAllPolicy` is what makes a fresh edge self-sufficient: it replays the
  retained state, writes it to Pebble, and starts serving without asking anyone.
- Messages are acked only after the config is persisted, so a crash mid-replay
  resumes rather than silently losing targets.
- Malformed messages are **terminated** (poison-message protection) instead of being
  left to loop forever.

### Consumer identity

Durable consumer names must be unique per edge, and stable across restarts, or two
edges will share one consumer and half the traffic.

`ConsumerName()` resolves in this order:

1. `PERETUM_EDGE_ID` environment variable
2. `os.Hostname()`

Set `PERETUM_EDGE_ID` explicitly whenever hostnames can collide — Kubernetes pods,
containers, or multiple edges per machine:

```bash
PERETUM_EDGE_ID=edge-us-east-1 peretum
```

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
- Incoming events are applied immediately: the target is persisted, evicted from the
  LRU, and re-routed on the next request.

### Lazy mode notes

- In lazy mode the router host key is the target **`server_name`**. The incoming Host
  header is normalized (lowercased, port and trailing dot stripped, IPv6 brackets
  handled) and matched directly against Pebble keys.
- Active health checks are only attached to **currently materialized** targets;
  evicted (cold) targets fall back to passive upstream health detection.
- A load that panics is converted into a 502 rather than crashing the server, and a
  request that is canceled while waiting for an in-flight load returns 408.
- Write durability: store writes use `pebble.NoSync` for throughput — configs are
  re-derivable from the stream, so a crash loses at most the last writes, which the
  next reconnect restores.

---

## Recommended: Public CDN Configuration

```yaml
cluster:
  enabled: true
  nats_uri: "nats://nats.example.com:4222"
  lazy: true
```

**What this gives you:**
- Full state replication at every edge node
- Target configs kept on disk, compiled on first request (bounded LRU)
- A new edge becomes useful on its own by replaying the stream
- Steady-state updates delivered over a single subject per target

---

## Control Plane

The control plane is a peretum node that watches `config.d/`, computes the diff, and
publishes the resulting target events to JetStream.

```bash
peretum controlplane \
  --nats nats://nats.example.com:4222 \
  --config-dir config.d \
  --http :9001
```

| Flag | Default | Description |
| --- | --- | --- |
| `--nats` | `nats://localhost:4222` | NATS JetStream URL(s), comma-separated for a cluster |
| `--config-dir` | `config.d` | Directory of target config files to watch |
| `--http` | `:9001` | HTTP listen address for health checks |

The control plane serves **health only** — there is no config-serving HTTP API:

| Endpoint | Description |
| --- | --- |
| `GET /health` | Liveness |
| `GET /readyz` | Readiness |

Multiple control plane nodes may run concurrently against the same JetStream cluster.
They are not leader-elected; they are expected to be configured with the same
`--config-dir`, and each publishes the events it observes.

---

## Deployment Architecture

### Public CDN (Full Replication / Lazy)

```
              ┌──────────────────────┐
              │     config.d/        │
              └───────────┬──────────┘
                          │ fsnotify diff
              ┌───────────▼──────────┐
              │    Control Plane    │
              └───────────┬──────────┘
                          │ publish config.target.{server_name}
              ┌───────────┴──────────┐
              │    NATS JetStream    │  MaxMsgsPerSubject: 1
              │  (current state)     │
              └───────────┬──────────┘
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
   ┌─────────┐      ┌─────────┐      ┌─────────┐
   │ Edge 1  │      │ Edge 2  │      │ Edge 3  │
   │ durable │      │ durable │      │ durable │
   │ consumer│      │ consumer│      │ consumer│
   │ Pebble  │      │ Pebble  │      │ Pebble  │
   │ + LRU   │      │ + LRU   │      │ + LRU   │
   └────┬────┘      └────┬────┘      └────┬────┘
        └─────────────────┴─────────────────┘
                          │
                 ┌────────▼────────┐
                 │   Upstream APIs │
                 └─────────────────┘
```

Every edge holds the full state on disk and serves cold configs off-RAM.

---

## Metrics

Cluster mode adds no metrics of its own. Request and cache metrics come from the
Prometheus plugin at `metrics_addr`:

```
peretum_target_requests_total{server_name}
peretum_target_cached_requests_total{server_name}
peretum_location_requests_total{server_name}
peretum_proxy_requests_total{server_name}
peretum_proxy_active_requests
peretum_proxy_request_duration_seconds
peretum_cache_size_bytes
peretum_cache_hits_total
peretum_cache_misses_total
peretum_cache_evictions_total
```

---

## Best Practices Summary

| Scenario | `cluster.enabled` | `lazy` | `nats_uri` |
|----------|-------------------|--------|-------------|
| Public CDN | `true` | ✅ | ✅ Required |
| Multi-tenant SaaS | `true` | ✅ | ✅ Required |
| Data residency (GDPR) | `true` | optional | ✅ Required |
| Single node | `false` | optional | — |
