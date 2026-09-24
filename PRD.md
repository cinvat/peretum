# Product Requirements Document: Peretum CDN Reverse Proxy

## 1. Executive Summary

**Product Name:** Peretum  
**Version:** 1.0 (CDN-Scale Release)  
**Type:** High-performance CDN reverse proxy with lazy target loading  
**Target Audience:** Infrastructure teams operating public CDNs, multi-tenant platforms, and data-residency-compliant deployments  
**Core Value Proposition:** Handle 10M+ target configurations with sub-second startup, bounded memory via on-disk Pebble storage, and incremental control-plane updates.

---

## 2. Problem Statement

### Current Pain Points
- **Traditional proxies** load all target configs into memory at startup → OOM at scale
- **Full router rebuilds** on any config change → latency spikes, dropped connections
- **No cold/hot separation** → rarely-used tenants consume same RAM as hot ones
- **Control-plane coupling** → edges need full config push, no incremental sync

### Market Opportunity
- Public CDNs (Cloudflare, Fastly, Akamai scale patterns)
- Multi-tenant SaaS platforms (strict isolation per tenant)
- GDPR/China data residency (per-region edge clusters)
- Hybrid cloud (on-prem + cloud edge nodes)

---

## 3. Target Users & Use Cases

| Persona | Primary Need | Success Metric |
|---------|--------------|----------------|
| **CDN Platform Engineer** | 10M+ targets, <1s startup, <2GB RAM | P99 startup <1s at 10M targets |
| **SaaS Infra Lead** | Per-tenant isolation, config streaming | Zero config leakage between tenants |
| **Compliance Engineer** | Data residency, audit trails | Config never leaves region; audit log |
| **SRE** | Graceful reload, observability | Zero-downtime reloads; P99 <50ms reload |

---

## 4. Functional Requirements

### 4.1 Core Proxy (MVP - Complete)
- [x] HTTP/1.1, HTTP/2 (h2/h2c), HTTP/3 (QUIC) support
- [x] TLS termination with SNI, auto-generated self-signed certs
- [x] Multiple listeners with nginx-style flags (`:443 ssl h2 h3`)
- [x] Load balancing: Round Robin, Weighted RR, Maglev, Least Connections
- [x] Passive + active health checks (configurable interval/timeout/path)
- [x] Disk cache with LRU eviction, size/age limits, write workers
- [x] Plugin architecture: WAF, CORS, Compression, Rewrite, Headers, Error Pages, JSON Log, Prometheus

### 4.2 CDN-Scale Cluster Mode (Complete)
- [x] **Lazy Target Loading**: Target configs stay on disk (Pebble); compiled handlers materialize on first request
- [x] **Bounded LRU**: Compiled handlers cached up to `cluster.lru_size` (default 1000); eviction returns to cold storage
- [x] **Single-Flight Coalescing**: Concurrent first requests for same host compile exactly once
- [x] **Pebble Store**: Atomic batch writes (config + version), `NoSync` for throughput, fast restart via `v/` prefix scan
- [x] **Control-Plane Pull**: New edge pulls full snapshot via NATS `config.snapshot` on first launch; falls back to local `config.d`
- [x] **Incremental Updates**: `applyTargetUpdate`/`applyTargetDelete` persist to store, evict LRU, upsert router stub via NATS JetStream
- [x] **NATS JetStream Sync**: `config.target.updated.*`, `config.target.deleted.*`, `config.snapshot`, `config.hot-targets`
- [x] **JetStream Stream**: `LimitsPolicy`, `MaxMsgsPerSubject: 1`, `DiscardOld`, `FileStorage`, 24h max age
- [x] **Delta Reloads**: SIGHUP triggers config diff; only changed targets rebuild (SHA-256 based)

### 4.3 Configuration (Complete)
```yaml
cluster:
  enabled: true              # master switch
  control_plane: "nats://nats.example.com:4222"   # NATS JetStream URL
  lazy: true                 # enable on-disk + LRU
  data_dir: "/var/lib/peretum/targetstore"  # Pebble dir
  lru_size: 1000             # compiled-handler LRU capacity
```

### 4.4 Observability (Complete)
- [x] Prometheus metrics at `metrics_addr`:
  - Reload: `peretum_reload_total{type="delta|full"}`, `peretum_reload_duration_ns`, `peretum_reload_errors_total`
  - Targets: `peretum_targets_loaded_total`, `peretum_targets_changed_total`, `peretum_targets_deleted_total`
  - NATS sync: `peretum_nats_updates_received_total`, `peretum_nats_deletes_received_total`, `peretum_nats_snapshot_requests_total`
  - Request latency histogram: `peretum_request_latency_bucket`
- [x] Structured JSON access/error logs
- [x] Health endpoints: `/health`, `/health/leader`

---

## 5. Non-Functional Requirements

| Category | Requirement | Target |
|----------|-------------|--------|
| **Performance** | Startup time (10M targets) | <1 second |
| **Performance** | Memory (cold configs) | 0 bytes RAM (on-disk only) |
| **Performance** | Memory (hot handlers) | Bounded by `lru_size` × handler size (~50KB each → 50MB at 1000) |
| **Performance** | First-request latency (cold) | <100ms (materialize + compile) |
| **Performance** | First-request latency (warm) | <1ms (LRU hit) |
| **Reliability** | Zero-downtime reload | SIGHUP → atomic router swap |
| **Reliability** | Config durability | `NoSync` writes; re-fetchable from control plane |
| **Reliability** | Graceful shutdown | 30s drain, health checker stop, store flush |
| **Scalability** | Target count | 10M+ per edge |
| **Scalability** | Control-plane HA | 3+ nodes, auto-failover |
| **Security** | TLS 1.2+ | Mandatory for HTTPS/QUIC listeners |
| **Security** | WAF | Per-location DNF rules, GeoIP |
| **Operability** | Config validation | `peretum -t` (syntax + semantic) |
| **Operability** | Documentation | Retype docs, 27 pages, auto-deployed |

---

## 6. Architecture

### 6.1 High-Level Components

```
┌─────────────────────────────────────────────────────────────────┐
│                        Control Plane                            │
│  (peretum controlplane --listen :4222 --config-dir config.d)   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ fsnotify    │  │ Config      │  │ NATS JetStream :4222    │  │
│  │ Watcher     │──│VersionStore │──│ config.target.updated.* │  │
│  └─────────────┘  └─────────────┘  │ config.snapshot         │  │
│                                    │ config.hot-targets      │  │
│                                    └─────────────────────────┘  │
└──────────────────────────────┬──────────────────────────────────┘
                               │ config.target.updated.* + config.snapshot
 ┌─────────────────────────────┼──────────────────────────────────┐
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                          Edge Node                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Pebble Store │  │ Lazy LRU     │  │ HostRouter           │  │
│  │ (h/<host>,   │◄─┤ (1000 cap)   │◄─┤ (LazyHandler stubs)  │  │
│  │  v/<host>)   │  └──────────────┘  └──────────┬───────────┘  │
│  └──────┬───────┘                                │            │
│         │ materialize                             ▼            │
│         │                            ┌──────────────────────┐  │
│         └───────────────────────────►│ TargetConfigHandler  │  │
│                                      │ (per-location handlers│  │
│                                      │  + DefaultLoc)       │  │
│                                      └──────────┬───────────┘  │
│                                                 │              │
│                                      ┌──────────▼───────────┐  │
│                                      │ LoadBalancer         │  │
│                                      │ (RR/WRR/Maglev/LC)   │  │
│                                      └──────────┬───────────┘  │
│                                                 │              │
│                                      ┌──────────▼───────────┐  │
│                                      │ Upstream Pool        │  │
│                                      │ (HealthChecker)      │  │
│                                      └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 Data Flow

1. **Startup (Lazy Edge)**
   - Open Pebble store at `data_dir`
   - `ListTargets()` → fast `v/` prefix scan → name→version map
   - Build `HostRouter` with `LazyHandler` stubs per name
   - If store empty: `GET /sync` from control plane → `PutTarget` each
   - If control plane unreachable: seed from local `config.d` (sha256 version)

2. **First Request (Cold Host)**
   - `LazyHandler.ServeHTTP` → LRU miss
   - Coalesce concurrent requests via `inFlight` map
   - `materialize()`: `GetTarget` → YAML unmarshal → `buildTargetConfigHandler` → LRU `Put`
   - Serve request from newly-compiled handler

3. **Subsequent Requests (Warm Host)**
   - LRU hit → direct `TargetConfigHandler.ServeHTTP`

4. **Control-Plane Update**
   - `POST /sync/target` or streaming push → `applyTargetUpdate`
   - `PutTarget` (atomic) → `lazyLRU.Delete(key)` → `lazyRouter.Upsert(key, new LazyHandler)`
   - Next request materializes fresh config

5. **SIGHUP Reload**
   - Reload `config.yaml` + `config.d`
   - `ConfigVersionStore.LoadTargets` → delta diff (SHA-256)
   - `buildHostRouter` (eager or lazy) → `router.Reload` (atomic)

---

## 7. Technical Decisions & Trade-offs

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| **Pebble over BoltDB/RocksDB** | Pure Go, no CGO, ACID batches, prefix scan | Slightly higher write latency than RocksDB |
| **NoSync writes** | Configs re-fetchable; throughput critical | Potential loss of last N writes on crash (mitigated by `/sync` re-pull) |
| **LRU on compiled handlers, not raw config** | Compilation is expensive (regex, template, plugin chain); raw YAML is small | Memory per entry higher (~50KB vs ~1KB) |
| **Host key = target name (lazy mode)** | Simpler; `listen` override ignored for stubs | Less flexible than full host routing |
| **No active health checks for lazy targets** | Lifecycle complexity on eviction; passive detection sufficient | Slightly slower failover for cold targets |
| **fsnotify over polling** | Instant config reload, lower CPU | Platform-dependent; falls back to 5s polling |
| **SHA-256 for versioning** | Content-addressable; deterministic | 32-byte overhead per version key |

---

## 8. Release Criteria

### Must-Have (Release Gate)
- [x] All gates pass: `gofmt`, `go vet`, `go build`, `go test -race`
- [x] 22 packages, 100% pass rate
- [x] Documentation builds (27 pages, 0 errors)
- [x] Example cluster demo runs (`docker compose up`)

### Should-Have (Post-Launch)
- [ ] gRPC config streaming implementation (`ConfigStreamClient` wiring)
- [ ] Maglev table lazy rebuild (dirty flag)
- [ ] Per-target metrics (latency, error rate)
- [ ] Config validation CLI (`peretum -t` for cluster config)

### Nice-to-Have
- [ ] Admin API for runtime config inspection
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Rate limiting plugin
- [ ] mTLS between control plane and edges

---

## 9. Timeline & Milestones

| Milestone | Date | Status |
|-----------|------|--------|
| Core proxy (HTTP/1.1, TLS, cache) | 2025-Q3 | ✅ Done |
| Plugin architecture | 2025-Q3 | ✅ Done |
| Cluster mode (sharding, delta reload) | 2025-Q4 | ✅ Done |
| **Lazy loading + Pebble store** | **2026-Q3** | **✅ Done** |
| Control plane HTTP + HA | 2026-Q3 | ✅ Done |
| Code review hardening | 2026-Q3 | ✅ Done |
| Production hardening (gRPC stream, metrics) | 2026-Q4 | 📋 Planned |

---

## 10. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Pebble corruption on crash | Low | High | `NoSync` + re-pull from control plane; backup strategy |
| LRU eviction storm under load | Medium | Medium | Configurable `lru_size`; monitor `peretum_evictions_total` |
| Control plane single point of failure | Medium | High | 3-node HA with leader election; edge falls back to local config.d |
| Memory leak in plugin chain | Low | High | Plugin sandbox; `StopAllPlugins` on shutdown |
| SHA-256 collision | Negligible | Critical | 256-bit space; accept risk |

---

## 11. Appendix: Key Files

| File | Purpose |
|------|---------|
| `internal/cluster/targetstore.go` | Pebble-backed config store |
| `internal/router/lazy.go` | Single-flight materialization |
| `cmd/server.go` | Proxy lifecycle, lazy wiring |
| `cmd/controlplane.go` | HTTP control plane server |
| `internal/cluster/version.go` | Delta reload, tiered storage |
| `internal/loadbalancer/loadbalancer.go` | LB algorithms + health checks |
| `docs/configuration/cluster.md` | User-facing cluster config guide |
| `examples/cluster/` | Docker Compose demo |

---

*Document Version: 1.0*  
*Last Updated: 2026-09-22*  
*Authors: Peretum Engineering*
