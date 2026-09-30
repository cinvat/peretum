---
label: Disk cache
icon: database
order: 300
---

# Disk cache

Peretum stores backend responses as files on disk and serves subsequent
requests straight from disk — no network, no upstream connection. The cache is
**transparent**: the proxy only caches responses that are cacheable, keyed by
method + route + upstream + request body hash when applicable.

## Cache keys

A response is cached per `(target, route, method + path + query)` and per
`cache_ttl`. `POST`/`PUT`/`PATCH` requests include a **body hash** in the key
(same request body reuses a cached response). A response is served from cache
only if the `GET`/`HEAD`/`POST` request's body hash matches (when required).

## Cacheable responses

Responses must have:

- an **HTTP status in the 200 range** (200–299);
- no `Set-Cookie` header;
- no `Authorization` header in the request (varies).

## Configuration

| Key | Where | Default | Description |
| --- | --- | --- | --- |
| `cache_dir` | global (`config.yaml`) | `./cache` | Root directory (created on demand). |
| `max_cache_size` | global | `0` (unlimited) | Max total bytes; LRU-evicted. |
| `max_cache_age` | global | `0` | Entry lifetime via `time.ParseDuration`. |
| `cache` | location | `false` | Enable caching for the location. |
| `cache_ttl` | location | — | Overrides `max_cache_age` per location. |

## Transparent headers

- `X-Cache: HIT` — response served from the disk cache.
- `X-Cache: MISS` — response fetched from upstream and cached (when
  cacheable).

## Strict verification

`curl -sI` returns `X-Cache: HIT` only on truly cached responses; the cache
is verified to stall/expire at the exact configured `cache_ttl` (17&nbsp;s)
and to serve `HIT` immediately after a `MISS`.

## HTTP/3 note

HTTP/3 (QUIC) responses bypass the disk cache path entirely (see
[HTTP/3 (QUIC)](http3.md)).

## Purging via the REST API

`DELETE /v1alpha1/cache` on the API server removes entries by site, object,
or location subtree:

```bash
# whole site
curl -X DELETE "localhost:8080/v1alpha1/cache?host=example.com"
# single object
curl -X DELETE "localhost:8080/v1alpha1/cache?host=example.com&path=/exact"
# location subtree
curl -X DELETE "localhost:8080/v1alpha1/cache?host=example.com&prefix=/blog/"
# everything (host/path/prefix must be empty)
curl -X DELETE "localhost:8080/v1alpha1/cache?all=true"
# → {"purged": 42}
```

| Param | Scope |
| --- | --- |
| `host` | Every entry for the site. |
| `host` + `path` | One object (exact path). |
| `host` + `prefix` | A location subtree (`prefix` may also be used without `host`). |
| `all=true` | The whole cache; exclusive with the other params. |

### Standalone vs cluster mode

- **Standalone** (default): the API purges local cache files
  (`peretum api --cache-dir` must point at the proxy's `cache_dir`) and
  reports `{"purged": N}`. Purged entries miss immediately — the read path
  stats files on every request.
- **Cluster** (`peretum api --nats-uri`): the API instead publishes the
  purge on the ephemeral `cache.purge` NATS subject, and **every edge**
  purges its own live cache in memory, reporting
  `{"published": true}` (no per-edge counts — fire-and-forget, like all
  cluster broadcasts). An edge offline during the broadcast keeps stale
  entries until TTL/expiry, then refills from origin.

Two caveats for standalone mode: the proxy's in-memory size accounting
drifts until purged keys age out of its LRU (never a stale serve), and a
purge racing an in-flight cache write can lose (the late write re-creates
the file).