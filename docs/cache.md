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