---
name: peretum-go-review
description: Use when writing, reviewing, or refactoring Go code in the peretum repo (/Users/cybercoder/Desktop/tinyws). Encodes the house rule — human-readable code that stays fast — plus the architecture invariants and the exact CI gate commands. Triggers on "review this code", "simplify", "reduce complexity", "is this correct", or any change under cmd/, internal/router/, internal/cluster/.
---

# peretum: simplicity without losing performance

peretum is a CDN reverse proxy. It is built for edges holding **10M+ targets**,
so the central tension in every change is: keep the code the simplest thing that
works, without ever trading away the memory or latency behaviour that makes it
scale. Never resolve that tension by picking one side.

## The one rule

**Readable first, then measure — never "optimize" what you haven't measured.**

A reviewer reading a diff should be able to follow the logic without mentally
simulating a lock. If a line exists only to save a nanosecond, it needs a comment
saying what it costs without it. If you cannot name the cost, delete it.

Concretely, prefer in this order:
1. A plain map lookup or a straightforward loop.
2. A bounded cache or a shared struct so per-request work is O(1).
3. A lock-free fast path **only** for the hot path, with the slow path written
   for clarity directly beneath it.

Never introduce a cache, a background goroutine, or a new abstraction to avoid
work that a reader can see is already cheap.

## Complexity budget

Before adding a type, method, or parameter, check whether an existing one
already does the job. In this repo that has repeatedly been the answer:

- One concept, one place. If two functions implement the same protocol, one
  should call the other, not restate it.
- Delete code that has no production caller. Test-only helpers in production
  files are dead weight; move them or drop them.
- Prefer one function over three that each hold one third of the logic. Split
  only when a caller needs a different lock scope or error contract.
- Data duplication is a bug waiting to happen. Do not keep a field that another
  struct already holds.
- Do not add a comment that only restates the code. Comment the *why*, and
  especially the non-obvious constraint: the lock you must not hold, the ordering
  that prevents a lost event, the state that must be reached before serving.

Removing a fallback, a defensive nil check, or a redundant branch is a real
improvement — but only if you have confirmed the path cannot occur. "Defensive
against a future caller" is not the same as "reaches production today".

## Invariants you must not break

These are load-bearing. Each one has caused a real bug here.

**Routing is O(1) in memory, always.**
The routing table stays *empty*; the Pebble target store is the routing table.
A request costs one point lookup on `h/<hostname>`. Never rebuild a
`map[string]http.Handler` of every target — that is the exact thing lazy mode
exists to avoid, and a store-backed router with a populated table is a bug even
if the tests pass.

**The single-flight table is shared across all hosts.**
`LazyHandler` is a disposable per-request value; all real state lives in
`FlightTable`. If a handler is constructed without the shared table, concurrent
cold requests for the same host each compile the config and coalescing is gone.
The `nil` table fallback exists only for single-host tests.

**No I/O while holding a lock.**
`HostRouter.lookup` copies the fields it needs out from under the read lock, then
calls the resolver — which does disk I/O — with the lock released. The LRU check
and the flight claim in `FlightTable.materialize` must share one lock so they
cannot disagree, but the load itself runs with the lock released.

**Reads must be cancellable.**
A per-request store lookup takes the request context. Reaching for
`context.Background()` on the request path leaves work running after the client
disconnects.

**The load outlives the request that triggered it.**
Materialization uses `context.WithoutCancel`, so one client disconnecting does
not fail every follower. Followers must still observe *their own* cancellation.

**Startup waits for the replay.**
The edge must not accept traffic until retained config events have been applied
to the store. Because routing is store-backed, a target with no row has no route,
so serving early returns 404 for exactly the targets the edge exists to serve.
Catch-up and the live consumer share **one durable** — that is what makes the
handoff lossless.

**Store writes are idempotent writes keyed by hostname.**
Upserts and deletes are safe to replay, which is what makes the at-least-once
consumer safe. Do not introduce order-dependent or cumulative state.

**Messages are acked only after the handler returns nil.**
A handler error is a `Nak`; a malformed event is a `Term`. Getting this wrong
either stalls the consumer or silently drops config.

## Testing

- A concurrency invariant needs a test that would fail without it. Count the
  operations; don't assert only that the response was correct, since a broken
  coalescer still returns the right body.
- Name the failure mode in the failure message, not just the mismatch.
- Use `-race` and run the suite more than once before believing it. Wall-clock
  tests with short TTLs flake under parallel load; if you touch one, know whether
  it was already flaky.
- `TestServeHTTP_CacheTTL` in `internal/handler` is a known pre-existing flake
  (200ms TTL under load). Do not attribute it to your change; do not fix it
  incidentally.

## Gates — run all of these before proposing a commit

CI enforces exactly this list:

```bash
gofmt -l .                                    # must print nothing
go vet ./...
go build ./...
go test ./... -race -count=1                  # must be clean; run twice
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out | tail -1     # total must be >= 95%
npx --yes retype build docs                    # if docs/*.md changed: 0 errors
docker compose -f examples/cluster/compose.yaml config -q
```

Coverage is a hard gate at 95%. Every function is also expected to reach 100%:
CI warns below that, so do not knowingly leave a new function partially covered.
Cover error branches you can reach — a nil handler, a canceled context, a
not-found lookup.

## Commits

One logical change per commit, each independently building and passing `go vet`.
Write the body as *why*, in prose, and say what the old behaviour was and why it
was wrong — the diff already shows the new behaviour. Do not squash unrelated
changes together, and do not commit without being asked.
