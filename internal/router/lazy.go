package router

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/cinvat/peretum/internal/cluster"
)

// errMaterializeFailed is reported to followers when the goroutine that owned
// the in-flight materialization completed without populating the LRU.
var errMaterializeFailed = errors.New("target config materialization failed")

// materializeTimeout bounds a single materialization. It applies to the load
// itself, not to the request that triggered it, so a slow disk or a pathological
// config cannot pin the flight open indefinitely.
const materializeTimeout = 30 * time.Second

// LazyHandler serves a single host whose compiled handler lives off-RAM until
// first use. On the first request it materializes the handler via load(), caches
// it in the shared LRU (bounded), and LRU eviction pushes cold configs back to
// disk. Parallel first-requests for the same host are coalesced onto a single
// materialization.
type LazyHandler struct {
	key  string
	lru  *cluster.LRUCache[string, *TargetConfigHandler]
	load func(context.Context) (*TargetConfigHandler, error)

	// flights coalesces materializations for this key. It is shared across
	// every LazyHandler serving the same host so that a store-backed router can
	// build a throwaway handler per request without losing coalescing.
	flights *flightTable
}

// loadCall is a single materialization that other requests can join.
type loadCall struct {
	done chan struct{}
}

// flightTable tracks in-flight materializations keyed by hostname.
//
// It is shared by every LazyHandler in the process. That matters for a
// store-backed router, which cannot keep one handler per target in memory
// without giving up the 10M+ target goal: it creates a LazyHandler on demand
// for each request instead. If the in-flight state lived on the handler, every
// concurrent request for the same cold host would start its own load and the
// coalescing guarantee would be lost. Sharing the table keeps it while the
// handler itself stays a throwaway.
//
// Entries exist only while a load is actually running, so the table is bounded
// by concurrent cold requests rather than by the number of targets.
type flightTable struct {
	lru *cluster.LRUCache[string, *TargetConfigHandler]

	mu    sync.Mutex
	calls map[string]*loadCall
}

// NewFlightTable returns a flight table for handlers that share lru. Pass the
// same table to every NewSharedLazyHandler for a given host so concurrent
// requests for that host coalesce onto one materialization.
func NewFlightTable(lru *cluster.LRUCache[string, *TargetConfigHandler]) *flightTable {
	return newFlightTable(lru)
}

func newFlightTable(lru *cluster.LRUCache[string, *TargetConfigHandler]) *flightTable {
	return &flightTable{lru: lru, calls: make(map[string]*loadCall)}
}

// acquire reports a cached handler for key, or else claims the flight for it.
// The LRU check happens under the same lock as the claim so that "nothing
// cached" and "no load in progress" cannot disagree, which would let a second
// caller start a redundant load.
func (ft *flightTable) acquire(key string) (tch *TargetConfigHandler, call *loadCall, owner bool) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	if cached, ok := ft.lru.Get(key); ok {
		return cached, nil, false
	}
	if existing, ok := ft.calls[key]; ok {
		return nil, existing, false
	}
	call = &loadCall{done: make(chan struct{})}
	ft.calls[key] = call
	return nil, call, true
}

// release publishes tch into the LRU (when materialization succeeded), clears
// the flight, and wakes every follower.
func (ft *flightTable) release(key string, call *loadCall, tch *TargetConfigHandler, ok bool) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if ok && tch != nil {
		ft.lru.Put(key, tch)
	}
	if current, exists := ft.calls[key]; exists && current == call {
		delete(ft.calls, key)
	}
	close(call.done)
}

// NewLazyHandler creates a lazily-materializing handler for key. The shared
// lru keeps compiled handlers resident across hosts up to its capacity.
// load is called with a context that is canceled when the request is canceled.
func NewLazyHandler(key string, lru *cluster.LRUCache[string, *TargetConfigHandler], load func(context.Context) (*TargetConfigHandler, error)) *LazyHandler {
	return &LazyHandler{
		key:     key,
		lru:     lru,
		load:    load,
		flights: newFlightTable(lru),
	}
}

// NewSharedLazyHandler is NewLazyHandler with an explicit shared flight table.
// Handlers for the same key must be built with the same table and the same lru
// for coalescing to work; see flightTable.
func NewSharedLazyHandler(key string, lru *cluster.LRUCache[string, *TargetConfigHandler], load func(context.Context) (*TargetConfigHandler, error), flights *flightTable) *LazyHandler {
	if flights == nil {
		return NewLazyHandler(key, lru, load)
	}
	return &LazyHandler{key: key, lru: lru, load: load, flights: flights}
}

func (lh *LazyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Fast path: already materialized in the shared LRU.
	if tch, ok := lh.lru.Get(lh.key); ok {
		tch.ServeHTTP(w, r)
		return
	}

	tch, err := lh.loadOnce(ctx)
	if err != nil || tch == nil {
		if ctx.Err() != nil {
			http.Error(w, "request canceled", http.StatusRequestTimeout)
			return
		}
		http.Error(w, "target config unavailable", http.StatusBadGateway)
		return
	}
	// The load deliberately outlives this request so that followers waiting on
	// the same flight still get a result. But this client is gone, so report
	// the cancellation rather than starting proxy work nobody will read.
	if ctx.Err() != nil {
		http.Error(w, "request canceled", http.StatusRequestTimeout)
		return
	}
	tch.ServeHTTP(w, r)
}

// loadOnce returns the compiled handler for lh.key, running lh.load at most
// once across concurrent callers. Followers wait on the in-flight call and then
// read the result from the LRU.
//
// The flight lock is only ever held for short critical sections, never across
// lh.load, which reads from disk and compiles the handler chain and can be
// slow. Holding it across the load would serialize every request for the host
// and stop followers from reacting to their own cancellation. The lock order is
// always flights.mu before lru.mu; the LRU has its own lock and never calls
// back into this handler.
func (lh *LazyHandler) loadOnce(ctx context.Context) (*TargetConfigHandler, error) {
	// A cached handler or an existing flight both come back from one atomic
	// check, so two callers can never both decide they own the load.
	cached, call, owner := lh.flights.acquire(lh.key)
	if cached != nil {
		return cached, nil
	}

	if !owner {
		select {
		case <-call.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		// The leader publishes to the LRU before clearing the flight, so a
		// hit here means the load succeeded.
		if tch, ok := lh.lru.Get(lh.key); ok {
			return tch, nil
		}
		return nil, errMaterializeFailed
	}

	// Detach the load from the initiating request's cancellation. Otherwise a
	// client that disconnects mid-load aborts the work for every follower that
	// joined this flight, and all of them fail even though their own requests
	// are still live. The load still bounds itself with a timeout.
	loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), materializeTimeout)
	tch, err := lh.runLoad(loadCtx)
	cancel()

	lh.flights.release(lh.key, call, tch, err == nil)

	return tch, err
}

// runLoad invokes lh.load without holding lh.mu. A panic from load is converted
// into an error so that the request fails with a gateway error instead of
// taking down the server, and so that followers waiting on the call are
// released rather than hanging forever.
func (lh *LazyHandler) runLoad(ctx context.Context) (tch *TargetConfigHandler, err error) {
	defer func() {
		if r := recover(); r != nil {
			tch, err = nil, fmt.Errorf("materialize %q: panic: %v", lh.key, r)
		}
	}()

	return lh.load(ctx)
}
