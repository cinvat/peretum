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

// loadFunc compiles a target's handlers. It is called with a context that is
// canceled when the request that triggered it is canceled.
type loadFunc func(context.Context) (*TargetConfigHandler, error)

// FlightTable owns the single-flight protocol for materialized targets: it holds
// the compiled-handler cache and the set of loads currently running.
//
// A store-backed router cannot keep one LazyHandler per target in memory without
// giving up the 10M+ target goal, so it builds a throwaway handler per request
// instead. If in-flight state lived on the handler, every concurrent request for
// the same cold host would start its own load. Sharing one table across all
// hosts keeps the coalescing guarantee while the handler itself stays
// disposable.
//
// Flight entries exist only while a load is actually running, so the table is
// bounded by concurrent cold requests rather than by the number of targets.
type FlightTable struct {
	lru *cluster.LRUCache[string, *TargetConfigHandler]

	mu    sync.Mutex
	calls map[string]chan struct{}
}

// NewFlightTable returns a table caching compiled handlers in lru. Handlers that
// may serve the same hostname must be built with the same table, or they will
// each run their own load.
func NewFlightTable(lru *cluster.LRUCache[string, *TargetConfigHandler]) *FlightTable {
	return &FlightTable{lru: lru, calls: make(map[string]chan struct{})}
}

// cached returns the resident handler for key, if any.
func (ft *FlightTable) cached(key string) (*TargetConfigHandler, bool) {
	return ft.lru.Get(key)
}

// materialize returns the handler for key, running load at most once across
// concurrent callers. Followers wait for the load that is already running and
// then read what it published to the cache.
//
// The lock is only held for short critical sections, never across load, which
// reads from disk and compiles a handler chain and can be slow. Holding it across
// the load would serialize every request for the host and stop followers from
// reacting to their own cancellation.
func (ft *FlightTable) materialize(ctx context.Context, key string, load loadFunc) (*TargetConfigHandler, error) {
	ft.mu.Lock()
	// The cache check and the flight claim share one lock, so "nothing cached"
	// and "no load in progress" cannot disagree and start two loads.
	if cached, ok := ft.cached(key); ok {
		ft.mu.Unlock()
		return cached, nil
	}
	if call, running := ft.calls[key]; running {
		ft.mu.Unlock()
		select {
		case <-call:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		// The owner publishes to the cache before waking followers, so a hit
		// here means the load succeeded.
		if cached, ok := ft.cached(key); ok {
			return cached, nil
		}
		return nil, errMaterializeFailed
	}
	call := make(chan struct{})
	ft.calls[key] = call
	ft.mu.Unlock()

	// Detach the load from the initiating request's cancellation. Otherwise a
	// client that disconnects mid-load aborts the work for every follower that
	// joined this flight, and all of them fail even though their own requests
	// are still live. The load still bounds itself with a timeout.
	loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), materializeTimeout)
	tch, err := runLoad(loadCtx, key, load)
	cancel()

	ft.mu.Lock()
	if err == nil && tch != nil {
		ft.lru.Put(key, tch)
	}
	delete(ft.calls, key)
	close(call)
	ft.mu.Unlock()

	return tch, err
}

// runLoad invokes load with the given key for error messages. A panic becomes an
// error, so that a bad config fails one request with a gateway error instead of
// taking down the server, and so that followers are released rather than hanging
// on a flight whose owner never returns.
func runLoad(ctx context.Context, key string, load loadFunc) (tch *TargetConfigHandler, err error) {
	defer func() {
		if r := recover(); r != nil {
			tch, err = nil, fmt.Errorf("materialize %q: panic: %v", key, r)
		}
	}()

	return load(ctx)
}

// LazyHandler serves a single host whose compiled handler lives off-RAM until
// first use. On the first request it materializes the handler via load and caches
// it in the shared LRU; LRU eviction pushes cold configs back to disk.
//
// A LazyHandler holds no state of its own, so building one per request is cheap.
// The state that matters -- the cache and in-flight loads -- lives in flights,
// which all handlers for the same host must share.
type LazyHandler struct {
	key     string
	flights *FlightTable
	load    loadFunc
}

// NewLazyHandler returns a lazily-materializing handler for key. The shared lru
// keeps compiled handlers resident across hosts up to its capacity. Handlers that
// may serve the same hostname concurrently must be built with the same flights
// table so their first requests coalesce; pass nil for a private table when no
// other handler serves that host.
func NewLazyHandler(key string, lru *cluster.LRUCache[string, *TargetConfigHandler], load loadFunc, flights *FlightTable) *LazyHandler {
	if flights == nil {
		flights = NewFlightTable(lru)
	}
	return &LazyHandler{key: key, flights: flights, load: load}
}

func (lh *LazyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Fast path: a resident handler needs neither the flight lock nor a load.
	// This is the common case for a hot target, so it is worth reading the
	// cache directly rather than going through materialize.
	if tch, ok := lh.flights.cached(lh.key); ok {
		tch.ServeHTTP(w, r)
		return
	}

	tch, err := lh.flights.materialize(ctx, lh.key, lh.load)
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
