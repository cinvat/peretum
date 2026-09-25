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

	// mu guards flight. It is also held across the LRU access in loadOnce so
	// that "no flight in progress" and "value already cached" cannot disagree.
	mu     sync.Mutex
	flight *loadCall
}

// loadCall is a single materialization that other requests can join.
type loadCall struct {
	done chan struct{}
}

// NewLazyHandler creates a lazily-materializing handler for key. The shared
// lru keeps compiled handlers resident across hosts up to its capacity.
// load is called with a context that is canceled when the request is canceled.
func NewLazyHandler(key string, lru *cluster.LRUCache[string, *TargetConfigHandler], load func(context.Context) (*TargetConfigHandler, error)) *LazyHandler {
	return &LazyHandler{
		key:  key,
		lru:  lru,
		load: load,
	}
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
// lh.mu is only ever held for short critical sections, never across lh.load,
// which reads from disk and compiles the handler and can be slow. Holding the
// lock across it would serialize every request for the host and stop followers
// from reacting to their own cancellation. The lock order is always
// lh.mu before lru.mu; the LRU has its own lock and never calls back into
// this handler.
func (lh *LazyHandler) loadOnce(ctx context.Context) (*TargetConfigHandler, error) {
	lh.mu.Lock()

	// Re-check the LRU under the lock. A materialization may have completed
	// between this caller's fast-path miss and acquiring the lock; without
	// this, such a caller would start a redundant second load.
	if tch, ok := lh.lru.Get(lh.key); ok {
		lh.mu.Unlock()
		return tch, nil
	}

	if call := lh.flight; call != nil {
		lh.mu.Unlock()
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

	// This caller owns the materialization.
	call := &loadCall{done: make(chan struct{})}
	lh.flight = call
	lh.mu.Unlock()

	// Detach the load from the initiating request's cancellation. Otherwise a
	// client that disconnects mid-load aborts the work for every follower that
	// joined this flight, and all of them fail even though their own requests
	// are still live. The load still bounds itself with a timeout.
	loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), materializeTimeout)
	tch, err := lh.runLoad(loadCtx)
	cancel()

	lh.mu.Lock()
	if err == nil && tch != nil {
		lh.lru.Put(lh.key, tch)
	}
	lh.flight = nil
	close(call.done)
	lh.mu.Unlock()

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
