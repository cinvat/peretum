package router

import (
	"context"
	"net/http"
	"sync"

	"github.com/cinvat/peretum/internal/cluster"
)

// LazyHandler serves a host whose compiled handler lives off-RAM until first
// use. On the first request it materializes the handler via load(), caches it
// in the shared LRU (bounded), and LRU eviction pushes cold configs back to
// disk. Parallel first-requests for the same host are coalesced onto a single
// materialization.
type LazyHandler struct {
	key  string
	lru  *cluster.LRUCache[string, *TargetConfigHandler]
	load func(context.Context) (*TargetConfigHandler, error)

	mu       sync.Mutex
	inFlight map[string]*inFlightState
}

type inFlightState struct {
	ch   chan struct{}
	done bool
}

// NewLazyHandler creates a lazily-materializing handler for key. The shared
// lru keeps compiled handlers resident across hosts up to its capacity.
// load is called with a context that is canceled when the request is canceled.
func NewLazyHandler(key string, lru *cluster.LRUCache[string, *TargetConfigHandler], load func(context.Context) (*TargetConfigHandler, error)) *LazyHandler {
	return &LazyHandler{
		key:      key,
		lru:      lru,
		load:     load,
		inFlight: make(map[string]*inFlightState),
	}
}

func (lh *LazyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Fast path: already in LRU
	if tch, ok := lh.lru.Get(lh.key); ok {
		tch.ServeHTTP(w, r)
		return
	}

	// Check if materialization is already in-flight for this key
	lh.mu.Lock()
	state, inFlight := lh.inFlight[lh.key]
	if !inFlight {
		// We're the first - start materialization
		state = &inFlightState{ch: make(chan struct{})}
		lh.inFlight[lh.key] = state
	}
	lh.mu.Unlock()

	if inFlight {
		// Another request is materializing; wait for it
		select {
		case <-state.ch:
		case <-ctx.Done():
			http.Error(w, "request canceled", http.StatusRequestTimeout)
			return
		}

		// Materialization done; check LRU
		if tch, ok := lh.lru.Get(lh.key); ok {
			tch.ServeHTTP(w, r)
			return
		}
		// Materialization failed; fall through to retry
	} else {
		// We're the first; materialize and serve directly
		tch, err := lh.materialize(ctx)
		if err == nil && tch != nil {
			tch.ServeHTTP(w, r)
			return
		}
		// If context was canceled during materialization, return 408
		select {
		case <-ctx.Done():
			http.Error(w, "request canceled", http.StatusRequestTimeout)
			return
		default:
		}
	}

	// After materialization (or retry), check LRU again
	if tch, ok := lh.lru.Get(lh.key); ok {
		tch.ServeHTTP(w, r)
		return
	}
	http.Error(w, "target config unavailable", http.StatusBadGateway)
}

// materialize loads and caches the compiled handler. Runs exactly once per
// in-flight key. Returns the handler on success.
func (lh *LazyHandler) materialize(ctx context.Context) (*TargetConfigHandler, error) {
	state := lh.inFlight[lh.key]
	defer func() {
		recover() // ignore panic from load()
		lh.mu.Lock()
		close(state.ch)
		delete(lh.inFlight, lh.key)
		lh.mu.Unlock()
	}()

	tch, err := lh.load(ctx)
	lh.mu.Lock()
	if err == nil && tch != nil {
		lh.lru.Put(lh.key, tch)
	}
	lh.mu.Unlock()
	return tch, err
}
