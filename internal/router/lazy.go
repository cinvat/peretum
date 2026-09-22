package router

import (
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
	load func() (*TargetConfigHandler, error)

	mu      sync.Mutex
	loading chan struct{}
}

// NewLazyHandler creates a lazily-materializing handler for key. The shared
// lru keeps compiled handlers resident across hosts up to its capacity.
func NewLazyHandler(key string, lru *cluster.LRUCache[string, *TargetConfigHandler], load func() (*TargetConfigHandler, error)) *LazyHandler {
	return &LazyHandler{
		key:  key,
		lru:  lru,
		load: load,
	}
}

func (lh *LazyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if tch, ok := lh.lru.Get(lh.key); ok {
		tch.ServeHTTP(w, r)
		return
	}

	// Coalesce concurrent first-request materializations.
	lh.mu.Lock()
	if lh.loading == nil {
		lh.loading = make(chan struct{})
		go lh.materialize()
	}
	ch := lh.loading
	lh.mu.Unlock()

	<-ch

	lh.mu.Lock()
	lh.loading = nil
	// A request that arrived while the load failed retries immediately with a
	// fresh materialization (load is best-effort and safe to repeat).
	lh.mu.Unlock()

	if tch, ok := lh.lru.Get(lh.key); ok {
		tch.ServeHTTP(w, r)
		return
	}
	http.Error(w, "target config unavailable", http.StatusBadGateway)
}

// materialize loads and caches the compiled handler. It runs on exactly one
// goroutine per in-flight first-request (see ServeHTTP). A failed load simply
// leaves the LRU empty and the next request retries.
func (lh *LazyHandler) materialize() {
	defer close(lh.loading)
	tch, err := lh.load()
	if err == nil && tch != nil {
		lh.lru.Put(lh.key, tch)
	}
}
