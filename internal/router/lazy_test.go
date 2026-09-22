package router

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/cinvat/peretum/internal/cluster"
	"github.com/cinvat/peretum/internal/config"
)

func lazyEchoHandler(t *testing.T, body string) *TargetConfigHandler {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
	t.Cleanup(ts.Close)
	return &TargetConfigHandler{
		DefaultLoc: testTargetHandler(t, ts, "echo", &config.LocationConfig{Path: "/", MatchType: config.MatchPrefix}),
	}
}

func TestLazyHandlerLoadsOnce(t *testing.T) {
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	loads := 0
	var mu sync.Mutex
	lh := NewLazyHandler("svc-a", lru, func() (*TargetConfigHandler, error) {
		mu.Lock()
		loads++
		mu.Unlock()
		return lazyEchoHandler(t, "lazy-body"), nil
	})

	// First request materializes.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://svc-a/", nil)
	lh.ServeHTTP(rec, req)
	if rec.Body.String() != "lazy-body" {
		t.Fatalf("first request body = %q", rec.Body.String())
	}

	// Second request served from LRU.
	rec2 := httptest.NewRecorder()
	lh.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "http://svc-a/", nil))
	if rec2.Body.String() != "lazy-body" {
		t.Fatalf("cached request body = %q", rec2.Body.String())
	}

	mu.Lock()
	defer mu.Unlock()
	if loads != 1 {
		t.Fatalf("loads = %d, want 1", loads)
	}
}

func TestLazyHandlerConcurrentCoalescing(t *testing.T) {
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	loads := 0
	var mu sync.Mutex
	lh := NewLazyHandler("svc-b", lru, func() (*TargetConfigHandler, error) {
		mu.Lock()
		loads++
		mu.Unlock()
		return lazyEchoHandler(t, "coalesced-body"), nil
	})

	const n = 32
	var wg sync.WaitGroup
	bodies := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := httptest.NewRecorder()
			lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-b/", nil))
			bodies[i] = rec.Body.String()
		}(i)
	}
	wg.Wait()

	mu.Lock()
	if loads != 1 {
		t.Fatalf("loads = %d, want 1 (coalesced)", loads)
	}
	mu.Unlock()
	for i, b := range bodies {
		if b != "coalesced-body" {
			t.Fatalf("body[%d] = %q", i, b)
		}
	}
}

func TestLazyHandlerReloadsAfterEviction(t *testing.T) {
	// Room for exactly one host so key A is evicted when B is served.
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](1)
	counts := make(map[string]int)
	var mu sync.Mutex
	makeLoad := func(name, body string) func() (*TargetConfigHandler, error) {
		return func() (*TargetConfigHandler, error) {
			mu.Lock()
			counts[name]++
			mu.Unlock()
			return lazyEchoHandler(t, body), nil
		}
	}
	a := NewLazyHandler("a", lru, makeLoad("a", "body-a"))
	b := NewLazyHandler("b", lru, makeLoad("b", "body-b"))

	serve := func(lh *LazyHandler, host string) string {
		rec := httptest.NewRecorder()
		lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://"+host+"/", nil))
		return rec.Body.String()
	}

	if got := serve(a, "a"); got != "body-a" {
		t.Fatalf("a body = %q", got)
	}
	if got := serve(b, "b"); got != "body-b" {
		t.Fatalf("b body = %q", got)
	}
	// Serving b evicted a; a re-materializes.
	if got := serve(a, "a"); got != "body-a" {
		t.Fatalf("a reloaded body = %q", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if counts["a"] != 2 {
		t.Fatalf("a loads = %d, want 2 (materialize + reload)", counts["a"])
	}
	if counts["b"] != 1 {
		t.Fatalf("b loads = %d, want 1", counts["b"])
	}
}

func TestLazyHandlerLoadFailureRetries(t *testing.T) {
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	loads := 0
	lh := NewLazyHandler("svc-c", lru, func() (*TargetConfigHandler, error) {
		loads++
		if loads == 1 {
			return nil, fmt.Errorf("boom")
		}
		return lazyEchoHandler(t, "retried-body"), nil
	})

	rec := httptest.NewRecorder()
	lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-c/", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("failure status = %d, want 502", rec.Code)
	}

	rec2 := httptest.NewRecorder()
	lh.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "http://svc-c/", nil))
	if rec2.Code != http.StatusOK || rec2.Body.String() != "retried-body" {
		t.Fatalf("retry = status %d body %q", rec2.Code, rec2.Body.String())
	}
	if loads != 2 {
		t.Fatalf("loads = %d, want 2", loads)
	}
}

func TestHostRouterUpsertAndRemove(t *testing.T) {
	hr := NewHostRouter()
	a := lazyEchoHandler(t, "upsert-body")

	hr.Upsert("svc-x", a)
	rec := httptest.NewRecorder()
	hr.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-x/", nil))
	if rec.Body.String() != "upsert-body" {
		t.Fatalf("after upsert body = %q", rec.Body.String())
	}

	hr.RemoveHost("svc-x")
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "http://svc-x/", nil)
	hr.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("after remove status = %d, want 404", rec2.Code)
	}
}
