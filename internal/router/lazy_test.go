package router

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

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
	lh := NewLazyHandler("svc-a", lru, func(ctx context.Context) (*TargetConfigHandler, error) {
		mu.Lock()
		loads++
		mu.Unlock()
		return lazyEchoHandler(t, "lazy-body"), nil
	}, nil)

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
	lh := NewLazyHandler("svc-b", lru, func(ctx context.Context) (*TargetConfigHandler, error) {
		mu.Lock()
		loads++
		mu.Unlock()
		return lazyEchoHandler(t, "coalesced-body"), nil
	}, nil)

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
	makeLoad := func(name, body string) func(context.Context) (*TargetConfigHandler, error) {
		return func(ctx context.Context) (*TargetConfigHandler, error) {
			mu.Lock()
			counts[name]++
			mu.Unlock()
			return lazyEchoHandler(t, body), nil
		}
	}
	a := NewLazyHandler("a", lru, makeLoad("a", "body-a"), nil)
	b := NewLazyHandler("b", lru, makeLoad("b", "body-b"), nil)

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
	lh := NewLazyHandler("svc-c", lru, func(ctx context.Context) (*TargetConfigHandler, error) {
		loads++
		if loads == 1 {
			return nil, fmt.Errorf("boom")
		}
		return lazyEchoHandler(t, "retried-body"), nil
	}, nil)

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

func TestLazyHandlerContextCancellation(t *testing.T) {
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	loadStarted := make(chan struct{})
	var loadStartedOnce sync.Once

	lh := NewLazyHandler("svc-d", lru, func(ctx context.Context) (*TargetConfigHandler, error) {
		loadStartedOnce.Do(func() { close(loadStarted) })
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
		return lazyEchoHandler(t, "context-body"), nil
	}, nil)

	// Start a request and cancel it before load completes
	ctx, cancel := context.WithCancel(context.Background())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://svc-d/", nil).WithContext(ctx)

	go func() {
		<-loadStarted
		cancel()
	}()
	lh.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestTimeout {
		t.Fatalf("canceled request status = %d, want %d", rec.Code, http.StatusRequestTimeout)
	}

	// Wait for first materialize to fully complete (including defer cleanup)
	time.Sleep(50 * time.Millisecond)

	// Next request should succeed (load will retry)
	rec2 := httptest.NewRecorder()
	lh.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "http://svc-d/", nil))
	if rec2.Code != http.StatusOK || rec2.Body.String() != "context-body" {
		t.Fatalf("retry after cancel = status %d body %q", rec2.Code, rec2.Body.String())
	}
}

func TestLazyHandlerPanicRecovery(t *testing.T) {
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	lh := NewLazyHandler("svc-e", lru, func(ctx context.Context) (*TargetConfigHandler, error) {
		panic("materialize panic")
	}, nil)

	// First request should get 502 (or 503) and not hang
	rec := httptest.NewRecorder()
	lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-e/", nil))
	if rec.Code != http.StatusServiceUnavailable && rec.Code != http.StatusBadGateway {
		t.Fatalf("panic on first request status = %d, want 502/503", rec.Code)
	}

	// Second request should retry (materialize will be called again)
	rec2 := httptest.NewRecorder()
	lh.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "http://svc-e/", nil))
	if rec2.Code != http.StatusServiceUnavailable && rec2.Code != http.StatusBadGateway {
		t.Fatalf("panic on second request status = %d, want 502/503", rec2.Code)
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

func TestLazyHandlerFollowerSeesLeaderFailure(t *testing.T) {
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	release := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once

	lh := NewLazyHandler("svc-f", lru, func(ctx context.Context) (*TargetConfigHandler, error) {
		once.Do(func() { close(started) })
		<-release
		return nil, fmt.Errorf("load boom")
	}, nil)

	// The leader blocks inside load until we release it.
	leaderDone := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-f/", nil))
		leaderDone <- rec.Code
	}()
	<-started

	// A follower that arrives while the leader is still in flight must not
	// start a second load; it should observe the failure and get a 502.
	followerDone := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-f/", nil))
		followerDone <- rec.Code
	}()

	close(release)
	if code := <-leaderDone; code != http.StatusBadGateway {
		t.Fatalf("leader status = %d, want 502", code)
	}
	if code := <-followerDone; code != http.StatusBadGateway {
		t.Fatalf("follower status = %d, want 502", code)
	}
}

func TestLazyHandlerFollowerCancelsWhileWaiting(t *testing.T) {
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	started := make(chan struct{})
	var once sync.Once
	release := make(chan struct{})
	defer close(release)

	lh := NewLazyHandler("svc-g", lru, func(ctx context.Context) (*TargetConfigHandler, error) {
		once.Do(func() { close(started) })
		<-release
		return lazyEchoHandler(t, "late-body"), nil
	}, nil)

	go func() {
		rec := httptest.NewRecorder()
		lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-g/", nil))
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceled := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "http://svc-g/", nil).WithContext(ctx)
		lh.ServeHTTP(rec, req)
		canceled <- rec.Code
	}()

	// Cancel while the leader is still materializing; the follower must give
	// up immediately rather than waiting for the load to finish.
	cancel()
	if code := <-canceled; code != http.StatusRequestTimeout {
		t.Fatalf("canceled follower status = %d, want 408", code)
	}
}

func TestLazyHandlerNilHandlerFailsClosed(t *testing.T) {
	// A nil target store is the closest proxy for "nothing to load"; the
	// handler must fail closed with a gateway error rather than panic.
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	lh := NewLazyHandler("svc-h", lru, func(ctx context.Context) (*TargetConfigHandler, error) {
		return nil, nil // no error, but no handler either
	}, nil)

	rec := httptest.NewRecorder()
	lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-h/", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

func TestLazyHandlerLoadSurvivesLeaderDisconnect(t *testing.T) {
	// A client that disconnects must not destroy the materialization for the
	// followers that joined the same flight. The load runs on a context
	// detached from the request, so it completes and populates the LRU; the
	// leader just never gets to read the response.
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	loadStarted := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	lh := NewLazyHandler("svc-detach", lru, func(ctx context.Context) (*TargetConfigHandler, error) {
		once.Do(func() { close(loadStarted) })
		<-release
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return lazyEchoHandler(t, "detached"), nil
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	leaderRec := httptest.NewRecorder()
	leaderDone := make(chan int, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "http://svc-detach/", nil).WithContext(ctx)
		lh.ServeHTTP(leaderRec, req)
		leaderDone <- leaderRec.Code
	}()
	<-loadStarted

	followerRec := httptest.NewRecorder()
	followerDone := make(chan int, 1)
	go func() {
		req := httptest.NewRequest(http.MethodGet, "http://svc-detach/", nil)
		lh.ServeHTTP(followerRec, req)
		followerDone <- followerRec.Code
	}()

	// The leader's client goes away mid-load.
	cancel()
	close(release)

	if code := <-leaderDone; code != http.StatusRequestTimeout {
		t.Fatalf("leader status = %d, want 408", code)
	}

	// The follower must still be served: the load survived the disconnect.
	if code := <-followerDone; code != http.StatusOK {
		t.Fatalf("follower status = %d, want 200 (body %q)", code, followerRec.Body.String())
	}
	if followerRec.Body.String() != "detached" {
		t.Fatalf("follower body = %q, want detached", followerRec.Body.String())
	}
}

// A store-backed router cannot keep one LazyHandler per target in memory, so it
// builds a throwaway handler per request instead. This test pins the invariant
// that makes that safe: coalescing lives in the shared table, not the handler.
func TestSharedFlightTableCoalescesAcrossHandlers(t *testing.T) {
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	flights := NewFlightTable(lru)

	var mu sync.Mutex
	loads := 0
	release := make(chan struct{})
	load := func(ctx context.Context) (*TargetConfigHandler, error) {
		mu.Lock()
		loads++
		mu.Unlock()
		// Hold the load open so every goroutine below is guaranteed to arrive
		// while it is in flight, rather than after it finished.
		<-release
		return lazyEchoHandler(t, "shared-body"), nil
	}

	const callers = 8
	var wg sync.WaitGroup
	bodies := make([]string, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// A distinct handler per goroutine, exactly as the resolver builds
			// them: no shared *LazyHandler, so the only thing that can coalesce
			// these is the table.
			lh := NewLazyHandler("svc-b", lru, load, flights)
			rec := httptest.NewRecorder()
			lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-b/", nil))
			bodies[i] = rec.Body.String()
		}(i)
	}

	// Wait until the leader is inside load, then let the followers pile up.
	for {
		mu.Lock()
		started := loads
		mu.Unlock()
		if started == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	mu.Lock()
	got := loads
	mu.Unlock()
	if got != 1 {
		t.Fatalf("load called %d times across %d distinct handlers, want 1", got, callers)
	}
	for i, body := range bodies {
		if body != "shared-body" {
			t.Fatalf("caller %d body = %q, want %q", i, body, "shared-body")
		}
	}
}

// The table must not leak: entries exist only while a load runs, so a store
// with 10M targets still costs nothing here.
func TestSharedFlightTableReleasesEntriesAfterLoad(t *testing.T) {
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	flights := NewFlightTable(lru)

	load := func(ctx context.Context) (*TargetConfigHandler, error) {
		return lazyEchoHandler(t, "b"), nil
	}

	for _, host := range []string{"a", "b", "c", "d", "e", "f"} {
		lh := NewLazyHandler(host, lru, load, flights)
		rec := httptest.NewRecorder()
		lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://"+host+"/", nil))
		if rec.Body.String() != "b" {
			t.Fatalf("host %s body = %q", host, rec.Body.String())
		}
	}

	flights.mu.Lock()
	inflight := len(flights.calls)
	flights.mu.Unlock()
	if inflight != 0 {
		t.Fatalf("flight table holds %d entries after all loads finished, want 0", inflight)
	}
}

// A failed load must also clear the flight, otherwise the key would be
// permanently pinned and every later request would wait on a call that already
// returned.
func TestSharedFlightTableReleasesEntriesAfterFailure(t *testing.T) {
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	flights := NewFlightTable(lru)

	var attempts int
	var mu sync.Mutex
	load := func(ctx context.Context) (*TargetConfigHandler, error) {
		mu.Lock()
		attempts++
		failed := attempts == 1
		mu.Unlock()
		if failed {
			return nil, errMaterializeFailed
		}
		return lazyEchoHandler(t, "b"), nil
	}

	lh := NewLazyHandler("svc-f", lru, load, flights)
	rec := httptest.NewRecorder()
	lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-f/", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status after failed load = %d, want %d", rec.Code, http.StatusBadGateway)
	}

	flights.mu.Lock()
	inflight := len(flights.calls)
	flights.mu.Unlock()
	if inflight != 0 {
		t.Fatalf("flight table holds %d entries after a failed load, want 0", inflight)
	}

	// The next request must be able to retry rather than joining a dead flight.
	rec = httptest.NewRecorder()
	lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-f/", nil))
	if rec.Body.String() != "b" {
		t.Fatalf("retry body = %q, want %q", rec.Body.String(), "b")
	}
}

func TestHostRouterResolverServesUnlistedHosts(t *testing.T) {
	hr := NewHostRouter()
	hr.Reload(nil, nil) // table deliberately empty, as in lazy mode

	known := map[string]string{"svc-r": "resolved-body"}
	hr.SetResolver(func(_ context.Context, host string) (http.Handler, bool) {
		body, ok := known[host]
		if !ok {
			return nil, false
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, body)
		}), true
	})

	rec := httptest.NewRecorder()
	hr.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-r/", nil))
	if rec.Body.String() != "resolved-body" {
		t.Fatalf("resolved body = %q, want %q", rec.Body.String(), "resolved-body")
	}

	// An unknown host must still 404 rather than falling through to a handler.
	rec = httptest.NewRecorder()
	hr.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-missing/", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown host status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// The default target has to resolve through the resolver too, otherwise a
// store-backed deployment would have no _default route.
func TestHostRouterResolverServesDefaultHost(t *testing.T) {
	hr := NewHostRouter()
	hr.Reload(nil, nil)

	known := map[string]string{DefaultHostname: "default-body"}
	hr.SetResolver(func(_ context.Context, host string) (http.Handler, bool) {
		body, ok := known[host]
		if !ok {
			return nil, false
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, body)
		}), true
	})

	rec := httptest.NewRecorder()
	hr.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://anything.example.com/", nil))
	if rec.Body.String() != "default-body" {
		t.Fatalf("default fallback body = %q, want %q", rec.Body.String(), "default-body")
	}
}

// A table entry wins over the resolver: table entries are explicit, and a
// stale resolver must not override them.
func TestHostRouterResolverDoesNotOverrideTable(t *testing.T) {
	hr := NewHostRouter()
	hr.Upsert("svc-t", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "table-body")
	}))
	hr.SetResolver(func(_ context.Context, host string) (http.Handler, bool) {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "resolver-body")
		}), true
	})

	rec := httptest.NewRecorder()
	hr.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-t/", nil))
	if rec.Body.String() != "table-body" {
		t.Fatalf("body = %q, want %q (table must win)", rec.Body.String(), "table-body")
	}
}

// Reload must not drop the resolver, or a reload would silently turn a
// store-backed router into one that 404s every target it does not hold in RAM.
func TestHostRouterReloadKeepsResolver(t *testing.T) {
	hr := NewHostRouter()
	called := false
	hr.SetResolver(func(_ context.Context, host string) (http.Handler, bool) {
		called = true
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), true
	})
	hr.Reload(map[string]http.Handler{}, nil)

	rec := httptest.NewRecorder()
	hr.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-q/", nil))
	if !called {
		t.Fatal("resolver was dropped by Reload")
	}
}

func TestHostRouterSetResolverNilRestoresTableOnly(t *testing.T) {
	hr := NewHostRouter()
	hr.SetResolver(func(_ context.Context, host string) (http.Handler, bool) {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "resolver-body")
		}), true
	})
	hr.SetResolver(nil)

	rec := httptest.NewRecorder()
	hr.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-n/", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status with nil resolver = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// A nil table must fall back to a private one rather than panicking, so a
// single-host caller does not have to construct a table.
func TestNewSharedLazyHandlerNilTableFallsBack(t *testing.T) {
	lru := cluster.NewLRUCache[string, *TargetConfigHandler](4)
	lh := NewLazyHandler("svc-nil", lru, func(ctx context.Context) (*TargetConfigHandler, error) {
		return lazyEchoHandler(t, "nil-table-body"), nil
	}, nil)
	if lh.flights == nil {
		t.Fatal("nil table left the handler without a flight table")
	}

	rec := httptest.NewRecorder()
	lh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-nil/", nil))
	if rec.Body.String() != "nil-table-body" {
		t.Fatalf("body = %q, want %q", rec.Body.String(), "nil-table-body")
	}
}

// "No table" is a legitimate state for a store-backed router, but Upsert must
// still work on it: assigning into a nil map panics.
func TestHostRouterReloadNilTableStaysUsable(t *testing.T) {
	hr := NewHostRouter()
	hr.Reload(nil, nil)

	hr.Upsert("svc-nil-table", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "body")
	}))
	rec := httptest.NewRecorder()
	hr.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-nil-table/", nil))
	if rec.Body.String() != "body" {
		t.Fatalf("body = %q, want %q", rec.Body.String(), "body")
	}
}
