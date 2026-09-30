package handler

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	disk "github.com/cinvat/peretum/internal/cache/disk"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/plugin/manager"
)

func TestServeHTTP_CacheMissOwnerAndHit(t *testing.T) {
	upURL, _ := newUpstream(t)
	stub := mkStub()
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location: &config.LocationConfig{
			Path:  "/",
			Cache: true,
		},
		pluginMgr: pm,
	})

	// Miss: goes upstream, writes cache, sets the flight response.
	rec1 := doRequest(th, "GET", "http://example.com/data")
	if rec1.Code != 200 || rec1.Body.String() != "UPSTREAM:/data" {
		t.Errorf("miss response code=%d body=%q", rec1.Code, rec1.Body.String())
	}
	if stub.missCount != 1 || stub.hitCount != 0 {
		t.Errorf("miss=%d hit=%d", stub.missCount, stub.hitCount)
	}
	if stub.afterProxy != 1 {
		t.Errorf("afterProxy=%d", stub.afterProxy)
	}

	// Hit: served straight from cache (post-transform content).
	rec2 := doRequest(th, "GET", "http://example.com/data")
	if rec2.Code != 200 {
		t.Errorf("hit code=%d", rec2.Code)
	}
	if got := rec2.Body.String(); got != "STORE:TRANS:UPSTREAM:/data" {
		t.Errorf("hit body=%q (want cached transformed body)", got)
	}
	if rec2.Header().Get("X-Cache") != "HIT" {
		t.Errorf("X-Cache=%q", rec2.Header().Get("X-Cache"))
	}
	if stub.hitCount != 1 {
		t.Errorf("hit hook count=%d", stub.hitCount)
	}
}

func TestServeHTTP_CacheTTL(t *testing.T) {
	upURL, state := newUpstream(t)
	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location: &config.LocationConfig{
			Path:     "/",
			Cache:    true,
			CacheTTL: "200ms",
		},
	})

	// Miss: stores the response into the cache.
	rec1 := doRequest(th, "GET", "http://example.com/ttl")
	if rec1.Code != 200 {
		t.Fatalf("first request code=%d", rec1.Code)
	}

	// Within the TTL the second request is a cache hit.
	rec2 := doRequest(th, "GET", "http://example.com/ttl")
	if rec2.Code != 200 || rec2.Header().Get("X-Cache") != "HIT" {
		t.Errorf("within-TTL request code=%d X-Cache=%q", rec2.Code, rec2.Header().Get("X-Cache"))
	}
	if state.hits != 1 {
		t.Fatalf("upstream hits=%d after within-TTL request", state.hits)
	}

	// Once the per-location TTL elapses the stale entry is dropped and the
	// request is re-fetched upstream instead of being served from cache.
	time.Sleep(600 * time.Millisecond)
	rec3 := doRequest(th, "GET", "http://example.com/ttl")
	if rec3.Code != 200 || rec3.Header().Get("X-Cache") == "HIT" {
		t.Errorf("post-TTL request code=%d X-Cache=%q", rec3.Code, rec3.Header().Get("X-Cache"))
	}
	if state.hits != 2 {
		t.Errorf("upstream hits=%d after TTL expiry (want 2)", state.hits)
	}
}

func TestNewTargetHandler_CacheTTLParsing(t *testing.T) {
	upURL, _ := newUpstream(t)

	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", Cache: true, CacheTTL: "5s"},
	})
	if th.cacheTTL != 5*time.Second {
		t.Errorf("parsed cacheTTL=%v", th.cacheTTL)
	}

	bad, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", Cache: true, CacheTTL: "not-a-duration"},
	})
	if bad.cacheTTL != 0 {
		t.Errorf("invalid CacheTTL should leave cacheTTL=0, got %v", bad.cacheTTL)
	}
}

func TestServeWaiterFromFlight_Success(t *testing.T) {
	stub := mkStub()
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, dc := setupHandler(t, fixture{
		location:  &config.LocationConfig{Path: "/", Cache: true},
		pluginMgr: pm,
	})

	key := disk.CacheKey("test-target", "/wf")
	if err := dc.SetResponseToCache(key, "test-target", "/wf", 200, http.Header{"Content-Type": {"text/plain"}}, []byte("cached-body")); err != nil {
		t.Fatal(err)
	}
	flight := &singleFlight{ch: make(chan struct{}), resp: &http.Response{StatusCode: 200}}
	th.flightMu.Store(key, flight)
	t.Cleanup(func() { th.flightMu.Delete(key) })

	rec := httptest.NewRecorder()
	if !th.serveWaiterFromFlight(rec, httptest.NewRequest("GET", "http://example.com/wf", nil), key, flight) {
		t.Fatal("expected the waiter to be served from cache")
	}
	if rec.Code != 200 || rec.Body.String() != "cached-body" {
		t.Errorf("body=%q", rec.Body.String())
	}
	if rec.Header().Get("X-Cache") != "HIT" {
		t.Errorf("X-Cache=%q", rec.Header().Get("X-Cache"))
	}
	if stub.hitCount != 1 {
		t.Errorf("hitCount=%d", stub.hitCount)
	}

	// The same flight attributes that make the waiter fall through.
	fails := &singleFlight{ch: make(chan struct{})}
	if th.serveWaiterFromFlight(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), key, fails) {
		t.Error("nil resp flight should not be served")
	}
	fails.resp = &http.Response{StatusCode: 500}
	fails.err = errors.New("boom")
	if th.serveWaiterFromFlight(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), key, fails) {
		t.Error("errored flight should not be served")
	}
}

func TestServeHTTP_CacheWaiter_ErrFallthrough(t *testing.T) {
	upURL, _ := newUpstream(t)
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(mkStub())

	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", Cache: true},
		pluginMgr:   pm,
	})

	key := disk.CacheKey("test-target", "/fail")
	flight := &singleFlight{ch: make(chan struct{}), err: errors.New("upstream blew up")}
	close(flight.ch)
	th.flightMu.Store(key, flight)
	t.Cleanup(func() { th.flightMu.Delete(key) })

	// The flight failed: the waiter falls through to a normal proxy fetch.
	rec := doRequest(th, "GET", "http://example.com/fail")
	if rec.Code != 200 || rec.Body.String() != "UPSTREAM:/fail" {
		t.Errorf("code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestServeHTTP_CacheWaiter_RespCacheMissing(t *testing.T) {
	upURL, _ := newUpstream(t)
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(mkStub())

	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", Cache: true},
		pluginMgr:   pm,
	})

	// Flight claims a successful response, but nothing landed in the
	// cache: the waiter falls through and re-fetches upstream.
	key := disk.CacheKey("test-target", "/gone")
	flight := &singleFlight{ch: make(chan struct{}), resp: &http.Response{StatusCode: 200}}
	close(flight.ch)
	th.flightMu.Store(key, flight)
	t.Cleanup(func() { th.flightMu.Delete(key) })

	rec := doRequest(th, "GET", "http://example.com/gone")
	if rec.Code != 200 || rec.Body.String() != "UPSTREAM:/gone" {
		t.Errorf("code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestServeHTTP_CacheWaiter_Success(t *testing.T) {
	// The upstream blocks the owner until the waiter is parked, making
	// the request-coalescing path deterministic.
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, "UPSTREAM:/w")
	}))
	t.Cleanup(srv.Close)

	stub := mkStub()
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, _ := setupHandler(t, fixture{
		upstreamURL: srv.URL,
		location:    &config.LocationConfig{Path: "/", Cache: true},
		pluginMgr:   pm,
	})

	ownerDone := make(chan *httptest.ResponseRecorder)
	go func() {
		ownerDone <- doRequest(th, "GET", "http://example.com/w")
	}()

	<-entered // owner is now blocked in the upstream fetch

	waiterDone := make(chan *httptest.ResponseRecorder)
	go func() {
		waiterDone <- doRequest(th, "GET", "http://example.com/w")
	}()

	// Give the waiter time to reach its <-flight.ch park before the
	// owner finishes, so the coalescing path is exercised deterministically.
	time.Sleep(200 * time.Millisecond)
	close(release)

	rec := <-waiterDone
	if rec.Code != 200 || rec.Body.String() != "STORE:TRANS:UPSTREAM:/w" {
		t.Errorf("waiter code=%d body=%q", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Cache") != "HIT" {
		t.Errorf("X-Cache=%q", rec.Header().Get("X-Cache"))
	}
	if stub.hitCount != 1 {
		t.Errorf("hitCount=%d", stub.hitCount)
	}

	owner := <-ownerDone
	if owner.Code != 200 || owner.Body.String() != "UPSTREAM:/w" {
		t.Errorf("owner code=%d body=%q", owner.Code, owner.Body.String())
	}
}

// --- plugins --------------------------------------------------------------
