package handler

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	disk "github.com/cinvat/peretum/internal/cache/disk"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/loadbalancer"
	"github.com/cinvat/peretum/internal/plugin/manager"
	"github.com/cinvat/peretum/plugins/base"
)

// --- stub plugins ---------------------------------------------------------

type stubPlugin struct {
	*base.BasePlugin
	beforeErr    error
	transformErr error
	storeErr     error
	shouldCache  bool

	beforeCount int
	afterProxy  int
	hitCount    int
	missCount   int
	logged      []string
}

func (s *stubPlugin) BeforeProxy(w http.ResponseWriter, r *http.Request, target, location string) error {
	s.beforeCount++
	return s.beforeErr
}
func (s *stubPlugin) AfterProxy(w http.ResponseWriter, r *http.Request, target, location string, resp *http.Response) error {
	s.afterProxy++
	return nil
}
func (s *stubPlugin) TransformResponseBody(target, location, contentType string, body []byte) ([]byte, error) {
	if s.transformErr != nil {
		return nil, s.transformErr
	}
	return append([]byte("TRANS:"), body...), nil
}
func (s *stubPlugin) BeforeCacheStore(key, target, location string, status int, headers http.Header, body []byte) ([]byte, error) {
	if s.storeErr != nil {
		return nil, s.storeErr
	}
	return append([]byte("STORE:"), body...), nil
}
func (s *stubPlugin) AfterCacheHit(w http.ResponseWriter, r *http.Request, key, target, location string) error {
	return nil
}
func (s *stubPlugin) ShouldCache(target, location string, status int, headers http.Header, body []byte) bool {
	return s.shouldCache
}
func (s *stubPlugin) RecordRequest(target, location, matchType string, cached bool, duration float64) {
}
func (s *stubPlugin) RecordCacheHit(target, location string) {
	s.hitCount++
}
func (s *stubPlugin) RecordCacheMiss(target, location string) {
	s.missCount++
}
func (s *stubPlugin) RecordCacheEviction()   {}
func (s *stubPlugin) RecordCacheExpiration() {}
func (s *stubPlugin) LogError(level, target, location, requestID, msg string, fields map[string]any) {
	s.logged = append(s.logged, level+": "+msg)
}
func (s *stubPlugin) StartRequest(w http.ResponseWriter, r *http.Request, target, location string) http.ResponseWriter {
	return w
}
func (s *stubPlugin) FinishRequest(w http.ResponseWriter, r *http.Request, target, location string, cached bool) {
}

func mkStub() *stubPlugin {
	return &stubPlugin{BasePlugin: base.NewBasePlugin("test"), shouldCache: true}
}

// plainPlugin implements base.Plugin and no hooks at all.
type plainPlugin struct{ *base.BasePlugin }

// wrapPlugin is a "compression" plugin that wraps the proxy handler.
type wrapPlugin struct {
	*base.BasePlugin
	calls int
}

func (w *wrapPlugin) WrapHandler(h http.Handler) http.Handler {
	w.calls++
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("X-Wrapped", "yes")
		h.ServeHTTP(rw, r)
	})
}

// --- fixtures -------------------------------------------------------------

type upstreamState struct {
	mu       sync.Mutex
	hits     int
	requests []struct {
		Path   string
		Query  string
		Host   string
		Header http.Header
	}
}

func newUpstream(t *testing.T) (string, *upstreamState) {
	t.Helper()
	state := &upstreamState{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		state.hits++
		state.requests = append(state.requests, struct {
			Path   string
			Query  string
			Host   string
			Header http.Header
		}{Path: r.URL.Path, Query: r.URL.RawQuery, Host: r.Host, Header: r.Header.Clone()})
		state.mu.Unlock()
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "UPSTREAM:%s", r.URL.Path)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, state
}

func newDownstream(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()
	return url
}

type fixture struct {
	targetName  string
	targetHost  string
	location    *config.LocationConfig
	upstreamURL string
	upstreams   []config.UpstreamConfig
	lbAlgorithm string
	maxSize     int64
	bodyLimit   int64
	semCap      int
	prefilled   int
	unlimitedW  bool
	pluginMgr   *manager.PluginManager
	direct      bool
}

func setupHandler(t *testing.T, f fixture) (*TargetHandler, *disk.DiskCache) {
	t.Helper()
	if f.targetName == "" {
		f.targetName = "test-target"
	}
	if f.location == nil {
		f.location = &config.LocationConfig{Path: "/"}
	}
	if f.maxSize <= 0 {
		f.maxSize = 100 << 20
	}
	dc, err := disk.New(t.TempDir(), f.maxSize, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dc.Close)

	var sem chan struct{}
	switch {
	case f.unlimitedW:
		sem = nil
	case f.semCap > 0:
		sem = make(chan struct{}, f.semCap)
		for i := 0; i < f.prefilled; i++ {
			sem <- struct{}{}
		}
	default:
		sem = make(chan struct{}, maxWriteWorkers)
	}

	target := &config.TargetConfig{Name: f.targetName}
	if f.targetHost != "" {
		target.Host = f.targetHost
	}
	if len(f.upstreams) > 0 {
		target.Upstreams = f.upstreams
	} else if f.upstreamURL != "" {
		target.Upstreams = []config.UpstreamConfig{{URL: f.upstreamURL}}
	}
	var lb loadbalancer.LoadBalancer
	lbUpstreams := make([]*loadbalancer.Upstream, 0, len(target.Upstreams))
	for _, uc := range target.Upstreams {
		if uc.URL == "" {
			continue
		}
		weight := uc.Weight
		if weight == 0 {
			weight = 1
		}
		lbUpstreams = append(lbUpstreams, &loadbalancer.Upstream{URL: uc.URL, Weight: weight})
	}
	lb = loadbalancer.New(f.lbAlgorithm, lbUpstreams)

	if f.direct {
		return &TargetHandler{
			target: target, location: f.location, diskCache: dc,
			writeSem: sem, lb: lb, pluginMgr: f.pluginMgr,
		}, dc
	}
	return NewTargetHandler(target, f.location, dc, sem, lb, f.pluginMgr, f.bodyLimit), dc
}

func doRequest(th *TargetHandler, method, target string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	th.ServeHTTP(rec, req)
	return rec
}

// --- plain proxy ----------------------------------------------------------

func TestServeHTTP_PlainProxy(t *testing.T) {
	upURL, state := newUpstream(t)
	th, _ := setupHandler(t, fixture{upstreamURL: upURL})

	rec := doRequest(th, "GET", "http://example.com/hello?q=1")

	if rec.Code != 200 {
		t.Errorf("code=%d", rec.Code)
	}
	if got := rec.Body.String(); got != "UPSTREAM:/hello" {
		t.Errorf("body=%q", got)
	}
	if state.hits != 1 {
		t.Errorf("upstream hits=%d", state.hits)
	}
}

// TestServeHTTP_RoundRobinAlternates guards against the LB pick being ignored:
// the reverse proxy's Rewrite pinned the outgoing URL to the first upstream,
// so every request went to upstream[0] regardless of round-robin selection.
func TestServeHTTP_RoundRobinAlternates(t *testing.T) {
	upA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "UPSTREAM-A:"+r.URL.Path)
	}))
	t.Cleanup(upA.Close)
	upB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "UPSTREAM-B:"+r.URL.Path)
	}))
	t.Cleanup(upB.Close)

	th, _ := setupHandler(t, fixture{
		upstreams:   []config.UpstreamConfig{{URL: upA.URL}, {URL: upB.URL}},
		lbAlgorithm: "round_robin",
		location:    &config.LocationConfig{Path: "/"},
	})

	want := []string{"UPSTREAM-A:", "UPSTREAM-B:"}
	for i := 0; i < 4; i++ {
		rec := doRequest(th, "GET", fmt.Sprintf("http://example.com/alt%d", i))
		if rec.Code != 200 {
			t.Fatalf("req %d: code=%d", i, rec.Code)
		}
		if body := rec.Body.String(); !strings.HasPrefix(body, want[i%2]) {
			t.Errorf("req %d: body=%q, want prefix %q", i, body, want[i%2])
		}
	}
}

// TestServeHTTP_PinnedUpstream serves through a location-pinned upstream,
// bypassing the load balancer entirely.
func TestServeHTTP_PinnedUpstream(t *testing.T) {
	pinned := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "PINNED:"+r.URL.Path)
	}))
	t.Cleanup(pinned.Close)
	unused := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "UNUSED")
	}))
	t.Cleanup(unused.Close)

	th, _ := setupHandler(t, fixture{
		upstreams: []config.UpstreamConfig{{URL: unused.URL}},
		location:  &config.LocationConfig{Path: "/", Proxy: &config.ProxyLocationConfig{Upstream: pinned.URL}},
	})

	rec := doRequest(th, "GET", "http://example.com/path")
	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	if body := rec.Body.String(); body != "PINNED:/path" {
		t.Errorf("body=%q", body)
	}
}

// quirkUpstream mirrors example.com behind Cloudflare: it gzip-compresses the
// response whenever the request advertises gzip but omits the Content-Encoding
// header, and serves plain HTML otherwise.
func quirkUpstream(t *testing.T) (upURL string, sawAE *bool, recBody *string) {
	t.Helper()
	var mu sync.Mutex
	sawAE = new(bool)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		*sawAE = r.Header.Get("Accept-Encoding") != ""
		mu.Unlock()
		body := "<html><body>upstream-clean</body></html>"
		if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			var buf bytes.Buffer
			gz := gzip.NewWriter(&buf)
			_, _ = gz.Write([]byte(body))
			_ = gz.Close()
			w.Header().Set("Content-Type", "text/html")
			// Deliberately no Content-Encoding header.
			_, _ = w.Write(buf.Bytes())
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, sawAE, recBody
}

func TestServeHTTP_UpstreamGzipWithoutContentEncoding(t *testing.T) {
	upURL, sawAE, _ := quirkUpstream(t)
	th, _ := setupHandler(t, fixture{upstreamURL: upURL})

	rec := doRequest(th, "GET", "http://example.com/hello")
	// The proxied upstream request must not advertise gzip (the Rewrite
	// strips Accept-Encoding and the transport must not re-add it), otherwise
	// the Cloudflare-style gzip-without-header response is buffered raw.
	if *sawAE {
		t.Errorf("upstream saw an advertised Accept-Encoding")
	}
	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	if got := rec.Body.String(); got != "<html><body>upstream-clean</body></html>" {
		t.Fatalf("body=%q want clean upstream HTML", got)
	}
}

// --- caching --------------------------------------------------------------

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

func TestServeHTTP_BeforeProxyError(t *testing.T) {
	upURL, state := newUpstream(t)
	stub := mkStub()
	stub.beforeErr = errors.New("deny")
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, _ := setupHandler(t, fixture{upstreamURL: upURL, pluginMgr: pm})
	rec := doRequest(th, "GET", "http://example.com/")

	if state.hits != 0 {
		t.Errorf("upstream should not have been hit, got %d", state.hits)
	}
	if stub.beforeCount != 1 {
		t.Errorf("beforeCount=%d", stub.beforeCount)
	}
	if rec.Header().Get("X-Cache") == "HIT" {
		t.Error("unexpected cache header")
	}
}

func TestServeHTTP_UpstreamDown(t *testing.T) {
	down := newDownstream(t)
	stub := mkStub()
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, _ := setupHandler(t, fixture{upstreamURL: down, pluginMgr: pm})
	rec := doRequest(th, "GET", "http://example.com/x")

	if rec.Code != http.StatusBadGateway {
		t.Errorf("code=%d, want 502", rec.Code)
	}
	if rec.Body.String() != "Bad Gateway\n" {
		t.Errorf("body=%q", rec.Body.String())
	}
	if len(stub.logged) == 0 || !strings.HasPrefix(stub.logged[0], "ERROR: proxy error") {
		t.Errorf("logged=%v", stub.logged)
	}
}

func TestServeHTTP_PluginTransformError(t *testing.T) {
	upURL, _ := newUpstream(t)
	stub := mkStub()
	stub.transformErr = errors.New("no transform")
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", Cache: true},
		pluginMgr:   pm,
	})
	rec := doRequest(th, "GET", "http://example.com/t")
	if rec.Code != 200 {
		t.Errorf("code=%d", rec.Code)
	}
	found := false
	for _, l := range stub.logged {
		if strings.HasPrefix(l, "WARN: response body transformation failed") {
			found = true
		}
	}
	if !found {
		t.Errorf("transform error not logged: %v", stub.logged)
	}
}

func TestServeHTTP_BeforeCacheStoreError(t *testing.T) {
	upURL, _ := newUpstream(t)
	stub := mkStub()
	stub.storeErr = errors.New("no store")
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", Cache: true},
		pluginMgr:   pm,
	})
	doRequest(th, "GET", "http://example.com/s")

	found := false
	for _, l := range stub.logged {
		if strings.HasPrefix(l, "WARN: before cache store failed") {
			found = true
		}
	}
	if !found {
		t.Errorf("store error not logged: %v", stub.logged)
	}
}

func TestServeHTTP_ShouldCacheFalse(t *testing.T) {
	upURL, _ := newUpstream(t)
	stub := mkStub()
	stub.shouldCache = false
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, dc := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", Cache: true},
		pluginMgr:   pm,
	})
	rec := doRequest(th, "GET", "http://example.com/nc")
	if rec.Code != 200 {
		t.Errorf("code=%d", rec.Code)
	}
	// Nothing must have been cached.
	key := disk.CacheKey("test-target", "/nc")
	if err := dc.StreamCachedResponse(httptest.NewRecorder(), key); err == nil {
		t.Error("should not have cached anything")
	}
}

func TestServeHTTP_CacheWriteSemFull(t *testing.T) {
	upURL, _ := newUpstream(t)
	stub := mkStub()
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", Cache: true},
		semCap:      1,
		prefilled:   1,
		pluginMgr:   pm,
	})
	rec := doRequest(th, "GET", "http://example.com/full")
	if rec.Code != 200 || rec.Body.String() != "UPSTREAM:/full" {
		t.Errorf("code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestServeHTTP_CacheUnlimitedWorkers(t *testing.T) {
	upURL, _ := newUpstream(t)
	th, dc := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", Cache: true},
		unlimitedW:  true,
	})

	rec1 := doRequest(th, "GET", "http://example.com/unlimited")
	if rec1.Code != 200 || rec1.Body.String() != "UPSTREAM:/unlimited" {
		t.Errorf("miss code=%d body=%q", rec1.Code, rec1.Body.String())
	}
	key := disk.CacheKey("test-target", "/unlimited")
	if err := dc.StreamCachedResponse(httptest.NewRecorder(), key); err != nil {
		t.Fatalf("expected byte-limited disk cache entry to be written with nil writeSem: %v", err)
	}

	rec2 := doRequest(th, "GET", "http://example.com/unlimited")
	if rec2.Header().Get("X-Cache") != "HIT" || rec2.Body.String() != "UPSTREAM:/unlimited" {
		t.Errorf("hit code=%d body=%q x-cache=%q", rec2.Code, rec2.Body.String(), rec2.Header().Get("X-Cache"))
	}
}

func TestResolveMaxBodySize(t *testing.T) {
	th := &TargetHandler{maxBodySize: -1}
	if got := th.resolveMaxBodySize(); got != -1 {
		t.Errorf("maxBodySize=-1 resolves to %d, want -1 (unlimited)", got)
	}
	th = &TargetHandler{maxBodySize: 0}
	if got := th.resolveMaxBodySize(); got != maxBodyReadSize {
		t.Errorf("maxBodySize=0 resolves to %d, want default %d", got, maxBodyReadSize)
	}
	th = &TargetHandler{maxBodySize: 4096}
	if got := th.resolveMaxBodySize(); got != 4096 {
		t.Errorf("maxBodySize=4096 resolves to %d", got)
	}
}

func TestServeHTTP_CacheWriteFail(t *testing.T) {
	upURL, _ := newUpstream(t)
	stub := mkStub()
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", Cache: true},
		maxSize:     1, // too small for the response
		pluginMgr:   pm,
	})
	rec := doRequest(th, "GET", "http://example.com/big")
	if rec.Code != 200 {
		t.Errorf("code=%d", rec.Code)
	}
	found := false
	for _, l := range stub.logged {
		if strings.HasPrefix(l, "WARN: cache write failed") {
			found = true
		}
	}
	if !found {
		t.Errorf("cache write error not logged: %v", stub.logged)
	}
}

// --- CORS ----------------------------------------------------------------

func TestServeHTTP_CORS(t *testing.T) {
	full := &config.LocationConfig{
		Path: "/",
		CORS: &config.CORSConfig{
			Enabled:          true,
			AllowOrigins:     []string{"*"},
			AllowMethods:     []string{"GET"},
			AllowHeaders:     []string{"X-Custom"},
			ExposeHeaders:    []string{"X-Expose"},
			AllowCredentials: true,
			MaxAge:           10,
		},
	}
	upURL, _ := newUpstream(t)
	th, _ := setupHandler(t, fixture{upstreamURL: upURL, location: full})

	t.Run("preflight all branches", func(t *testing.T) {
		req := httptest.NewRequest("OPTIONS", "http://example.com/api", nil)
		req.Header.Set("Origin", "http://client")
		req.Header.Set("Access-Control-Request-Headers", "X-Anything")
		rec := httptest.NewRecorder()
		th.ServeHTTP(rec, req)

		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://client" {
			t.Errorf("allow-origin=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET" {
			t.Errorf("allow-methods=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "X-Custom" {
			t.Errorf("allow-headers=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "X-Expose" {
			t.Errorf("expose-headers=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
			t.Errorf("credentials=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Max-Age"); got != "10" {
			t.Errorf("max-age=%q", got)
		}
	})

	t.Run("preflight default branches", func(t *testing.T) {
		def := upURL + ""
		thd, _ := setupHandler(t, fixture{
			upstreamURL: def,
			location: &config.LocationConfig{
				Path: "/",
				CORS: &config.CORSConfig{Enabled: true, AllowOrigins: []string{"http://good"}},
			},
		})
		req := httptest.NewRequest("OPTIONS", "http://example.com/api", nil)
		req.Header.Set("Origin", "http://good")
		req.Header.Set("Access-Control-Request-Headers", "X-A, X-B")
		rec := httptest.NewRecorder()
		thd.ServeHTTP(rec, req)

		if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET, POST, PUT, DELETE, OPTIONS, HEAD" {
			t.Errorf("default methods=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "X-A, X-B" {
			t.Errorf("echoed headers=%q", got)
		}
		if rec.Header().Get("Access-Control-Allow-Credentials") != "" {
			t.Error("no credentials expected")
		}
		if rec.Header().Get("Access-Control-Max-Age") != "" {
			t.Error("no max-age expected")
		}
	})

	t.Run("non-OPTIONS continues to origin", func(t *testing.T) {
		req := httptest.NewRequest("GET", "http://example.com/g", nil)
		req.Header.Set("Origin", "http://client")
		rec := httptest.NewRecorder()
		th.ServeHTTP(rec, req)
		if rec.Body.String() != "UPSTREAM:/g" {
			t.Errorf("body=%q", rec.Body.String())
		}
		if rec.Header().Get("Access-Control-Allow-Origin") != "http://client" {
			t.Errorf("allow-origin=%q", rec.Header().Get("Access-Control-Allow-Origin"))
		}
	})

	t.Run("no origin", func(t *testing.T) {
		req := httptest.NewRequest("OPTIONS", "http://example.com/api", nil)
		rec := httptest.NewRecorder()
		th.ServeHTTP(rec, req)
		if rec.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Error("no CORS headers without Origin")
		}
	})

	t.Run("disallowed origin", func(t *testing.T) {
		thd, _ := setupHandler(t, fixture{
			location: &config.LocationConfig{
				Path: "/",
				CORS: &config.CORSConfig{Enabled: true, AllowOrigins: []string{"http://good"}},
			},
		})
		req := httptest.NewRequest("OPTIONS", "http://example.com/api", nil)
		req.Header.Set("Origin", "http://evil")
		rec := httptest.NewRecorder()
		thd.ServeHTTP(rec, req)
		if rec.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Error("origin should not be allowed")
		}
	})
}

func TestServeHTTP_CORS_Disabled(t *testing.T) {
	upURL, _ := newUpstream(t)
	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", CORS: &config.CORSConfig{Enabled: false}},
	})
	req := httptest.NewRequest("OPTIONS", "http://example.com/api", nil)
	req.Header.Set("Origin", "http://client")
	rec := httptest.NewRecorder()
	th.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("CORS disabled but headers set")
	}
}

// --- headers --------------------------------------------------------------

func TestServeHTTP_RequestHeaders(t *testing.T) {
	upURL, state := newUpstream(t)
	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location: &config.LocationConfig{
			Path: "/",
			Headers: &config.HeadersConfig{
				RequestAdd:     map[string]string{"X-Add": "v1"},
				RequestRemove:  []string{"X-Remove"},
				ResponseAdd:    map[string]string{"X-Resp": "r1"},
				ResponseRemove: []string{"X-Resp-Remove"},
			},
		},
	})

	req := httptest.NewRequest("GET", "http://example.com/h", nil)
	req.Header.Set("X-Remove", "gone")
	req.Header.Set("X-Resp-Remove", "gone-when-reply")
	rec := httptest.NewRecorder()
	th.ServeHTTP(rec, req)

	s := state.requests[0]
	if s.Header.Get("X-Add") != "v1" {
		t.Errorf("request X-Add=%q", s.Header.Get("X-Add"))
	}
	if s.Header.Get("X-Remove") != "" {
		t.Error("request X-Remove should be stripped")
	}
	if rec.Header().Get("X-Resp") != "r1" {
		t.Errorf("response X-Resp=%q", rec.Header().Get("X-Resp"))
	}
	if rec.Header().Get("X-Resp-Remove") != "" {
		t.Error("response X-Resp-Remove should be stripped")
	}
}

func TestServeHTTP_SanitizesUpstreamHeaders(t *testing.T) {
	// The upstream dresses its response like a CDN (Cloudflare-style).
	upURL, _ := leakyUpstream(t)
	th, _ := setupHandler(t, fixture{upstreamURL: upURL})

	rec := doRequest(th, "GET", "http://example.com/leaky")
	if rec.Header().Get("Server") != "peretum" {
		t.Errorf("Server=%q, want peretum", rec.Header().Get("Server"))
	}
	for _, leak := range []string{"Cf-Ray", "Cf-Cache-Status", "Age", "X-Powered-By", "X-Cache-Hits", "Via"} {
		if v := rec.Header().Get(leak); v != "" {
			t.Errorf("upstream leak header %s=%q should be stripped", leak, v)
		}
	}
	if got := rec.Header().Get("X-Cache"); got != "MISS" {
		t.Errorf("live X-Cache=%q, want MISS (upstream X-Cache value replaced)", got)
	}
}

func TestServeHTTP_CacheHitHidesUpstreamHeaders(t *testing.T) {
	upURL, _ := leakyUpstream(t)
	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", Cache: true},
	})

	// Miss populates the cache, hit serves from it. Neither may leak the
	// upstream CDN headers.
	doRequest(th, "GET", "http://example.com/leaky")
	rec := doRequest(th, "GET", "http://example.com/leaky")

	if rec.Header().Get("X-Cache") != "HIT" {
		t.Errorf("X-Cache=%q, want HIT", rec.Header().Get("X-Cache"))
	}
	if rec.Header().Get("Server") != "peretum" {
		t.Errorf("Server=%q, want peretum", rec.Header().Get("Server"))
	}
	for _, leak := range []string{"Cf-Ray", "Cf-Cache-Status", "Age", "X-Powered-By", "X-Cache-Hits"} {
		if v := rec.Header().Get(leak); v != "" {
			t.Errorf("cache hit leaked %s=%q", leak, v)
		}
	}
}

// leakyUpstream mirrors a CDN-fronted origin that leaks its identity in
// response headers.
func leakyUpstream(t *testing.T) (string, *upstreamState) {
	t.Helper()
	state := &upstreamState{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		state.hits++
		state.mu.Unlock()
		w.Header().Set("Server", "cloudflare")
		w.Header().Set("Cf-Ray", "a3c6b6676d077104-AMS")
		w.Header().Set("Cf-Cache-Status", "HIT")
		w.Header().Set("Age", "14098")
		w.Header().Set("X-Powered-By", "PHP/8.2")
		w.Header().Set("X-Cache", "HIT")
		w.Header().Set("X-Cache-Hits", "3")
		w.Header().Set("Via", "1.1 CloudFront")
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>clean</html>"))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, state
}

func TestServeHTTP_RequestHeaders_AddNil(t *testing.T) {
	upURL, state := newUpstream(t)
	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location: &config.LocationConfig{
			Path: "/",
			Headers: &config.HeadersConfig{
				RequestRemove: []string{"X-Remove"},
			},
		},
	})
	req := httptest.NewRequest("GET", "http://example.com/h", nil)
	req.Header.Set("X-Remove", "gone")
	th.ServeHTTP(httptest.NewRecorder(), req)

	if got := state.requests[0].Header.Get("X-Remove"); got != "" {
		t.Errorf("X-Remove=%q", got)
	}
}

func TestServeHTTP_PassHostHeader(t *testing.T) {
	t.Run("pass host", func(t *testing.T) {
		upURL, state := newUpstream(t)
		th, _ := setupHandler(t, fixture{
			upstreamURL: upURL,
			location: &config.LocationConfig{
				Path:  "/",
				Proxy: &config.ProxyLocationConfig{PassHostHeader: true},
			},
		})
		req := httptest.NewRequest("GET", "http://client.example.com/hh", nil)
		th.ServeHTTP(httptest.NewRecorder(), req)
		if state.requests[0].Host != "client.example.com" {
			t.Errorf("host=%q", state.requests[0].Host)
		}
	})

	t.Run("use upstream host", func(t *testing.T) {
		upURL, state := newUpstream(t)
		th, _ := setupHandler(t, fixture{
			upstreamURL: upURL,
			location: &config.LocationConfig{
				Path:  "/",
				Proxy: &config.ProxyLocationConfig{PassHostHeader: false},
			},
		})
		req := httptest.NewRequest("GET", "http://client.example.com/hh", nil)
		th.ServeHTTP(httptest.NewRecorder(), req)
		if got := state.requests[0].Host; got != strings.TrimPrefix(upURL, "http://") {
			t.Errorf("host=%q want %q", got, strings.TrimPrefix(upURL, "http://"))
		}
	})
}

func TestServeHTTP_TargetHost(t *testing.T) {
	t.Run("overrides pass_host_header", func(t *testing.T) {
		upURL, state := newUpstream(t)
		th, _ := setupHandler(t, fixture{
			upstreamURL: upURL,
			targetHost:  "override.example.com",
			location: &config.LocationConfig{
				Path:  "/",
				Proxy: &config.ProxyLocationConfig{PassHostHeader: true},
			},
		})
		req := httptest.NewRequest("GET", "http://client.example.com/hh", nil)
		th.ServeHTTP(httptest.NewRecorder(), req)
		if got := state.requests[0].Host; got != "override.example.com" {
			t.Errorf("host=%q", got)
		}
	})

	t.Run("overrides upstream host", func(t *testing.T) {
		upURL, state := newUpstream(t)
		th, _ := setupHandler(t, fixture{
			upstreamURL: upURL,
			targetHost:  "override.example.com",
			location: &config.LocationConfig{
				Path: "/",
			},
		})
		req := httptest.NewRequest("GET", "http://client.example.com/hh", nil)
		th.ServeHTTP(httptest.NewRecorder(), req)
		if got := state.requests[0].Host; got != "override.example.com" {
			t.Errorf("host=%q", got)
		}
	})

	t.Run("empty falls back to upstream host", func(t *testing.T) {
		upURL, state := newUpstream(t)
		th, _ := setupHandler(t, fixture{
			upstreamURL: upURL,
			location: &config.LocationConfig{
				Path: "/",
			},
		})
		req := httptest.NewRequest("GET", "http://client.example.com/hh", nil)
		th.ServeHTTP(httptest.NewRecorder(), req)
		if got := state.requests[0].Host; got != strings.TrimPrefix(upURL, "http://") {
			t.Errorf("host=%q want %q", got, strings.TrimPrefix(upURL, "http://"))
		}
	})
}

// --- rewrite & redirect ---------------------------------------------------

func TestServeHTTP_Rewrite(t *testing.T) {
	upURL, state := newUpstream(t)
	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location: &config.LocationConfig{
			Path: "/",
			Rewrite: &config.RewriteConfig{
				Pattern:     "^/old/(.*)$",
				Replacement: "/new/$1",
			},
		},
	})
	rec := doRequest(th, "GET", "http://example.com/old/abc?q=1")
	if rec.Body.String() != "UPSTREAM:/new/abc" {
		t.Errorf("body=%q", rec.Body.String())
	}
	if state.requests[0].Path != "/new/abc" {
		t.Errorf("upstream path=%q", state.requests[0].Path)
	}
	if state.requests[0].Query != "q=1" {
		t.Errorf("upstream query=%q", state.requests[0].Query)
	}
}

func TestServeHTTP_Rewrite_LazyCompile(t *testing.T) {
	upURL, state := newUpstream(t)
	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location: &config.LocationConfig{
			Path: "/",
			Rewrite: &config.RewriteConfig{
				Pattern:     "^/old/(.*)$",
				Replacement: "/new/$1",
			},
		},
		direct: true, // rewriter must be compiled on first request
	})
	rec := doRequest(th, "GET", "http://example.com/old/z")
	if state.requests[0].Path != "/new/z" {
		t.Errorf("upstream path=%q", state.requests[0].Path)
	}
	if rec.Body.String() != "UPSTREAM:/new/z" {
		t.Errorf("body=%q", rec.Body.String())
	}
}

func TestServeHTTP_Redirect(t *testing.T) {
	t.Run("redirect=302", func(t *testing.T) {
		upURL, _ := newUpstream(t)
		th, _ := setupHandler(t, fixture{
			upstreamURL: upURL,
			location: &config.LocationConfig{
				Path: "/",
				Rewrite: &config.RewriteConfig{
					Pattern:     "^/old/(.*)$",
					Replacement: "/new/$1",
					Redirect:    "redirect",
				},
			},
		})
		rec := doRequest(th, "GET", "http://example.com/old/abc?q=1")
		if rec.Code != http.StatusFound {
			t.Errorf("code=%d, want 302", rec.Code)
		}
		if got := rec.Header().Get("Location"); got != "/new/abc?q=1" {
			t.Errorf("location=%q", got)
		}
	})

	t.Run("redirect=permanent", func(t *testing.T) {
		upURL, _ := newUpstream(t)
		th, _ := setupHandler(t, fixture{
			upstreamURL: upURL,
			location: &config.LocationConfig{
				Path: "/",
				Rewrite: &config.RewriteConfig{
					Pattern:     "^/old/(.*)$",
					Replacement: "/new/$1",
					Redirect:    "permanent",
				},
			},
		})
		rec := doRequest(th, "GET", "http://example.com/old/abc?q=1")
		if rec.Code != http.StatusMovedPermanently {
			t.Errorf("code=%d, want 301", rec.Code)
		}
	})

	t.Run("no rewriter", func(t *testing.T) {
		upURL, _ := newUpstream(t)
		th, _ := setupHandler(t, fixture{
			upstreamURL: upURL,
			location: &config.LocationConfig{
				Path: "/",
				Rewrite: &config.RewriteConfig{
					Redirect: "redirect",
				},
			},
			direct: true,
		})
		rec := doRequest(th, "GET", "http://example.com/same?q=1")
		if rec.Code != http.StatusFound {
			t.Errorf("code=%d", rec.Code)
		}
		if got := rec.Header().Get("Location"); got != "/same?q=1" {
			t.Errorf("location=%q", got)
		}
	})
}

// --- websocket ------------------------------------------------------------

func TestServeHTTP_WebSocket(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Respond 101 then immediately close the upgraded connection so
		// the proxy's body reader sees EOF.
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		fmt.Fprint(buf, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		buf.Flush()
		conn.Close()
	}))
	t.Cleanup(srv.Close)

	stub := mkStub()
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, _ := setupHandler(t, fixture{
		upstreamURL: srv.URL,
		location: &config.LocationConfig{
			Path:  "/",
			Proxy: &config.ProxyLocationConfig{WebSocket: true},
		},
		pluginMgr: pm,
	})

	req := httptest.NewRequest("GET", "http://example.com/ws", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	rec := httptest.NewRecorder()
	th.ServeHTTP(rec, req)

	// Recorder can't hijack, so the 101 upgrade fails -> 502 via handler.
	if rec.Code != http.StatusBadGateway {
		t.Errorf("code=%d, want 502", rec.Code)
	}
	if len(stub.logged) == 0 {
		t.Error("expected a logged proxy error for failed websocket upgrade")
	}
}

// --- compression wrap -----------------------------------------------------

func TestServeHTTP_CompressionWrap(t *testing.T) {
	upURL, _ := newUpstream(t)
	wrap := &wrapPlugin{BasePlugin: base.NewBasePlugin("compression")}
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(wrap)

	th, _ := setupHandler(t, fixture{upstreamURL: upURL, pluginMgr: pm})
	rec := doRequest(th, "GET", "http://example.com/c")

	if rec.Header().Get("X-Wrapped") != "yes" {
		t.Error("handler was not wrapped")
	}
	if wrap.calls != 1 {
		t.Errorf("wraps=%d", wrap.calls)
	}
	if rec.Body.String() != "UPSTREAM:/c" {
		t.Errorf("body=%q", rec.Body.String())
	}
}

func TestServeHTTP_CompressionNoWrapper(t *testing.T) {
	upURL, _ := newUpstream(t)
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(&plainPlugin{BasePlugin: base.NewBasePlugin("compression")})

	th, _ := setupHandler(t, fixture{upstreamURL: upURL, pluginMgr: pm})
	rec := doRequest(th, "GET", "http://example.com/c")
	if rec.Header().Get("X-Wrapped") != "" {
		t.Error("unexpected wrap")
	}
	if rec.Body.String() != "UPSTREAM:/c" {
		t.Errorf("body=%q", rec.Body.String())
	}
}

func TestServeHTTP_LoggerNotFound(t *testing.T) {
	upURL, _ := newUpstream(t)
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(&plainPlugin{BasePlugin: base.NewBasePlugin("other")})

	th, _ := setupHandler(t, fixture{upstreamURL: upURL, pluginMgr: pm})
	// No requestLogger plugin: request is served without wrapping.
	if rec := doRequest(th, "GET", "http://example.com/x"); rec.Body.String() != "UPSTREAM:/x" {
		t.Errorf("body=%q", rec.Body.String())
	}
}

// --- helpers / misc -------------------------------------------------------

func TestCapturingTransport_SetsContentLengthForChunkedUpstream(t *testing.T) {
	// The upstream streams a chunked (unknown-length) response. Because the
	// capturing transport buffers the whole body, it must report the real
	// ContentLength; otherwise httputil.ReverseProxy treats the response as
	// unbounded and flushes after every write, which can emit response
	// headers before the compression plugin sets Content-Encoding.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte(strings.Repeat("<div>line</div>\n", 100)))
	}))
	defer upstream.Close()

	lb := loadbalancer.New("", []*loadbalancer.Upstream{{URL: upstream.URL, Weight: 1}})
	ct := &capturingTransport{transport: http.DefaultTransport, lb: lb}
	req, err := http.NewRequest("GET", "http://example.com/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ct.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(ct.body) == 0 {
		t.Fatal("expected a buffered body")
	}
	if resp.ContentLength != int64(len(ct.body)) {
		t.Errorf("ContentLength=%d, want %d (fully buffered upstream body)", resp.ContentLength, len(ct.body))
	}

	// Buffered body must be readable back from the replaced response body.
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, ct.body) {
		t.Error("response body does not match buffered body")
	}
}

func TestGetUpstreamURL(t *testing.T) {
	t.Run("proxy override", func(t *testing.T) {
		th, _ := setupHandler(t, fixture{
			location: &config.LocationConfig{
				Path:  "/",
				Proxy: &config.ProxyLocationConfig{Upstream: "http://override:8080"},
			},
		})
		if got := th.getUpstreamURL(httptest.NewRequest("GET", "/", nil)); got != "http://override:8080" {
			t.Errorf("got=%q", got)
		}
	})

	t.Run("from target", func(t *testing.T) {
		upURL, _ := newUpstream(t)
		th, _ := setupHandler(t, fixture{upstreamURL: upURL})
		if got := th.getUpstreamURL(httptest.NewRequest("GET", "/", nil)); got != upURL {
			t.Errorf("got=%q", got)
		}
	})

	t.Run("empty", func(t *testing.T) {
		th, _ := setupHandler(t, fixture{})
		if got := th.getUpstreamURL(httptest.NewRequest("GET", "/", nil)); got != "" {
			t.Errorf("got=%q", got)
		}
	})
}

func TestLogError(t *testing.T) {
	t.Run("nil manager", func(t *testing.T) {
		th, _ := setupHandler(t, fixture{})
		th.logError("ERROR", httptest.NewRequest("GET", "/", nil), "boom", errors.New("cause")) // no panic
	})

	pm := manager.NewPluginManager()
	stub := mkStub()
	pm.RegisterPlugin(stub)
	th, _ := setupHandler(t, fixture{pluginMgr: pm})

	t.Run("with error", func(t *testing.T) {
		th.logError("ERROR", httptest.NewRequest("GET", "/x", nil), "boom", errors.New("cause"))
		if len(stub.logged) == 0 || !strings.Contains(stub.logged[0], "boom: cause") {
			t.Errorf("logged=%v", stub.logged)
		}
	})

	t.Run("without error", func(t *testing.T) {
		th.logError("WARN", httptest.NewRequest("GET", "/y", nil), "plain", nil)
		if len(stub.logged) < 2 || stub.logged[1] != "WARN: plain" {
			t.Errorf("logged=%v", stub.logged)
		}
	})

	t.Run("nil request", func(t *testing.T) {
		th.logError("WARN", nil, "boom", nil)
		if len(stub.logged) < 3 || !strings.HasSuffix(stub.logged[2], "boom") {
			t.Errorf("logged=%v", stub.logged)
		}
	})
}

func TestTargetHandler_Location(t *testing.T) {
	loc := &config.LocationConfig{Path: "/x"}
	th, _ := setupHandler(t, fixture{location: loc})
	if th.Location() != loc {
		t.Error("Location mismatch")
	}
}

func TestNewTargetHandler_RewriteCompile(t *testing.T) {
	th, _ := setupHandler(t, fixture{
		location: &config.LocationConfig{Path: "/", Rewrite: &config.RewriteConfig{Pattern: "^x"}},
	})
	if th.rewriter == nil {
		t.Error("rewriter should be compiled")
	}

	th2, _ := setupHandler(t, fixture{
		location: &config.LocationConfig{Path: "/", Rewrite: &config.RewriteConfig{}},
	})
	if th2.rewriter != nil {
		t.Error("rewriter should stay nil")
	}
}

// --- gRPC -----------------------------------------------------------------

func grpcReq(method, target string) *http.Request {
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("Te", "trailers")
	return req
}

// newH2CUpstream starts a plaintext HTTP/2 (h2c) backend — the transport
// real gRPC servers speak — and returns its base URL.
func newH2CUpstream(t *testing.T, h http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("h2c listen: %v", err)
	}
	protoSet := new(http.Protocols)
	protoSet.SetHTTP1(true)
	protoSet.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: h, Protocols: protoSet}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "http://" + ln.Addr().String()
}

func TestGRPCStreamPassthrough(t *testing.T) {
	// Upstream simulating a gRPC server: streams two frames and then sends
	// the grpc-status trailer.
	var sawTeTrailers bool
	up := newH2CUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawTeTrailers = strings.Contains(r.Header.Get("Te"), "trailers")
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Trailer", "Grpc-Status")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("frame1"))
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte("frame2"))
		w.Header().Set("Grpc-Status", "0")
	}))

	th, _ := setupHandler(t, fixture{upstreamURL: up, location: &config.LocationConfig{Path: "/"}})
	rec := httptest.NewRecorder()
	th.ServeHTTP(rec, grpcReq("POST", "http://example.com/test.Greeter/SayHello"))

	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}
	if got := rec.Body.String(); got != "frame1frame2" {
		t.Errorf("body=%q", got)
	}
	if !sawTeTrailers {
		t.Error("upstream did not receive te: trailers")
	}
	if got := rec.Header().Get("Grpc-Status"); got != "0" {
		t.Errorf("grpc-status trailer=%q (header map %v)", got, rec.Header())
	}
}

func TestGRPCStreamingNotCached(t *testing.T) {
	var hits int
	var mu sync.Mutex
	up := newH2CUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(200)
	}))
	th, dc := setupHandler(t, fixture{
		upstreamURL: up,
		location:    &config.LocationConfig{Path: "/", Cache: true},
	})

	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		th.ServeHTTP(rec, grpcReq("POST", "http://example.com/pkg.Svc/Foo"))
		if rec.Code != 200 {
			t.Fatalf("code=%d", rec.Code)
		}
	}

	mu.Lock()
	got := hits
	mu.Unlock()
	if got != 2 {
		t.Errorf("upstream hits=%d (gRPC requests must not be cached)", got)
	}
	files, err := filepath.Glob(filepath.Join(dc.CacheDir, "*", "*.meta"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Errorf("gRPC response was cached to disk: %v", files)
	}
}

func TestServeHTTP_GRPCNoUpstream(t *testing.T) {
	th, _ := setupHandler(t, fixture{}) // no proxy.upstream, target has no upstreams
	rec := httptest.NewRecorder()
	th.ServeHTTP(rec, grpcReq("POST", "http://example.com/pkg.Svc/Foo"))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("code=%d want 502", rec.Code)
	}
}

func TestServeHTTP_GRPCUpstreamError(t *testing.T) {
	// Upstream that refuses connections: the gRPC stream fails through the
	// proxy ErrorHandler with 502.
	th, _ := setupHandler(t, fixture{upstreamURL: "http://127.0.0.1:1"}) // closed port
	rec := httptest.NewRecorder()
	th.ServeHTTP(rec, grpcReq("POST", "http://example.com/pkg.Svc/Foo"))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("code=%d want 502", rec.Code)
	}
}

func TestServeHTTP_GRPCProxyPassHostHeader(t *testing.T) {
	up := newH2CUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "host:%s", r.Host)
	}))
	th, _ := setupHandler(t, fixture{
		upstreamURL: up,
		location:    &config.LocationConfig{Path: "/", Proxy: &config.ProxyLocationConfig{PassHostHeader: true}},
	})
	req := httptest.NewRequest("POST", "http://original.example/pkg.Svc/Foo", nil)
	req.Header.Set("Content-Type", "application/grpc")
	rec := httptest.NewRecorder()
	th.ServeHTTP(rec, req)
	if rec.Code != 200 || rec.Body.String() != "host:original.example" {
		t.Fatalf("code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestServeHTTP_GRPCTargetHost(t *testing.T) {
	up := newH2CUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "host:%s", r.Host)
	}))
	th, _ := setupHandler(t, fixture{
		upstreamURL: up,
		targetHost:  "override.example.com",
		location:    &config.LocationConfig{Path: "/", Proxy: &config.ProxyLocationConfig{PassHostHeader: true}},
	})
	rec := httptest.NewRecorder()
	th.ServeHTTP(rec, grpcReq("POST", "http://original.example/pkg.Svc/Foo"))
	if rec.Code != 200 || rec.Body.String() != "host:override.example.com" {
		t.Fatalf("code=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestGRPCTransport_RoundTrip(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://127.0.0.1:1"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	gt := &grpcTransport{lb: lb}

	// Transport failure: health marked unhealthy, error returned.
	req, _ := http.NewRequest("POST", "http://127.0.0.1:1/pkg.Svc/Foo", nil)
	if _, err := gt.RoundTrip(req); err == nil {
		t.Fatal("expected error while upstream is down")
	}
	if up.Healthy.Load() {
		t.Fatal("upstream should be unhealthy after failure")
	}

	// Recovery over a real h2c backend: the body is streamed untouched and
	// the upstream is marked healthy again. With LB-authoritative routing the
	// transport re-points the request at the selected upstream, so the live
	// LB entry must be the h2c backend.
	echo := newH2CUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("stream"))
	}))
	upLive := &loadbalancer.Upstream{URL: echo}
	lbLive := loadbalancer.New("", []*loadbalancer.Upstream{upLive})
	gtLive := &grpcTransport{lb: lbLive}
	req2, _ := http.NewRequest("POST", "http://placeholder.invalid/pkg.Svc/Foo", nil)
	resp, err := gtLive.RoundTrip(req2)
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if !upLive.Healthy.Load() {
		t.Fatal("upstream should be healthy after success")
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "stream" {
		t.Errorf("body=%q", string(body))
	}
}

func TestGRPCTransport_HTTPSBackend(t *testing.T) {
	// TLS upstreams are handled by http.DefaultTransport (h2 via ALPN).
	up := &loadbalancer.Upstream{URL: "https://127.0.0.1:1"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	gt := &grpcTransport{lb: lb}
	req, _ := http.NewRequest("POST", "https://127.0.0.1:1/pkg.Svc/Foo", nil)
	if _, err := gt.RoundTrip(req); err == nil {
		t.Fatal("expected error for unreachable TLS backend")
	}
	if up.Healthy.Load() {
		t.Fatal("upstream should be unhealthy after failure")
	}
}

func TestGRPCTransport_NoUpstreams(t *testing.T) {
	gt := &grpcTransport{lb: loadbalancer.New("", nil)}
	if _, err := gt.RoundTrip(httptest.NewRequest("POST", "http://example.com/x", nil)); err == nil {
		t.Error("expected error with no healthy upstreams")
	}
}

// --- capturingTransport ---------------------------------------------------

func TestCapturingTransport_NoUpstreams(t *testing.T) {
	ct := &capturingTransport{transport: http.DefaultTransport, lb: loadbalancer.New("", nil)}
	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Error("expected error with no healthy upstreams")
	}
}

func TestCapturingTransport_UpstreamRecoversAfterDown(t *testing.T) {
	// Regression test for the reported bug: an upstream that failed once was
	// permanently excluded (Next returned nil forever), so requests kept
	// returning 502 until the proxy was restarted even after the upstream
	// came back.
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	rt := &toggleRT{}
	ct := &capturingTransport{transport: rt, lb: lb}

	// Upstream down: first request fails and marks it unhealthy.
	rt.err = errors.New("connection refused")
	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Fatal("expected error while upstream is down")
	}
	if up.Healthy.Load() {
		t.Fatal("upstream should be unhealthy after failure")
	}

	// Upstream comes back: the next request must probe it (fail-back in the
	// load balancer) and restore its health — no restart required.
	rt.err = nil
	rt.resp = &http.Response{
		StatusCode: 200,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("ok"))),
	}
	resp, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil))
	if err != nil {
		t.Fatalf("request should succeed after upstream recovers: %v", err)
	}
	resp.Body.Close()
	if !up.Healthy.Load() {
		t.Fatal("upstream should be healthy again after successful probe")
	}
}

func TestCapturingTransport_TransportError(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	ct := &capturingTransport{transport: &fakeRT{err: errors.New("conn refused")}, lb: lb}

	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Fatal("expected transport error")
	}
	if up.Healthy.Load() {
		t.Error("upstream should be marked unhealthy after transport error")
	}
}

func TestCapturingTransport_BodyReadError(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	ct := &capturingTransport{
		transport: &fakeRT{resp: &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       io.NopCloser(errBody{errors.New("read failed")}),
		}},
		lb: lb,
	}
	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Fatal("expected body read error")
	}
	if up.Healthy.Load() {
		t.Error("upstream should be marked unhealthy after body read failure")
	}
}

func TestCapturingTransport_OversizedBody(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	ct := &capturingTransport{
		transport: &fakeRT{resp: &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       io.NopCloser(io.LimitReader(zeroReader{}, maxBodyReadSize+1)),
		}},
		lb: lb,
	}
	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Fatal("expected oversized body error")
	}
	if up.Healthy.Load() {
		t.Error("upstream should be marked unhealthy after oversized response")
	}
}

func TestCapturingTransport_ExplicitLimit(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	ct := &capturingTransport{
		transport: &fakeRT{resp: &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("0123456789")),
		}},
		lb:          lb,
		maxBodySize: 4,
	}
	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Fatal("expected oversized body error for explicit limit")
	}
	if up.Healthy.Load() {
		t.Error("upstream should be marked unhealthy after oversized response")
	}
}

func TestCapturingTransport_Unlimited(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	const payload = "0123456789"
	ct := &capturingTransport{
		transport: &fakeRT{resp: &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(payload)),
		}},
		lb:          lb,
		maxBodySize: -1,
	}
	resp, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	if !up.Healthy.Load() {
		t.Error("upstream should remain healthy for unlimited body")
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != payload {
		t.Errorf("body=%q", got)
	}
}

func TestCapturingTransport_Success(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	ct := &capturingTransport{
		transport: &fakeRT{resp: &http.Response{
			StatusCode: 201,
			Header:     http.Header{"Content-Type": []string{"text/plain"}},
			Body:       io.NopCloser(strings.NewReader("hello upstream")),
		}},
		lb: lb,
	}

	resp, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	if ct.statusCode != 201 {
		t.Errorf("statusCode=%d", ct.statusCode)
	}
	if ct.headers.Get("Content-Type") != "text/plain" {
		t.Errorf("headers=%v", ct.headers)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "hello upstream" {
		t.Errorf("body=%q", got)
	}
	if string(ct.body) != "hello upstream" {
		t.Errorf("captured=%q", ct.body)
	}
	if ct.selectedUpstream != "http://a.example" {
		t.Errorf("selected=%q", ct.selectedUpstream)
	}
	if !up.Healthy.Load() {
		t.Error("upstream should be healthy after success")
	}
}

// --- test doubles ---------------------------------------------------------

type fakeRT struct {
	resp *http.Response
	err  error
}

func (f *fakeRT) RoundTrip(*http.Request) (*http.Response, error) {
	return f.resp, f.err
}

type toggleRT struct {
	resp *http.Response
	err  error
}

func (t *toggleRT) RoundTrip(*http.Request) (*http.Response, error) {
	return t.resp, t.err
}

type errBody struct{ err error }

func (e errBody) Read([]byte) (int, error) { return 0, e.err }

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
