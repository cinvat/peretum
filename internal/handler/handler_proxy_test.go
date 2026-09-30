package handler

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	disk "github.com/cinvat/peretum/internal/cache/disk"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/plugin/manager"
	"github.com/cinvat/peretum/plugins/base"
)

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
