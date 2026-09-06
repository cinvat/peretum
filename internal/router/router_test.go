package router

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/handler"
	"github.com/cinvat/peretum/internal/loadbalancer"
)

func newEchoServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
}

func testTargetHandler(t *testing.T, ts *httptest.Server, name string, loc *config.LocationConfig) *handler.TargetHandler {
	t.Helper()
	lb := loadbalancer.NewRoundRobin([]*loadbalancer.Upstream{{URL: ts.URL}})
	return handler.NewTargetHandler(
		&config.TargetConfig{
			Name:      name,
			Upstreams: []config.UpstreamConfig{{URL: ts.URL}},
		},
		loc,
		nil, nil, lb, nil, 0,
	)
}

func TestNewHostRouter(t *testing.T) {
	hr := NewHostRouter()
	if hr.targets == nil {
		t.Fatal("expected initialized targets map")
	}
	if got := hr.GetTargets(); got == nil || len(got) != 0 {
		t.Fatalf("GetTargets = %v", got)
	}
	if got := hr.GetDefaultHandler(); got != nil {
		t.Fatalf("GetDefaultHandler = %v", got)
	}
}

func TestTargetConfigHandlerServeHTTP(t *testing.T) {
	tsExact := newEchoServer(t, "exact-body")
	tsRegex := newEchoServer(t, "regex-body")
	tsPrefix := newEchoServer(t, "prefix-body")
	tsDefault := newEchoServer(t, "default-body")
	defer tsExact.Close()
	defer tsRegex.Close()
	defer tsPrefix.Close()
	defer tsDefault.Close()

	hExact := testTargetHandler(t, tsExact, "exact", &config.LocationConfig{Path: "/a", MatchType: config.MatchExact})
	hRegex := testTargetHandler(t, tsRegex, "regex", &config.LocationConfig{Path: "^/r/", MatchType: config.MatchRegex})
	hPrefix := testTargetHandler(t, tsPrefix, "prefix", &config.LocationConfig{Path: "/p/", MatchType: config.MatchPrefix})
	hDefault := testTargetHandler(t, tsDefault, "default", &config.LocationConfig{Path: "/d", MatchType: config.MatchExact})

	tch := &TargetConfigHandler{Handlers: []*handler.TargetHandler{hPrefix, hExact, hRegex}}

	assertBody(t, tch, "/a", "exact-body")     // exact priority 1000 beats prefix 2
	assertBody(t, tch, "/r/xyz", "regex-body") // regex priority 500
	assertBody(t, tch, "/p/x", "prefix-body")  // prefix priority 2 (only match)
	assertBody(t, tch, "/nomatch", "No matching location\n")

	// DefaultLoc used when nothing matches.
	tchDefault := &TargetConfigHandler{
		Handlers:   []*handler.TargetHandler{hExact},
		DefaultLoc: hDefault,
	}
	assertBody(t, tchDefault, "/zzz", "default-body")

	// No handlers, no default -> 404.
	assertBody(t, &TargetConfigHandler{}, "/anything", "No matching location\n")
}

func assertBody(t *testing.T, tch *TargetConfigHandler, path, want string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://example.com"+path, nil)
	rec := httptest.NewRecorder()
	tch.ServeHTTP(rec, req)
	if rec.Body.String() != want {
		t.Fatalf("path %s: body = %q, want %q", path, rec.Body.String(), want)
	}
}

func TestHostRouterServeHTTP(t *testing.T) {
	tsExact := newEchoServer(t, "exact-body")
	tsDefaultH := newEchoServer(t, "default-body")
	defer tsExact.Close()
	defer tsDefaultH.Close()

	loc := &config.LocationConfig{Path: "/a", MatchType: config.MatchExact}
	hExact := testTargetHandler(t, tsExact, "exact", loc)
	hDefault := testTargetHandler(t, tsDefaultH, "default", &config.LocationConfig{Path: "/", MatchType: config.MatchExact})

	hr := NewHostRouter()
	tch := &TargetConfigHandler{Handlers: []*handler.TargetHandler{hExact}}
	hr.Reload(map[string]*TargetConfigHandler{
		"svc-a":    tch,
		"_default": {Handlers: []*handler.TargetHandler{hDefault}},
	}, hDefault)

	// Exact host.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://svc-a/a", nil)
	hr.ServeHTTP(rec, req)
	if rec.Body.String() != "exact-body" {
		t.Fatalf("exact host body = %q", rec.Body.String())
	}

	// Host with port stripped -> still matches svc-a.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "http://svc-a:8080/a", nil)
	hr.ServeHTTP(rec2, req2)
	if rec2.Body.String() != "exact-body" {
		t.Fatalf("port host body = %q", rec2.Body.String())
	}

	// Unknown host falls back to _default target.
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodGet, "http://unknown.svc/", nil)
	hr.ServeHTTP(rec3, req3)
	if rec3.Body.String() != "default-body" {
		t.Fatalf("_default body = %q", rec3.Body.String())
	}

	// Unknown host, no _default target but a default handler.
	hr2 := NewHostRouter()
	hr2.Reload(map[string]*TargetConfigHandler{}, hDefault)
	rec4 := httptest.NewRecorder()
	req4 := httptest.NewRequest(http.MethodGet, "http://unknown.svc/", nil)
	hr2.ServeHTTP(rec4, req4)
	if rec4.Body.String() != "default-body" {
		t.Fatalf("default handler body = %q", rec4.Body.String())
	}

	// Unknown host with neither _default nor default handler -> 404.
	hr3 := NewHostRouter()
	rec5 := httptest.NewRecorder()
	req5 := httptest.NewRequest(http.MethodGet, "http://unknown.svc/", nil)
	hr3.ServeHTTP(rec5, req5)
	if rec5.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec5.Code)
	}
}

func TestReload(t *testing.T) {
	ts := newEchoServer(t, "x")
	defer ts.Close()
	h := testTargetHandler(t, ts, "x", &config.LocationConfig{Path: "/", MatchType: config.MatchPrefix})
	def := testTargetHandler(t, ts, "def", &config.LocationConfig{Path: "/", MatchType: config.MatchPrefix})

	hr := NewHostRouter()
	targets := map[string]*TargetConfigHandler{"x": {Handlers: []*handler.TargetHandler{h}}}
	hr.Reload(targets, def)

	if got := hr.GetTargets(); len(got) != 1 || got["x"] == nil {
		t.Fatalf("GetTargets = %v", got)
	}
	if got := hr.GetDefaultHandler(); got != def {
		t.Fatalf("GetDefaultHandler = %v", got)
	}
}
