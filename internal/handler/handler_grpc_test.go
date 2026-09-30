package handler

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/loadbalancer"
)

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
