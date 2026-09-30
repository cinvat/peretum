package handler

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
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

	target := &config.TargetConfig{ServerName: f.targetName}
	if f.targetHost != "" {
		target.HostHeader = f.targetHost
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
