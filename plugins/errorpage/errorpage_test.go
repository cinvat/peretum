package errorpage

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cinvat/peretum/plugins/base"
)

func TestPluginName(t *testing.T) {
	p := NewErrorPagePlugin()
	if p.Name() != "error_page" {
		t.Fatalf("Name = %q, want error_page", p.Name())
	}
}

func TestInitDefaults(t *testing.T) {
	p := NewErrorPagePlugin()
	if err := p.Init(nil); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	if p.enabled {
		t.Fatal("enabled should default to false")
	}
	if len(p.statuses) != len(defaultStatuses) {
		t.Fatalf("statuses = %v, want defaults %v", p.statuses, defaultStatuses)
	}
	for _, s := range defaultStatuses {
		if !p.statuses[s] {
			t.Fatalf("missing default status %d", s)
		}
	}
}

func TestInitCustomStatuses(t *testing.T) {
	p := NewErrorPagePlugin()
	cfg := map[string]any{
		"enabled":  true,
		"statuses": []int{403, 410},
	}
	if err := p.Init(cfg); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	if !p.enabled {
		t.Fatal("enabled should be true")
	}
	if len(p.statuses) != 2 || !p.statuses[403] || !p.statuses[410] {
		t.Fatalf("statuses = %v, want {403:true 410:true}", p.statuses)
	}
	if p.statuses[404] {
		t.Fatal("404 should not be in custom statuses")
	}
}

func TestInitStatusesFromAny(t *testing.T) {
	p := NewErrorPagePlugin()
	cfg := map[string]any{"enabled": true, "statuses": []any{float64(502), int64(503), int(504), "bad", 500}}
	if err := p.Init(cfg); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	for _, s := range []int{500, 502, 503, 504} {
		if !p.statuses[s] {
			t.Fatalf("missing status %d in %v", s, p.statuses)
		}
	}
	if len(p.statuses) != 4 {
		t.Fatalf("statuses = %v, want 4 entries", p.statuses)
	}
}

func TestInitStatusesDefaultsForBadTypes(t *testing.T) {
	p := NewErrorPagePlugin()
	cfg := map[string]any{"enabled": true, "statuses": "not-a-slice"}
	if err := p.Init(cfg); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	if len(p.statuses) != len(defaultStatuses) {
		t.Fatalf("statuses = %v, want defaults", p.statuses)
	}
}

func TestLastInitWinsForStatuses(t *testing.T) {
	p := NewErrorPagePlugin()
	if err := p.Init(map[string]any{"enabled": true, "statuses": []int{404}}); err != nil {
		t.Fatalf("Init error: %v", err)
	}
	if err := p.Init(nil); err != nil {
		t.Fatalf("second Init error: %v", err)
	}
	// Second Init must reset the statuses map to defaults, not merge.
	if len(p.statuses) != len(defaultStatuses) {
		t.Fatalf("statuses after re-Init = %v, want defaults", p.statuses)
	}
}

func TestWrapRouterDisabled(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": false})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("plain 404"))
	})
	h := p.WrapRouter(inner)
	if h == nil {
		t.Fatal("WrapRouter returned nil")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if rec.Body.String() != "plain 404" {
		t.Fatalf("disabled wrapper must pass through, got %q", rec.Body.String())
	}
}

func TestWrapRouterErrorPage(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	p.hostname = "edge-01"
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("plain 404"))
	})
	h := p.WrapRouter(inner)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/<script>alert(1)</script>", nil)
	req.RemoteAddr = "10.0.0.1:1234"
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "404") || !strings.Contains(body, "Not Found") {
		t.Fatalf("page missing status: %s", body)
	}
	if !strings.Contains(body, "Request ID") {
		t.Fatalf("page missing request ID: %s", body)
	}
	if !strings.Contains(body, "edge-01") {
		t.Fatalf("page missing hostname: %s", body)
	}
	if !strings.Contains(body, "10.0.0.1") {
		t.Fatalf("page missing client IP: %s", body)
	}
	if !strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt;") {
		t.Fatalf("request path not escaped: %s", body)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("Content-Type = %q, want text/html", ct)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
	}
	if !strings.Contains(rec.Header().Get("X-Request-ID"), "Request") {
		// X-Request-ID is a hex id, confirm it is non-empty
		if rec.Header().Get("X-Request-ID") == "" {
			t.Fatal("X-Request-ID header missing")
		}
	}
}

func TestWrapRouterHeaderOnly(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	h := p.WrapRouter(inner)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Service Unavailable") {
		t.Fatalf("expected rendered page, got %q", rec.Body.String())
	}
}

func TestWrapRouterHeadNoBody(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGatewayTimeout)
	})
	h := p.WrapRouter(inner)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodHead, "/", nil))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD must not write a body, got %d bytes", rec.Body.Len())
	}
}

func TestWrapRouterPassthrough(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Custom", "yes")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	})
	h := p.WrapRouter(inner)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ok", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "hello" {
		t.Fatalf("body = %q, want hello", rec.Body.String())
	}
	if rec.Header().Get("X-Custom") != "yes" {
		t.Fatal("passthrough must preserve headers")
	}
}

func TestWrapRouterNonInterceptStatus(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true, "statuses": []int{404}})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("raw 502"))
	})
	h := p.WrapRouter(inner)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if rec.Body.String() != "raw 502" {
		t.Fatalf("502 not in status set, must pass through, got %q", rec.Body.String())
	}
}

func TestWrapRouterExistingRequestID(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	h := p.WrapRouter(inner)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Request-ID", "fixed-id")
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got != "fixed-id" {
		t.Fatalf("X-Request-ID = %q, want fixed-id", got)
	}
	if !strings.Contains(rec.Body.String(), "fixed-id") {
		t.Fatalf("page must show the client-provided request ID")
	}
}

func TestEnsureRequestIDAlreadySet(t *testing.T) {
	p := NewErrorPagePlugin()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-Request-ID", "context-id")
	withCtx, _ := base.EnsureRequestID(r)
	got := p.ensureRequestID(withCtx)
	if base.RequestID(got) != "context-id" {
		t.Fatalf("request ID = %q, want context-id", base.RequestID(got))
	}
}

func TestRequestIDHeaderFallback(t *testing.T) {
	p := NewErrorPagePlugin()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("X-Request-ID", "hdr-id")
	if got := p.requestID(r); got != "hdr-id" {
		t.Fatalf("requestID = %q, want hdr-id", got)
	}
}

func TestRequestIDNone(t *testing.T) {
	p := NewErrorPagePlugin()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if got := p.requestID(r); got != "-" {
		t.Fatalf("requestID = %q, want -", got)
	}
}

func TestWriterFlush(t *testing.T) {
	inner := httptest.NewRecorder()
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	ew := &errorPageWriter{ResponseWriter: inner, plugin: p}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ew.request = req
	ew.WriteHeader(http.StatusNotFound)
	if !ew.render {
		t.Fatal("WriteHeader must set render for 404")
	}
	if !ew.wroteHeader {
		t.Fatal("WriteHeader must record that a header was written")
	}
	// The upstream header must not be forwarded to the underlying writer.
	if inner.Code != http.StatusOK {
		t.Fatalf("underlying status = %d, want default 200", inner.Code)
	}
	ew.Flush()
	if inner.Code != http.StatusNotFound {
		t.Fatalf("underlying status after Flush = %d, want 404", inner.Code)
	}
	if !strings.Contains(inner.Body.String(), "Not Found") {
		t.Fatalf("Flush must render the page, got %q", inner.Body.String())
	}
	// Second flush must not produce duplicate output.
	before := inner.Body.Len()
	ew.Flush()
	if inner.Body.Len() != before {
		t.Fatal("Flush after render must be a no-op")
	}
}

func TestWriterNonInterceptFlush(t *testing.T) {
	inner := httptest.NewRecorder()
	inner.Header().Set("Content-Type", "text/plain")
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	ew := &errorPageWriter{ResponseWriter: inner, plugin: p}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ew.request = req
	ew.WriteHeader(http.StatusOK)
	ew.Flush()
	if inner.Code != http.StatusOK {
		t.Fatalf("underlying status = %d, want 200", inner.Code)
	}
}

func TestWriterHijackSupported(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(c1), bufio.NewWriter(c2))
	fake := &fakeHijacker{
		ResponseRecorder: httptest.NewRecorder(),
		conn:             c1,
		rw:               rw,
		err:              nil,
	}
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	ew := &errorPageWriter{ResponseWriter: fake, plugin: p}
	c, gotRW, err := ew.Hijack()
	if c != c1 {
		t.Fatal("Hijack must delegate the connection to the underlying writer")
	}
	if gotRW != rw {
		t.Fatal("Hijack must delegate the readwriter to the underlying writer")
	}
	if err != nil {
		t.Fatalf("Hijack error = %v, want nil", err)
	}
}

func TestWriterHijackUnsupported(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	ew := &errorPageWriter{ResponseWriter: httptest.NewRecorder(), plugin: p}
	if _, _, err := ew.Hijack(); err != http.ErrNotSupported {
		t.Fatalf("Hijack error = %v, want ErrNotSupported", err)
	}
}

func TestWriterUnwrap(t *testing.T) {
	inner := httptest.NewRecorder()
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	ew := &errorPageWriter{ResponseWriter: inner, plugin: p}
	if ew.Unwrap() != inner {
		t.Fatal("Unwrap must return the underlying ResponseWriter")
	}
}

func TestWriterWriteThenStatus(t *testing.T) {
	// A handler that writes a body before calling WriteHeader must be
	// treated as 200 and pass through untouched.
	inner := httptest.NewRecorder()
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	ew := &errorPageWriter{ResponseWriter: inner, plugin: p}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	ew.request = req
	n, err := ew.Write([]byte("raw"))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != 3 {
		t.Fatalf("Write n = %d, want 3", n)
	}
	if inner.Body.String() != "raw" {
		t.Fatalf("underlying body = %q, want raw", inner.Body.String())
	}
	ew.finish()
}

func TestFinishWithoutWriteHeader(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	ew := &errorPageWriter{ResponseWriter: httptest.NewRecorder(), plugin: p}
	ew.finish()
	if ew.wroteHeader {
		t.Fatal("finish must not write a header when none was set")
	}
}

func TestWriteMultiple(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("first"))
		w.Write([]byte("second"))
	})
	h := p.WrapRouter(inner)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	// The page is rendered exactly once and both writes after the first
	// render are swallowed.
	if strings.Count(rec.Body.String(), "<!DOCTYPE html>") != 1 {
		t.Fatalf("page rendered more than once: %q", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "firstsecond") {
		t.Fatal("upstream body leaked into error page")
	}
}

func TestDoubleWriteHeader(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.WriteHeader(http.StatusNotFound)
	})
	h := p.WrapRouter(inner)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("first WriteHeader must win, got %d", rec.Code)
	}
	if rec.Body.String() != "" {
		t.Fatalf("body = %q, want empty", rec.Body.String())
	}
}

func TestRenderPageNilRequest(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	page := p.renderPage(http.StatusBadGateway, nil)
	if !strings.Contains(string(page), "502") || !strings.Contains(string(page), "Bad Gateway") {
		t.Fatalf("page missing status: %s", page)
	}
}

func TestRenderPageUnknownStatus(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	page := p.renderPage(599, req)
	if !strings.Contains(string(page), "599") {
		t.Fatalf("page missing code 599: %s", page)
	}
	if !strings.Contains(string(page), "Error") {
		t.Fatalf("page missing fallback status text: %s", page)
	}
}

func TestRenderPageStringEscaping(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	req := httptest.NewRequest(http.MethodGet, "/pa<th>", nil)
	req.RemoteAddr = "203.0.113.5:31337"
	page := p.renderPage(http.StatusBadGateway, req)
	s := string(page)
	if strings.Contains(s, "/pa<th>") {
		t.Fatal("unescaped path in page")
	}
	if !strings.Contains(s, "/pa&lt;th&gt;") {
		t.Fatalf("path not escaped: %s", s)
	}
	if !strings.Contains(s, "203.0.113.5") {
		t.Fatalf("client IP missing: %s", s)
	}
}

func TestRenderHostOnly(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "192.168.1.1:8080"
	page := p.renderPage(http.StatusBadGateway, req)
	if !strings.Contains(string(page), "192.168.1.1") {
		t.Fatalf("hostonly failed to strip port: %s", page)
	}
}

func TestHostOnlyFallbacks(t *testing.T) {
	if got := hostOnly(""); got != "-" {
		t.Fatalf("hostOnly(\"\") = %q, want -", got)
	}
	if got := hostOnly("not-an-addr"); got != "not-an-addr" {
		t.Fatalf("hostOnly(no port) = %q", got)
	}
}

func TestMessageForStatus(t *testing.T) {
	for _, c := range []int{400, 403, 404, 500, 502, 503, 504} {
		if m := messageForStatus(c); m == "" {
			t.Fatalf("messageForStatus(%d) is empty", c)
		}
	}
	if m := messageForStatus(418); m != "An error occurred while processing your request." {
		t.Fatalf("fallback message = %q", m)
	}
}

func TestEscapingEdge(t *testing.T) {
	in := `a&b<c>"d'e`
	want := "a&amp;b&lt;c&gt;&quot;d&#39;e"
	if got := escape(in); got != want {
		t.Fatalf("escape(%q) = %q, want %q", in, got, want)
	}
}

func TestLogoEmbedded(t *testing.T) {
	if !strings.Contains(logoSVG, "<svg") {
		t.Fatal("embedded logo.svg does not look like an SVG")
	}
}

func TestStartStopNoop(t *testing.T) {
	p := NewErrorPagePlugin()
	_ = p.Init(map[string]any{"enabled": true})
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("Stop error: %v", err)
	}
}

type fakeHijacker struct {
	*httptest.ResponseRecorder
	conn net.Conn
	rw   *bufio.ReadWriter
	err  error
}

func (f *fakeHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return f.conn, f.rw, f.err
}
