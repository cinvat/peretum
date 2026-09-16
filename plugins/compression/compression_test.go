package compression

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func baseCfg() map[string]any {
	return map[string]any{
		"enabled":    true,
		"level":      6,
		"min_length": 100,
		"types":      []string{"text/custom", "application/octet-stream"},
	}
}

func newCompressionPlugin(t *testing.T, cfg map[string]any) *CompressionPlugin {
	t.Helper()
	p := NewCompressionPlugin()
	if err := p.Init(cfg); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	return p
}

func TestInit(t *testing.T) {
	p := newCompressionPlugin(t, baseCfg())
	if !p.config.Enabled {
		t.Error("Enabled not set")
	}
	if p.config.Level != 6 || p.config.MinLength != 100 {
		t.Errorf("bad config: %+v", p.config)
	}
	for _, ct := range []string{"text/html", "text/css", "application/json", "image/svg+xml"} {
		if !p.config.typeMap[ct] {
			t.Errorf("default type %q missing", ct)
		}
	}
	for _, ct := range []string{"text/custom", "application/octet-stream"} {
		if !p.config.typeMap[ct] {
			t.Errorf("custom type %q missing", ct)
		}
	}
	gz := p.pool.Get().(*gzip.Writer)
	if gz == nil {
		t.Error("pool returned nil writer")
	}
	p.pool.Put(gz)
}

func TestInitTrimsTypes(t *testing.T) {
	cfg := baseCfg()
	cfg["types"] = []string{"  TEXT/html ", "APPLICATION/javascript"}
	p := newCompressionPlugin(t, cfg)
	if !p.config.typeMap["text/html"] {
		t.Error("trimmed/lowered type missing")
	}
	if !p.config.typeMap["application/javascript"] {
		t.Error("lowered type missing")
	}
}

func TestStartStop(t *testing.T) {
	p := newCompressionPlugin(t, baseCfg())
	if err := p.Start(context.Background()); err != nil {
		t.Errorf("Start: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestBeforeProxy(t *testing.T) {
	p := newCompressionPlugin(t, map[string]any{
		"enabled":    false,
		"level":      6,
		"min_length": 100,
		"types":      []string{},
	})
	if err := p.BeforeProxy(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "t", "l"); err != nil {
		t.Errorf("disabled BeforeProxy: %v", err)
	}

	p = newCompressionPlugin(t, baseCfg())
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "br, deflate")
	if err := p.BeforeProxy(httptest.NewRecorder(), req, "t", "l"); err != nil {
		t.Errorf("BeforeProxy no-gzip: %v", err)
	}

	req.Header.Set("Accept-Encoding", "gzip, deflate")
	if err := p.BeforeProxy(httptest.NewRecorder(), req, "t", "l"); err != nil {
		t.Errorf("BeforeProxy gzip: %v", err)
	}
}

func TestAfterProxy(t *testing.T) {
	p := newCompressionPlugin(t, baseCfg())
	if err := p.AfterProxy(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "t", "l", &http.Response{StatusCode: 200}); err != nil {
		t.Errorf("AfterProxy: %v", err)
	}
}

func servingHandler(body []byte, ct string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", ct)
		_, _ = w.Write(body)
	})
}

func TestWrapHandlerDisabled(t *testing.T) {
	p := newCompressionPlugin(t, map[string]any{
		"enabled":    false,
		"level":      6,
		"min_length": 100,
		"types":      []string{},
	})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("plain"))
	})
	wrapped := p.WrapHandler(h)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)
	if rec.Header().Get("Content-Encoding") != "" || rec.Body.String() != "plain" {
		t.Error("disabled WrapHandler should pass through")
	}
}

func TestWrapHandlerNoGzip(t *testing.T) {
	p := newCompressionPlugin(t, baseCfg())
	body := []byte(strings.Repeat("hello", 200))
	inner := servingHandler(body, "text/html")
	h := p.WrapHandler(inner)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "br")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Content-Encoding") != "" {
		t.Error("body should not be gzip encoded")
	}
	if !bytes.Equal(rec.Body.Bytes(), body) {
		t.Error("body mismatch")
	}
}

func TestWrapHandlerCompresses(t *testing.T) {
	p := newCompressionPlugin(t, baseCfg())
	body := []byte(strings.Repeat("compressible body ", 20))
	inner := servingHandler(body, "text/html")
	h := p.WrapHandler(inner)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("expected gzip, got %q", rec.Header().Get("Content-Encoding"))
	}
	if rec.Header().Get("Vary") != "Accept-Encoding" {
		t.Error("missing Vary header")
	}
	if rec.Body.Len() >= len(body) {
		t.Error("compressed body not smaller")
	}
	zr, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	decoded, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gzip read: %v", err)
	}
	if !bytes.Equal(decoded, body) {
		t.Error("roundtrip mismatch")
	}
}

func TestWrapHandlerBelowMinLength(t *testing.T) {
	p := newCompressionPlugin(t, baseCfg())
	body := []byte("tiny body")
	inner := servingHandler(body, "text/html")
	h := p.WrapHandler(inner)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Content-Encoding") != "" {
		t.Error("small body should not be compressed")
	}
	if !bytes.Equal(rec.Body.Bytes(), body) {
		t.Error("raw body mismatch")
	}
}

func TestWrapHandlerUnsupportedType(t *testing.T) {
	p := newCompressionPlugin(t, baseCfg())
	body := []byte(strings.Repeat("x", 500))
	inner := servingHandler(body, "video/mp4")
	h := p.WrapHandler(inner)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Content-Encoding") != "" {
		t.Error("unsupported content type should not compress")
	}
	if !bytes.Equal(rec.Body.Bytes(), body) {
		t.Error("raw body mismatch")
	}
}

func TestWrapHandlerGzipCloseError(t *testing.T) {
	orig := closeGzipWriter
	defer func() { closeGzipWriter = orig }()
	closeGzipWriter = func(gz *gzip.Writer) error { return errors.New("boom") }

	p := newCompressionPlugin(t, baseCfg())
	inner := servingHandler([]byte(strings.Repeat("a", 120)), "text/html")
	h := p.WrapHandler(inner)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Error("expected gzip content encoding despite close error")
	}
}

func TestWrapHandlerAlreadyEncoded(t *testing.T) {
	// Upstream already negotiated gzip: the plugin must pass the encoded
	// body through untouched instead of double-compressing it.
	p := newCompressionPlugin(t, baseCfg())
	body := []byte(strings.Repeat("compressible body ", 20))
	var gzBuf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&gzBuf, 6)
	_, _ = zw.Write(body)
	_ = zw.Close()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(gzBuf.Bytes())
	})
	h := p.WrapHandler(inner)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", rec.Header().Get("Content-Encoding"))
	}
	zr, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	decoded, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gzip read: %v", err)
	}
	if !bytes.Equal(decoded, body) {
		t.Errorf("single-decompressed body mismatch: got %q", decoded)
	}
}

func TestShouldCompress(t *testing.T) {
	p := newCompressionPlugin(t, baseCfg())
	if p.shouldCompress("text/html", 50) {
		t.Error("below min length should be false")
	}
	if !p.shouldCompress("text/html", 200) {
		t.Error("valid type+length should be true")
	}
	if p.shouldCompress("video/mp4", 200) {
		t.Error("unsupported type should be false")
	}
	p.config.MinLength = 0
	if !p.shouldCompress("text/html; charset=utf-8", 0) {
		t.Error("content type params should be stripped")
	}
	if !p.shouldCompress("TEXT/custom", 5) {
		t.Error("custom type should compress")
	}
}

type noFlusher struct {
	http.ResponseWriter
}

type fakeHijacker struct {
	http.ResponseWriter
	conn net.Conn
	rw   *bufio.ReadWriter
}

func (f *fakeHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return f.conn, f.rw, errors.New("hijacked")
}

func TestGzipResponseWriterWriteHeader(t *testing.T) {
	rec := httptest.NewRecorder()
	rec.Header().Set("Content-Type", "text/plain")
	gz, _ := gzip.NewWriterLevel(&bytes.Buffer{}, 6)
	grw := &gzipResponseWriter{ResponseWriter: rec, writer: gz}
	grw.WriteHeader(201)
	if grw.status != 201 || !grw.wroteHeader || grw.contentType != "text/plain" {
		t.Errorf("WriteHeader state: %+v", grw)
	}
}

func TestGzipResponseWriterWrite(t *testing.T) {
	rec := httptest.NewRecorder()
	var buf bytes.Buffer
	gz, _ := gzip.NewWriterLevel(&buf, 6)
	grw := &gzipResponseWriter{ResponseWriter: rec, writer: gz, buf: &buf}
	n, err := grw.Write([]byte("hello"))
	if n != 5 || err != nil {
		t.Errorf("Write: n=%d err=%v", n, err)
	}
	if grw.status != http.StatusOK {
		t.Errorf("auto WriteHeader status = %d", grw.status)
	}
	if grw.originalSize != 5 {
		t.Errorf("originalSize = %d", grw.originalSize)
	}
	// second write: header already written, raw is nil
	n, err = grw.Write([]byte(" world"))
	if n != 6 || err != nil {
		t.Errorf("Write2: n=%d err=%v", n, err)
	}
}

func TestGzipResponseWriterFlush(t *testing.T) {
	rec := httptest.NewRecorder()
	gz, _ := gzip.NewWriterLevel(&bytes.Buffer{}, 6)
	grw := &gzipResponseWriter{ResponseWriter: rec, writer: gz, buf: &bytes.Buffer{}, raw: &bytes.Buffer{}}
	grw.Flush()

	nilWriter := &gzipResponseWriter{ResponseWriter: rec, buf: &bytes.Buffer{}}
	nilWriter.Flush()

	grw2 := &gzipResponseWriter{ResponseWriter: &noFlusher{rec}, writer: gz, buf: &bytes.Buffer{}}
	grw2.Flush()
}

func TestGzipResponseWriterHijack(t *testing.T) {
	rec := httptest.NewRecorder()
	gz, _ := gzip.NewWriterLevel(&bytes.Buffer{}, 6)
	grw := &gzipResponseWriter{ResponseWriter: rec, writer: gz}
	if _, _, err := grw.Hijack(); err != http.ErrNotSupported {
		t.Errorf("expected ErrNotSupported, got %v", err)
	}

	hw := &fakeHijacker{ResponseWriter: rec}
	grw2 := &gzipResponseWriter{ResponseWriter: hw, writer: gz}
	conn, buf, err := grw2.Hijack()
	if err == nil {
		t.Error("expected hijack error")
	}
	_ = conn
	_ = buf
}
