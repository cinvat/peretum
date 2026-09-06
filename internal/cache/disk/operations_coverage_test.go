package diskcache

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// errWriter fails every Write.
type errWriter struct{ err error }

func (e errWriter) Write(p []byte) (int, error) { return 0, e.err }

// callFailingWriter fails the underlying Write at a given call number.
type callFailingWriter struct {
	calls  int
	failAt int
}

func (w *callFailingWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return 0, errors.New("injected write failure")
	}
	return len(p), nil
}

// fakeTempFile satisfies tempFile and injects failures on demand.
type fakeTempFile struct {
	name      string
	failWrite bool
	failSync  bool
	failClose bool
}

func (f *fakeTempFile) Name() string { return f.name }
func (f *fakeTempFile) Write(p []byte) (int, error) {
	if f.failWrite {
		return 0, errors.New("write: injected failure")
	}
	return len(p), nil
}
func (f *fakeTempFile) Sync() error {
	if f.failSync {
		return errors.New("sync: injected failure")
	}
	return nil
}
func (f *fakeTempFile) Close() error {
	if f.failClose {
		return errors.New("close: injected failure")
	}
	return nil
}

func withCreateTempFile(t *testing.T, fn func(dir, pattern string) (tempFile, error)) {
	t.Helper()
	orig := createTempFile
	createTempFile = fn
	t.Cleanup(func() { createTempFile = orig })
}

func withWriteMetadataFn(t *testing.T, fn func(w io.Writer, statusCode int, host, path string, headers http.Header) error) {
	t.Helper()
	orig := writeMetadataFn
	writeMetadataFn = fn
	t.Cleanup(func() { writeMetadataFn = orig })
}

// failingResponseWriter records header writes but errors on body writes.
type failingResponseWriter struct {
	h    http.Header
	code int
}

func (w *failingResponseWriter) Header() http.Header {
	if w.h == nil {
		w.h = make(http.Header)
	}
	return w.h
}
func (w *failingResponseWriter) WriteHeader(code int) { w.code = code }
func (w *failingResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("injected client write failure")
}

// failReader returns an error on every Read.
type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, errors.New("injected read failure") }

func TestWriteMetadata_ErrorBranches(t *testing.T) {
	// Fail at each successive underlying Write call to exercise every
	// error-return statement: version, status, host len/bytes,
	// path len/bytes, numHeaders, header key len/bytes, header value len/bytes.
	for _, failAt := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11} {
		w := &callFailingWriter{failAt: failAt}
		if err := writeMetadata(w, 200, "host", "/path", http.Header{"K": {"v"}}); err == nil {
			t.Errorf("failAt=%d: expected error", failAt)
		}
	}
}

func TestWriteString_ErrorBranches(t *testing.T) {
	if err := writeString(&callFailingWriter{failAt: 1}, "ab"); err == nil {
		t.Error("expected error on length write")
	}
	if err := writeString(&callFailingWriter{failAt: 2}, "ab"); err == nil {
		t.Error("expected error on string write")
	}
}

func TestReadString_ErrorBranches(t *testing.T) {
	if _, err := readString(bufio.NewReader(bytes.NewReader(nil))); err == nil {
		t.Error("expected error reading length")
	}
	if _, err := readString(bufio.NewReader(bytes.NewReader([]byte{10, 0, 0, 0, 'a'}))); err == nil {
		t.Error("expected error reading full string")
	}
}

func TestReadMetadata_Empty(t *testing.T) {
	if _, _, _, _, err := readMetadata(bufio.NewReader(bytes.NewReader(nil))); err == nil {
		t.Error("expected error on empty input")
	}
}

func TestReadMetadataV1_ErrorBranches(t *testing.T) {
	cases := [][]byte{
		{3},
		{3, 0, 1},
		{3, 0, 1, 0, 0, 0},
		{3, 0, 1, 0, 0, 0, 2, 0, 0, 0, 'k', 'k'},
	}
	for _, in := range cases {
		if _, _, _, _, err := readMetadata(bufio.NewReader(bytes.NewReader(in))); err == nil {
			t.Errorf("input %v: expected error", in)
		}
	}
}

func TestReadMetadataV2_ErrorBranches(t *testing.T) {
	cases := [][]byte{
		{2},
		{2, 0, 0},
		{2, 0, 0, 2, 0, 0, 0, 'h'},
		{2, 0, 0, 2, 0, 0, 0, 'h', 'h'},
		{2, 0, 0, 2, 0, 0, 0, 'h', 'h', 2, 0, 0, 0, 'p', 'p'},
		{2, 0, 0, 2, 0, 0, 0, 'h', 'h', 2, 0, 0, 0, 'p', 'p', 1, 0, 0, 0},
		{2, 0, 0, 2, 0, 0, 0, 'h', 'h', 2, 0, 0, 0, 'p', 'p', 1, 0, 0, 0, 1, 0, 0, 0, 'k'},
	}
	for _, in := range cases {
		if _, _, _, _, err := readMetadata(bufio.NewReader(bytes.NewReader(in))); err == nil {
			t.Errorf("input %v: expected error", in)
		}
	}
}

func TestWriteAtomic_ErrorBranches(t *testing.T) {
	// CreateTemp failure: parent directory does not exist.
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if err := writeAtomic(filepath.Join(missing, "f"), []byte("x")); err == nil {
		t.Error("expected error when parent dir does not exist")
	}

	realDir := t.TempDir()
	fails := []struct {
		name string
		f    *fakeTempFile
	}{
		{"write", &fakeTempFile{name: filepath.Join(realDir, "t1"), failWrite: true}},
		{"sync", &fakeTempFile{name: filepath.Join(realDir, "t2"), failSync: true}},
		{"close", &fakeTempFile{name: filepath.Join(realDir, "t3"), failClose: true}},
	}
	for _, tc := range fails {
		withCreateTempFile(t, func(dir, pattern string) (tempFile, error) {
			return tc.f, nil
		})
		if err := writeAtomic(filepath.Join(realDir, "out_"+tc.name), []byte("x")); err == nil {
			t.Errorf("%s: expected error", tc.name)
		}
	}

	// Rename failure: target path is an existing directory.
	target := filepath.Join(realDir, "existing-dir")
	if err := os.MkdirAll(target, 0755); err != nil {
		t.Fatal(err)
	}
	withCreateTempFile(t, func(dir, pattern string) (tempFile, error) {
		return os.CreateTemp(dir, pattern)
	})
	if err := writeAtomic(target, []byte("x")); err == nil {
		t.Error("expected error renaming onto an existing directory")
	}
}

func TestSetResponseToCache_MetadataWriteError(t *testing.T) {
	c := newTestCache(t)
	withWriteMetadataFn(t, func(w io.Writer, statusCode int, host, path string, headers http.Header) error {
		return errors.New("injected metadata error")
	})
	if err := c.SetResponseToCache("kk", "h", "/p", 200, http.Header{"A": {"b"}}, []byte("body")); err == nil {
		t.Error("expected metadata write error")
	}
	if c.CurrentSize.Load() != 0 {
		t.Errorf("nothing should be cached, CurrentSize=%d", c.CurrentSize.Load())
	}
}

func TestSetResponseToCache_CacheFull(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 5, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	if err := c.SetResponseToCache("kk", "h", "/p", 200, http.Header{}, []byte("boddddddy")); err == nil {
		t.Error("expected cache-full error")
	}
}

func TestSetResponseToCache_EvictsToFit(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 60, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	c.SetResponseToCache("k1", "h", "/p1", 200, http.Header{}, []byte("short"))
	c.SetResponseToCache("k2", "h", "/p2", 200, http.Header{}, []byte("short"))

	if sz := c.CurrentSize.Load(); sz > 60 {
		t.Errorf("cache exceeded MaxSize: %d", sz)
	}
}

func TestSetResponseToCache_MetaAtomicWriteError(t *testing.T) {
	c := newTestCache(t)
	withCreateTempFile(t, func(dir, pattern string) (tempFile, error) {
		return nil, errors.New("atomics unavailable")
	})
	if err := c.SetResponseToCache("kk", "h", "/p", 200, http.Header{}, []byte("body")); err == nil {
		t.Error("expected error on meta atomic write")
	}
}

func TestSetResponseToCache_BodyAtomicWriteError(t *testing.T) {
	c := newTestCache(t)
	var calls int
	withCreateTempFile(t, func(dir, pattern string) (tempFile, error) {
		calls++
		if calls == 2 {
			return nil, errors.New("atomics unavailable on body write")
		}
		return os.CreateTemp(dir, pattern)
	})
	if err := c.SetResponseToCache("kk", "h", "/p", 200, http.Header{}, []byte("body")); err == nil {
		t.Error("expected error on body atomic write")
	}
}

func TestStreamCachedResponse_NotExpired(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 10<<20, time.Hour)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	c.SetResponseToCache("fresh", "h", "/p", 200, http.Header{}, []byte("data"))

	w := httptest.NewRecorder()
	if err := c.StreamCachedResponse(w, "fresh"); err != nil {
		t.Fatalf("StreamCachedResponse: %v", err)
	}
	if w.Body.String() != "data" {
		t.Errorf("body mismatch: %q", w.Body.String())
	}
}

func TestStreamCachedResponse_MetaOpenError(t *testing.T) {
	c := newTestCache(t)
	c.SetResponseToCache("kk", "h", "/p", 200, http.Header{}, []byte("data"))

	orig := openMetaFile
	openMetaFile = func(name string) (*os.File, error) {
		return nil, errors.New("injected open failure")
	}
	t.Cleanup(func() { openMetaFile = orig })

	w := httptest.NewRecorder()
	if err := c.StreamCachedResponse(w, "kk"); err == nil {
		t.Error("expected error on meta open failure")
	}
}

func TestStreamCachedResponse_CorruptMeta(t *testing.T) {
	c := newTestCache(t)
	c.SetResponseToCache("bad", "h", "/p", 200, http.Header{}, []byte("data"))

	if err := os.WriteFile(CachePath(c.CacheDir, "bad", metaExt), []byte{2}, 0644); err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	if err := c.StreamCachedResponse(w, "bad"); err == nil {
		t.Error("expected error on corrupt metadata")
	}
}

func TestStreamCachedResponse_MissingBody(t *testing.T) {
	c := newTestCache(t)
	c.SetResponseToCache("nobody", "h", "/p", 200, http.Header{}, []byte("data"))

	if err := os.Remove(CachePath(c.CacheDir, "nobody", bodyExt)); err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	if err := c.StreamCachedResponse(w, "nobody"); err == nil {
		t.Error("expected error on missing body file")
	}
}

func TestStreamCachedResponse_CopyError(t *testing.T) {
	c := newTestCache(t)
	c.SetResponseToCache("copyfail", "h", "/p", 200, http.Header{}, []byte("data"))

	fw := &failingResponseWriter{}
	if err := c.StreamCachedResponse(fw, "copyfail"); err == nil {
		t.Error("expected error on client write failure")
	}
	if fw.code != 200 {
		t.Errorf("expected status 200 to be written first, got %d", fw.code)
	}
}

func TestParseCacheStatus_ReadError(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(failReader{}),
	}
	if _, _, _, err := ParseCacheStatus(resp); err == nil {
		t.Error("expected read error")
	}
}
