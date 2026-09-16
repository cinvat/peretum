package diskcache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWriteReadMetadata_Roundtrip(t *testing.T) {
	headers := http.Header{
		"Content-Type":  {"application/json"},
		"X-Custom":      {"value1"},
		"Cache-Control": {"max-age=3600"},
	}

	var buf bytes.Buffer
	if err := writeMetadata(&buf, 201, "example.com", "/api/test", headers); err != nil {
		t.Fatalf("writeMetadata: %v", err)
	}

	statusCode, host, path, parsed, err := readMetadata(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("readMetadata: %v", err)
	}

	if statusCode != 201 {
		t.Errorf("expected status 201, got %d", statusCode)
	}
	if host != "example.com" {
		t.Errorf("expected host example.com, got %q", host)
	}
	if path != "/api/test" {
		t.Errorf("expected path /api/test, got %q", path)
	}
	if parsed.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type mismatch: %q", parsed.Get("Content-Type"))
	}
	if parsed.Get("X-Custom") != "value1" {
		t.Errorf("X-Custom mismatch: %q", parsed.Get("X-Custom"))
	}
	if parsed.Get("Cache-Control") != "max-age=3600" {
		t.Errorf("Cache-Control mismatch: %q", parsed.Get("Cache-Control"))
	}
}

func TestWriteReadMetadata_EmptyHeaders(t *testing.T) {
	headers := http.Header{}

	var buf bytes.Buffer
	if err := writeMetadata(&buf, 404, "example.com", "/notfound", headers); err != nil {
		t.Fatalf("writeMetadata: %v", err)
	}

	statusCode, host, path, parsed, err := readMetadata(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("readMetadata: %v", err)
	}

	if statusCode != 404 {
		t.Errorf("expected status 404, got %d", statusCode)
	}
	if host != "example.com" {
		t.Errorf("expected host example.com, got %q", host)
	}
	if path != "/notfound" {
		t.Errorf("expected path /notfound, got %q", path)
	}
	if len(parsed) != 0 {
		t.Errorf("expected 0 headers, got %d", len(parsed))
	}
}

func TestWriteReadMetadata_MultiValueHeader(t *testing.T) {
	headers := http.Header{
		"Set-Cookie": {"a=1", "b=2"},
	}

	var buf bytes.Buffer
	if err := writeMetadata(&buf, 200, "example.com", "/cookie", headers); err != nil {
		t.Fatalf("writeMetadata: %v", err)
	}

	_, _, _, parsed, err := readMetadata(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("readMetadata: %v", err)
	}

	val := parsed.Get("Set-Cookie")
	if !strings.Contains(val, "a=1") || !strings.Contains(val, "b=2") {
		t.Errorf("multi-value header not preserved: %q", val)
	}
}

func TestWriteAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")
	data := []byte("hello world")

	if err := writeAtomic(path, data); err != nil {
		t.Fatalf("writeAtomic: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("expected %q, got %q", data, got)
	}
}

func TestWriteAtomic_NoTempFilesLeft(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.txt")

	if err := writeAtomic(path, []byte("data")); err != nil {
		t.Fatalf("writeAtomic: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".cache-tmp-") {
			t.Errorf("temp file left behind: %s", e.Name())
		}
	}
}

func TestSetResponseToCache_Caches2xx(t *testing.T) {
	c := newTestCache(t)
	headers := http.Header{"Content-Type": {"text/plain"}}
	body := []byte("hello")

	if err := c.SetResponseToCache("testkey", "example.com", "/test", 200, headers, body); err != nil {
		t.Fatalf("SetResponseToCache: %v", err)
	}

	metaPath := CachePath(c.CacheDir, "testkey", metaExt)
	bodyPath := CachePath(c.CacheDir, "testkey", bodyExt)

	if _, err := os.Stat(metaPath); err != nil {
		t.Errorf("meta file not created: %v", err)
	}
	if _, err := os.Stat(bodyPath); err != nil {
		t.Errorf("body file not created: %v", err)
	}

	if c.CurrentSize.Load() == 0 {
		t.Error("CurrentSize should be > 0 after caching")
	}
}

func TestSetResponseToCache_SkipsNon2xx(t *testing.T) {
	c := newTestCache(t)

	for _, code := range []int{199, 300, 404, 500} {
		c.SetResponseToCache("testkey", "example.com", "/test", code, http.Header{}, []byte("body"))
	}

	if c.CurrentSize.Load() != 0 {
		t.Errorf("non-2xx responses should not be cached, CurrentSize=%d", c.CurrentSize.Load())
	}
}

func TestSetResponseToCache_SkipsEmptyBody(t *testing.T) {
	c := newTestCache(t)

	c.SetResponseToCache("testkey", "example.com", "/test", 200, http.Header{}, []byte{})

	if c.CurrentSize.Load() != 0 {
		t.Errorf("empty body should not be cached, CurrentSize=%d", c.CurrentSize.Load())
	}
}

func TestStreamCachedResponse_CacheHit(t *testing.T) {
	c := newTestCache(t)
	headers := http.Header{"X-Test": {"yes"}}
	body := []byte("cached content")

	if err := c.SetResponseToCache("hitkey", "example.com", "/hit", 200, headers, body); err != nil {
		t.Fatalf("SetResponseToCache: %v", err)
	}

	w := httptest.NewRecorder()
	if err := c.StreamCachedResponse(w, "hitkey"); err != nil {
		t.Fatalf("StreamCachedResponse: %v", err)
	}

	if w.Code != 200 {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	if w.Body.String() != "cached content" {
		t.Errorf("body mismatch: %q", w.Body.String())
	}
	if w.Header().Get("X-Test") != "yes" {
		t.Errorf("X-Test header missing: %v", w.Header())
	}
	if w.Header().Get("X-Cache") != "HIT" {
		t.Errorf("X-Cache header missing or wrong: %q", w.Header().Get("X-Cache"))
	}
	if w.Header().Get("X-Cache-Age") == "" {
		t.Error("X-Cache-Age header missing")
	}
}

func TestStreamCachedResponse_CacheMiss(t *testing.T) {
	c := newTestCache(t)

	w := httptest.NewRecorder()
	err := c.StreamCachedResponse(w, "nonexistent")
	if err == nil {
		t.Fatal("expected error for cache miss")
	}
}

func TestPeek_ExistenceAndFreshness(t *testing.T) {
	c := newTestCache(t) // MaxAge = 0: only an explicit per-entry TTL expires entries

	if c.Peek("nonexistent", 0) {
		t.Error("Peek on a missing key should return false")
	}

	c.SetResponseToCache("peekkey", "example.com", "/peek", 200, http.Header{}, []byte("data"))
	if !c.Peek("peekkey", time.Hour) {
		t.Error("fresh entry should Peek true")
	}
	if !c.Peek("peekkey", 0) {
		t.Error("global MaxAge 0 means never expired, Peek should be true")
	}

	time.Sleep(50 * time.Millisecond)
	if c.Peek("peekkey", 10*time.Millisecond) {
		t.Error("entry past its per-entry TTL should Peek false")
	}
}

func TestStreamCachedResponse_ExpiredEntry(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 10<<20, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	c.SetResponseToCache("expkey", "example.com", "/expired", 200, http.Header{}, []byte("data"))

	time.Sleep(100 * time.Millisecond)

	w := httptest.NewRecorder()
	err = c.StreamCachedResponse(w, "expkey")
	if err == nil {
		t.Fatal("expected error for expired entry")
	}

	metaPath := CachePath(dir, "expkey", metaExt)
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Error("expired .meta file should have been deleted")
	}
}

func TestStreamCachedResponseTTL_Expiry(t *testing.T) {
	c := newTestCache(t) // MaxAge = 0: only an explicit per-entry TTL expires entries
	c.SetResponseToCache("ttlkey", "example.com", "/ttl", 200, http.Header{}, []byte("data"))

	// Fresh entry: a TTL that has not elapsed yet still serves a hit.
	w := httptest.NewRecorder()
	if err := c.StreamCachedResponseTTL(w, "ttlkey", time.Hour); err != nil {
		t.Fatalf("StreamCachedResponseTTL: %v", err)
	}
	if w.Header().Get("X-Cache") != "HIT" {
		t.Errorf("X-Cache=%q", w.Header().Get("X-Cache"))
	}

	time.Sleep(50 * time.Millisecond)

	// A short per-entry TTL expires the entry even though the global
	// MaxAge (0) would never expire it.
	w2 := httptest.NewRecorder()
	err := c.StreamCachedResponseTTL(w2, "ttlkey", 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected error for entry expired by per-entry TTL")
	}

	metaPath := CachePath(c.CacheDir, "ttlkey", metaExt)
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Error("expired .meta file should have been deleted")
	}
}

func TestSetAndStream_LargeBody(t *testing.T) {
	c := newTestCache(t)
	body := bytes.Repeat([]byte("x"), 1<<20) // 1MB

	if err := c.SetResponseToCache("large", "example.com", "/large", 200, http.Header{"Content-Type": {"application/octet-stream"}}, body); err != nil {
		t.Fatalf("SetResponseToCache: %v", err)
	}

	w := httptest.NewRecorder()
	if err := c.StreamCachedResponse(w, "large"); err != nil {
		t.Fatalf("StreamCachedResponse: %v", err)
	}

	if w.Body.Len() != len(body) {
		t.Errorf("body length mismatch: expected %d, got %d", len(body), w.Body.Len())
	}
}

func TestSetAndStream_Concurrent(t *testing.T) {
	c := newTestCache(t)
	headers := http.Header{"Content-Type": {"text/plain"}}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := "concurrent_key"
			body := []byte("data")
			c.SetResponseToCache(key, "example.com", "/concurrent", 200, headers, body)

			w := httptest.NewRecorder()
			c.StreamCachedResponse(w, key)
		}(i)
	}
	wg.Wait()
}

func TestSetResponseToCache_EvictsWhenFull(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 200, 0) // very small cache
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	c.SetResponseToCache("k1", "example.com", "/k1", 200, http.Header{}, []byte("short"))
	c.SetResponseToCache("k2", "example.com", "/k2", 200, http.Header{}, []byte("short"))

	if c.CurrentSize.Load() > 200 {
		t.Errorf("cache exceeded MaxSize: %d", c.CurrentSize.Load())
	}
}

func TestParseCacheStatus(t *testing.T) {
	resp := &http.Response{
		StatusCode: 201,
		Header:     http.Header{"X-Foo": {"bar"}},
		Body:       httptest.NewRecorder().Result().Body,
	}
	// httptest.NewRecorder().Result().Body is empty; use a real body
	resp.Body.Close()

	resp.Body = io.NopCloser(strings.NewReader("response body"))

	statusCode, headers, body, err := ParseCacheStatus(resp)
	if err != nil {
		t.Fatalf("ParseCacheStatus: %v", err)
	}

	if statusCode != 201 {
		t.Errorf("expected 201, got %d", statusCode)
	}
	if headers.Get("X-Foo") != "bar" {
		t.Errorf("X-Foo mismatch: %q", headers.Get("X-Foo"))
	}
	if string(body) != "response body" {
		t.Errorf("body mismatch: %q", body)
	}
}

func TestPurgeByHost(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 10<<20, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	c.SetResponseToCache("k1", "example.com", "/a", 200, http.Header{}, []byte("data1"))
	c.SetResponseToCache("k2", "example.com", "/b", 200, http.Header{}, []byte("data2"))
	c.SetResponseToCache("k3", "other.com", "/a", 200, http.Header{}, []byte("data3"))

	if c.CurrentSize.Load() == 0 {
		t.Fatal("cache should have entries")
	}

	purged := c.PurgeByHost("example.com")
	if purged != 2 {
		t.Errorf("expected 2 purged, got %d", purged)
	}

	// Verify other.com entry still exists
	w := httptest.NewRecorder()
	err = c.StreamCachedResponse(w, "k3")
	if err != nil {
		t.Errorf("other.com entry should still exist: %v", err)
	}
}

func TestPurgeByPath(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 10<<20, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	c.SetResponseToCache("k1", "example.com", "/api/users", 200, http.Header{}, []byte("data1"))
	c.SetResponseToCache("k2", "example.com", "/api/posts", 200, http.Header{}, []byte("data2"))
	c.SetResponseToCache("k3", "example.com", "/admin", 200, http.Header{}, []byte("data3"))

	purged := c.PurgeByPath("/api/users")
	if purged != 1 {
		t.Errorf("expected 1 purged, got %d", purged)
	}

	// Verify other entries still exist
	w := httptest.NewRecorder()
	err = c.StreamCachedResponse(w, "k2")
	if err != nil {
		t.Errorf("/api/posts entry should still exist: %v", err)
	}
	err = c.StreamCachedResponse(w, "k3")
	if err != nil {
		t.Errorf("/admin entry should still exist: %v", err)
	}
}

func TestPurgeByPrefix(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 10<<20, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	c.SetResponseToCache("k1", "example.com", "/api/users", 200, http.Header{}, []byte("data1"))
	c.SetResponseToCache("k2", "example.com", "/api/posts", 200, http.Header{}, []byte("data2"))
	c.SetResponseToCache("k3", "example.com", "/admin", 200, http.Header{}, []byte("data3"))
	c.SetResponseToCache("k4", "other.com", "/api/users", 200, http.Header{}, []byte("data4"))

	purged := c.PurgeByPrefix("/api/")
	if purged != 3 {
		t.Errorf("expected 3 purged, got %d", purged)
	}

	// Verify /admin still exists
	w := httptest.NewRecorder()
	err = c.StreamCachedResponse(w, "k3")
	if err != nil {
		t.Errorf("/admin entry should still exist: %v", err)
	}
}

func TestPurgeAll(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 10<<20, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	c.SetResponseToCache("k1", "example.com", "/a", 200, http.Header{}, []byte("data1"))
	c.SetResponseToCache("k2", "other.com", "/b", 200, http.Header{}, []byte("data2"))

	purged := c.PurgeAll()
	if purged != 2 {
		t.Errorf("expected 2 purged, got %d", purged)
	}

	if c.CurrentSize.Load() != 0 {
		t.Errorf("cache should be empty after PurgeAll, CurrentSize=%d", c.CurrentSize.Load())
	}
}

func TestReadMetadata_BackwardCompatibility(t *testing.T) {
	// Test reading v1 format (legacy, no version byte)
	headers := http.Header{
		"Content-Type": {"application/json"},
	}

	var buf bytes.Buffer
	// Write v1 format manually (no version byte)
	if err := binary.Write(&buf, binary.LittleEndian, uint16(200)); err != nil {
		t.Fatalf("write status: %v", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(headers))); err != nil {
		t.Fatalf("write numHeaders: %v", err)
	}
	for k, vals := range headers {
		if err := writeString(&buf, k); err != nil {
			t.Fatalf("write key: %v", err)
		}
		joined := strings.Join(vals, ", ")
		if err := writeString(&buf, joined); err != nil {
			t.Fatalf("write val: %v", err)
		}
	}

	statusCode, host, path, parsed, err := readMetadata(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("readMetadata v1: %v", err)
	}

	if statusCode != 200 {
		t.Errorf("expected status 200, got %d", statusCode)
	}
	if host != "" {
		t.Errorf("expected empty host for v1, got %q", host)
	}
	if path != "" {
		t.Errorf("expected empty path for v1, got %q", path)
	}
	if parsed.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type mismatch: %q", parsed.Get("Content-Type"))
	}
}

func TestReadMetadata_VersionedFormat(t *testing.T) {
	// Test reading v2 format (with version byte)
	headers := http.Header{
		"Content-Type": {"application/json"},
	}

	var buf bytes.Buffer
	// Write v2 format using writeMetadata
	if err := writeMetadata(&buf, 201, "example.com", "/api/test", headers); err != nil {
		t.Fatalf("writeMetadata: %v", err)
	}

	statusCode, host, path, parsed, err := readMetadata(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("readMetadata v2: %v", err)
	}

	if statusCode != 201 {
		t.Errorf("expected status 201, got %d", statusCode)
	}
	if host != "example.com" {
		t.Errorf("expected host example.com, got %q", host)
	}
	if path != "/api/test" {
		t.Errorf("expected path /api/test, got %q", path)
	}
	if parsed.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type mismatch: %q", parsed.Get("Content-Type"))
	}
}
