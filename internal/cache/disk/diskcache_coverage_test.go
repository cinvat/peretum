package diskcache

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLRUShard_GetSizeLen(t *testing.T) {
	s := newLRUShard()

	if e, ok := s.get("missing"); ok || e != nil {
		t.Errorf("expected miss for missing key, got ok=%v e=%v", ok, e)
	}

	s.put("k", 10, time.Time{})
	if e, ok := s.get("k"); !ok || e.size != 10 {
		t.Errorf("expected hit with size 10, got ok=%v size=%v", ok, e.size)
	}
	if got := s.sizeBytes(); got != 10 {
		t.Errorf("sizeBytes=%d, want 10", got)
	}
	if got := s.len(); got != 1 {
		t.Errorf("len=%d, want 1", got)
	}
}

func TestNew_MkdirError(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(filePath, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(filePath, 0, 0); err == nil {
		t.Fatal("expected error when cacheDir is an existing file")
	}
}

func TestStats_RecordHitAndMiss(t *testing.T) {
	c := newTestCache(t)
	c.RecordHit()
	c.RecordMiss()

	hits, misses, evictions, expirations, size := c.Stats()
	if hits != 1 || misses != 1 || evictions != 0 || expirations != 0 {
		t.Errorf("unexpected stats: hits=%d misses=%d evictions=%d expirations=%d", hits, misses, evictions, expirations)
	}
	if size != 0 {
		t.Errorf("expected size 0, got %d", size)
	}
}

func TestSweepLoop_TickerBranch(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1024, 30*time.Millisecond)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	c.touch("sweepkey", 100)
	time.Sleep(50 * time.Millisecond)

	go c.sweepLoop(1 * time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for c.CurrentSize.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if c.CurrentSize.Load() != 0 {
		t.Fatal("sweep did not run through the ticker branch")
	}
}

func TestEnsureSpace_AllBranches(t *testing.T) {
	// MaxSize == 0: unlimited.
	dir := t.TempDir()
	c, err := New(dir, 0, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	if !c.ensureSpace(123456) {
		t.Error("unlimited cache should always have space")
	}

	// Space available without eviction.
	c2, err := New(filepath.Join(t.TempDir()), 1000, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c2.Close()
	c2.touch("k1", 100)
	if !c2.ensureSpace(50) {
		t.Error("expected space to be available")
	}

	// Evict until it fits.
	c3, err := New(filepath.Join(t.TempDir()), 60, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c3.Close()
	c3.touch("k1", 45)
	if !c3.ensureSpace(50) {
		t.Error("expected eviction to free space")
	}
	if c3.CurrentSize.Load() != 0 {
		t.Errorf("expected eviction of k1, CurrentSize=%d", c3.CurrentSize.Load())
	}

	// Nothing left to evict.
	c4, err := New(filepath.Join(t.TempDir()), 5, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c4.Close()
	if c4.ensureSpace(100) {
		t.Error("expected failure when nothing can be evicted")
	}
}

func TestEnsureDir_RecreatesRemovedDirectory(t *testing.T) {
	c := newTestCache(t)

	dir := filepath.Join(c.CacheDir, "sub")
	c.ensureDir(dir)

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}

	c.ensureDir(dir)

	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("directory not recreated: %v", err)
	}
}

func TestPurgeByHost_MissingAndCorruptMeta(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 10<<20, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	c.SetResponseToCache("k1", "example.com", "/a", 200, http.Header{}, []byte("data1"))
	c.SetResponseToCache("k2", "other.com", "/b", 200, http.Header{}, []byte("data2"))

	ghost := CacheKey("ghost.com", "/x")
	c.touch(ghost, 10) // LRU entry with no files on disk

	corrupt := CacheKey("corrupt.com", "/y")
	c.SetResponseToCache(corrupt, "corrupt.com", "/y", 200, http.Header{}, []byte("data3"))
	if err := os.WriteFile(CachePath(c.CacheDir, corrupt, metaExt), []byte{7}, 0644); err != nil {
		t.Fatal(err)
	}

	purged := c.PurgeByHost("example.com")
	if purged != 1 {
		t.Errorf("expected 1 purged (ghost/corrupt skipped), got %d", purged)
	}

	w := httptest.NewRecorder()
	if err := c.StreamCachedResponse(w, "k2"); err != nil {
		t.Errorf("other.com entry should still exist: %v", err)
	}
}

func TestPurgeByPrefix_MissingAndCorruptMeta(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 10<<20, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	c.SetResponseToCache("p1", "example.com", "/api/users", 200, http.Header{}, []byte("d1"))
	c.SetResponseToCache("p2", "example.com", "/admin", 200, http.Header{}, []byte("d2"))

	ghost := CacheKey("ghost.com", "/api/ghost")
	c.touch(ghost, 10)

	corrupt := CacheKey("corrupt.com", "/api/x")
	c.SetResponseToCache(corrupt, "corrupt.com", "/api/x", 200, http.Header{}, []byte("d3"))
	if err := os.WriteFile(CachePath(c.CacheDir, corrupt, metaExt), []byte{7}, 0644); err != nil {
		t.Fatal(err)
	}

	purged := c.PurgeByPrefix("/api/")
	if purged != 1 {
		t.Errorf("expected 1 purged (ghost/corrupt skipped), got %d", purged)
	}

	w := httptest.NewRecorder()
	if err := c.StreamCachedResponse(w, "p2"); err != nil {
		t.Errorf("/admin entry should still exist: %v", err)
	}
}

// httpHeader is a small alias to keep test declarations terse.
type httpHeader = map[string][]string
