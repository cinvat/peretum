package diskcache

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestCache(t *testing.T) *DiskCache {
	t.Helper()
	dir := t.TempDir()
	c, err := New(dir, 10<<20, 0)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestNew_CreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub", "cache")
	c, err := New(dir, 1024, time.Hour)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("cache dir not created: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("cache path is not a directory")
	}
}

func TestNew_InitialSizes(t *testing.T) {
	c := newTestCache(t)
	if c.CurrentSize.Load() != 0 {
		t.Errorf("expected CurrentSize=0, got %d", c.CurrentSize.Load())
	}
	if c.MaxSize != 10<<20 {
		t.Errorf("expected MaxSize=%d, got %d", int64(10<<20), c.MaxSize)
	}
}

func TestClose_StopsSweep(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 1024, time.Hour)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	c.Close()
	time.Sleep(50 * time.Millisecond)
}

func TestTouch_InsertAndUpdate(t *testing.T) {
	c := newTestCache(t)

	c.touch("key1", 100)
	if c.CurrentSize.Load() != 100 {
		t.Errorf("expected 100, got %d", c.CurrentSize.Load())
	}

	c.touch("key1", 200)
	if c.CurrentSize.Load() != 200 {
		t.Errorf("expected 200 after update, got %d", c.CurrentSize.Load())
	}

	// Check via shard
	shard := c.shard("key1")
	shard.mu.Lock()
	e, ok := shard.lru["key1"]
	shard.mu.Unlock()
	if !ok {
		t.Fatal("key1 not in LRU map")
	}
	if e.size != 200 {
		t.Errorf("expected size=200, got %d", e.size)
	}
}

func TestTouch_MultipleKeys(t *testing.T) {
	c := newTestCache(t)

	c.touch("k1", 100)
	c.touch("k2", 200)
	c.touch("k3", 300)

	if c.CurrentSize.Load() != 600 {
		t.Errorf("expected 600, got %d", c.CurrentSize.Load())
	}
}

func TestTouch_WithMaxAge(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 10<<20, time.Hour)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	c.touch("key1", 100)

	shard := c.shard("key1")
	shard.mu.Lock()
	e := shard.lru["key1"]
	shard.mu.Unlock()

	if e.expiresAt.IsZero() {
		t.Fatal("expected expiresAt to be set when MaxAge > 0")
	}
	if time.Until(e.expiresAt) > time.Hour+time.Second {
		t.Errorf("expiresAt is too far in the future: %v", e.expiresAt)
	}
}

func TestRemove(t *testing.T) {
	c := newTestCache(t)

	c.touch("key1", 100)
	c.remove("key1")

	if c.CurrentSize.Load() != 0 {
		t.Errorf("expected 0 after remove, got %d", c.CurrentSize.Load())
	}

	shard := c.shard("key1")
	shard.mu.Lock()
	_, ok := shard.lru["key1"]
	shard.mu.Unlock()
	if ok {
		t.Error("key1 should not be in LRU map after remove")
	}
}

func TestRemove_NonexistentKey(t *testing.T) {
	c := newTestCache(t)
	c.remove("nonexistent") // should not panic
	if c.CurrentSize.Load() != 0 {
		t.Error("CurrentSize should remain 0")
	}
}

func TestEvict_RemovesLRU(t *testing.T) {
	c := newTestCache(t)

	c.touch("k1", 100)
	time.Sleep(time.Millisecond)
	c.touch("k2", 200)
	time.Sleep(time.Millisecond)
	c.touch("k3", 300)

	c.evict()

	if c.CurrentSize.Load() != 500 {
		t.Errorf("expected 500 after evict (k1 removed), got %d", c.CurrentSize.Load())
	}
}

func TestEvict_EmptyCache(t *testing.T) {
	c := newTestCache(t)
	c.evict() // should not panic
	if c.CurrentSize.Load() != 0 {
		t.Error("CurrentSize should remain 0")
	}
}

func TestEnsureDir_CachesDirectory(t *testing.T) {
	c := newTestCache(t)

	dir := filepath.Join(c.CacheDir, "a", "b", "c")
	c.ensureDir(dir)

	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("directory not created: %v", err)
	}

	c.dirsMu.RLock()
	_, ok := c.dirs[dir]
	c.dirsMu.RUnlock()
	if !ok {
		t.Error("directory should be cached in dirs map")
	}
}

func TestEnsureDir_NoRedundantMkdirAll(t *testing.T) {
	c := newTestCache(t)

	dir := filepath.Join(c.CacheDir, "x")
	c.ensureDir(dir)
	c.ensureDir(dir) // second call should be a no-op

	c.dirsMu.RLock()
	count := len(c.dirs)
	c.dirsMu.RUnlock()
	if count != 1 {
		t.Errorf("expected 1 cached dir, got %d", count)
	}
}

func TestSweep_RemovesExpiredEntries(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 10<<20, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	c.touch("expired_key", 100)

	time.Sleep(100 * time.Millisecond)
	c.sweep()

	if c.CurrentSize.Load() != 0 {
		t.Errorf("expected 0 after sweep, got %d", c.CurrentSize.Load())
	}
}

func TestSweep_KeepsFreshEntries(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, 10<<20, time.Hour)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	c.touch("fresh_key", 100)
	c.sweep()

	if c.CurrentSize.Load() != 100 {
		t.Errorf("expected 100 (fresh entry kept), got %d", c.CurrentSize.Load())
	}
}

func TestSweep_NoMaxAge(t *testing.T) {
	c := newTestCache(t) // MaxAge = 0
	c.touch("key1", 100)
	c.sweep()

	if c.CurrentSize.Load() != 100 {
		t.Errorf("expected 100 (no expiration), got %d", c.CurrentSize.Load())
	}
}

func TestTouch_Concurrent(t *testing.T) {
	c := newTestCache(t)
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := "key"
			c.touch(key, int64(n))
		}(i)
	}
	wg.Wait()

	shard := c.shard("key")
	shard.mu.Lock()
	_, ok := shard.lru["key"]
	shard.mu.Unlock()
	if !ok {
		t.Error("key should exist after concurrent touches")
	}
}

func TestRemove_Concurrent(t *testing.T) {
	c := newTestCache(t)
	c.touch("key1", 100)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.remove("key1")
		}()
	}
	wg.Wait()

	if c.CurrentSize.Load() != 0 {
		t.Errorf("expected 0 after concurrent removes, got %d", c.CurrentSize.Load())
	}
}
