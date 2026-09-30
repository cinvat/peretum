package diskcache

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func seedPurgeEntries(t *testing.T, dir string) *DiskCache {
	t.Helper()
	c, err := New(dir, 10<<20, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.SetResponseToCache("k1", "example.com", "/api/users", 200, http.Header{}, []byte("u"))
	c.SetResponseToCache("k2", "example.com", "/api/posts", 200, http.Header{}, []byte("p"))
	c.SetResponseToCache("k3", "example.com", "/admin", 200, http.Header{}, []byte("a"))
	c.SetResponseToCache("k4", "other.com", "/api/users", 200, http.Header{}, []byte("o"))
	return c
}

func mustMiss(t *testing.T, c *DiskCache, key string) {
	t.Helper()
	if err := c.StreamCachedResponse(httptest.NewRecorder(), key); err == nil {
		t.Fatalf("key %s should miss after purge", key)
	}
}

func TestPurgeFilesByHost(t *testing.T) {
	dir := t.TempDir()
	c := seedPurgeEntries(t, dir)
	c.Close() // simulate out-of-process: files only, no live index

	purged, err := PurgeFilesByHost(dir, "example.com")
	if err != nil {
		t.Fatalf("PurgeFilesByHost: %v", err)
	}
	if purged != 3 {
		t.Fatalf("purged = %d, want 3", purged)
	}

	c2, err := New(dir, 10<<20, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c2.Close()
	mustMiss(t, c2, "k1")
	mustMiss(t, c2, "k2")
	mustMiss(t, c2, "k3")
	if err := c2.StreamCachedResponse(httptest.NewRecorder(), "k4"); err != nil {
		t.Fatalf("other.com entry should survive: %v", err)
	}
}

func TestPurgeFilesByPath(t *testing.T) {
	dir := t.TempDir()
	seedPurgeEntries(t, dir).Close()

	purged, err := PurgeFilesByPath(dir, "example.com", "/api/users")
	if err != nil {
		t.Fatalf("PurgeFilesByPath: %v", err)
	}
	if purged != 1 {
		t.Fatalf("purged = %d, want 1", purged)
	}
}

func TestPurgeFilesByPrefix(t *testing.T) {
	dir := t.TempDir()
	seedPurgeEntries(t, dir).Close()

	// Empty host matches every host under the prefix.
	purged, err := PurgeFilesByPrefix(dir, "", "/api/")
	if err != nil {
		t.Fatalf("PurgeFilesByPrefix: %v", err)
	}
	if purged != 3 {
		t.Fatalf("purged = %d, want 3", purged)
	}
}

func TestPurgeFilesSkipsCorrupt(t *testing.T) {
	dir := t.TempDir()
	seedPurgeEntries(t, dir).Close()

	// Corrupt meta + orphan body must be skipped, never fatal.
	shards, _ := os.ReadDir(dir)
	bad := filepath.Join(dir, shards[0].Name(), "zz.meta")
	if err := os.WriteFile(bad, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	purged, err := PurgeFilesAll(dir)
	if err != nil {
		t.Fatalf("PurgeFilesAll: %v", err)
	}
	if purged != 4 {
		t.Fatalf("purged = %d, want 4", purged)
	}
}

func TestPurgeFilesMissingDir(t *testing.T) {
	if _, err := PurgeFilesAll(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("expected error for missing cache dir")
	}
}
