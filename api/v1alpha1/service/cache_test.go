package service

import (
	"net/http"
	"testing"

	diskcache "github.com/cinvat/peretum/internal/cache/disk"
)

func seedServiceCache(t *testing.T, dir string) {
	t.Helper()
	SetCacheDir(dir)
	c, err := diskcache.New(dir, 10<<20, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	c.SetResponseToCache("k1", "example.com", "/a", 200, http.Header{}, []byte("a"))
	c.SetResponseToCache("k2", "example.com", "/b", 200, http.Header{}, []byte("b"))
}

func TestPurgeCacheScopes(t *testing.T) {
	dir := t.TempDir()
	seedServiceCache(t, dir)

	res, err := PurgeCache("example.com", "/a", "", false)
	if err != nil || res.Purged != 1 || res.Published {
		t.Fatalf("path purge = %+v, %v; want {Purged:1}, nil", res, err)
	}
	res, err = PurgeCache("example.com", "", "", false)
	if err != nil || res.Purged != 1 || res.Published {
		t.Fatalf("host purge = %+v, %v; want {Purged:1}, nil", res, err)
	}
}

func TestPurgeCacheAll(t *testing.T) {
	dir := t.TempDir()
	seedServiceCache(t, dir)

	res, err := PurgeCache("", "", "", true)
	if err != nil || res.Purged != 2 || res.Published {
		t.Fatalf("all purge = %+v, %v; want {Purged:2}, nil", res, err)
	}
}

func TestPurgeCacheValidation(t *testing.T) {
	SetCacheDir(t.TempDir())
	for name, args := range map[string]struct {
		host, path, prefix string
		all                bool
	}{
		"empty":             {},
		"path without host": {path: "/a"},
		"path+prefix":       {host: "h", path: "/a", prefix: "/b"},
		"all with host":     {host: "h", all: true},
	} {
		if _, err := PurgeCache(args.host, args.path, args.prefix, args.all); err == nil {
			t.Fatalf("%s: expected validation error", name)
		}
	}
}

func TestPurgeCacheClusterPublishes(t *testing.T) {
	SetCacheDir(t.TempDir())
	SetNATSURI("nats://127.0.0.1:1") // unroutable: publish must fail loudly
	t.Cleanup(func() { SetNATSURI("") })
	if _, err := PurgeCache("example.com", "", "", false); err == nil {
		t.Fatal("expected error publishing to unreachable NATS")
	}
}
