package diskcache

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestCacheKey_Deterministic(t *testing.T) {
	key1 := CacheKey("example.com", "/api/data")
	key2 := CacheKey("example.com", "/api/data")
	if key1 != key2 {
		t.Errorf("CacheKey not deterministic: %q != %q", key1, key2)
	}
}

func TestCacheKey_DifferentInputs(t *testing.T) {
	key1 := CacheKey("example.com", "/api/data")
	key2 := CacheKey("example.com", "/api/data?page=1")
	if key1 == key2 {
		t.Error("CacheKey should differ for different inputs")
	}
}

func TestCacheKey_DifferentHosts(t *testing.T) {
	key1 := CacheKey("example.com", "/path")
	key2 := CacheKey("other.com", "/path")
	if key1 == key2 {
		t.Error("CacheKey should differ for different hosts")
	}
}

func TestCacheKey_HexFormat(t *testing.T) {
	key := CacheKey("example.com", "/test")
	if len(key) != 64 {
		t.Errorf("expected 64-char hex string, got length %d", len(key))
	}
	for _, c := range key {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Errorf("unexpected character %q in cache key", c)
			break
		}
	}
}

func TestCacheKey_EmptyInputs(t *testing.T) {
	key := CacheKey("", "")
	if len(key) != 64 {
		t.Errorf("expected 64-char hex string for empty inputs, got length %d", len(key))
	}
}

func TestCacheKey_SpecialCharacters(t *testing.T) {
	key := CacheKey("example.com", "/path?q=hello&x=1#frag")
	if len(key) != 64 {
		t.Errorf("expected 64-char hex string, got length %d", len(key))
	}
}

func TestCachePath_ShardedStructure(t *testing.T) {
	key := CacheKey("example.com", "/test")
	path := CachePath("/cache", key, ".body")

	// Should be sharded: /cache/<first2hex>/<rest>.body
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	if !strings.HasPrefix(base, key[2:]) {
		t.Errorf("base %q should start with key[2:] %q", base, key[2:])
	}
	if !strings.HasSuffix(base, ".body") {
		t.Errorf("base %q should end with .body", base)
	}
	if filepath.Base(dir) != key[:2] {
		t.Errorf("shard dir should be %q, got %q", key[:2], filepath.Base(dir))
	}
}

func TestCachePath_DifferentExtensions(t *testing.T) {
	key := CacheKey("example.com", "/test")
	meta := CachePath("/cache", key, ".meta")
	body := CachePath("/cache", key, ".body")

	if filepath.Dir(meta) != filepath.Dir(body) {
		t.Error("meta and body should be in the same shard directory")
	}
	if filepath.Base(meta) == filepath.Base(body) {
		t.Error("meta and body filenames should differ")
	}
}

func TestCachePath_Deterministic(t *testing.T) {
	key := CacheKey("example.com", "/test")
	p1 := CachePath("/cache", key, ".body")
	p2 := CachePath("/cache", key, ".body")
	if p1 != p2 {
		t.Errorf("CachePath not deterministic: %q != %q", p1, p2)
	}
}

func BenchmarkCacheKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		CacheKey("example.com", "/api/v1/users/123?page=1&limit=10")
	}
}
