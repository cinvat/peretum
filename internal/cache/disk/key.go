package diskcache

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
)

// CacheKey generates a deterministic cache key by SHA-256 hashing the
// concatenation of host and requestURI with a delimiter. The result is a 64-character
// hex string safe for use as a filename, eliminating path traversal
// risks and deeply nested directory trees.
//
// The delimiter prevents collisions like host="a", path="bc" vs host="ab", path="c".
//
// Example:
//
//	key := CacheKey("example.com", "/api/data?page=1")
//	// key = "a3f2b8c9..."
func CacheKey(host, path string) string {
	h := sha256.Sum256([]byte(host + "|" + path))
	return fmt.Sprintf("%x", h)
}

// CachePath returns the on-disk path for a cache key, sharded into
// subdirectories to avoid placing thousands of files in a single
// directory. The first two hex characters of the key are used as
// the subdirectory name (256 possible buckets).
//
// Result: <cacheDir>/<first2hex>/<remaining>.<ext>
//
// Example:
//
//	CachePath("/cache", "a3f2b8c9...", ".body")
//	// => "/cache/a3/f2b8c9....body"
func CachePath(cacheDir, key, ext string) string {
	return filepath.Join(cacheDir, key[:2], key[2:]+ext)
}
