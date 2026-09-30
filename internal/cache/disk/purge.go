package diskcache

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// PurgeFiles removes cached entries on disk whose metadata matches match,
// without requiring a live DiskCache index. It exists for out-of-process
// callers (e.g. the REST API, which runs in a different process than the
// proxy): the proxy's read path stats/opens entry files on every request,
// so files removed here become misses immediately. The proxy's in-memory
// LRU may briefly over-count size until those keys age out — a bounded
// accounting drift, never a stale serve.
//
// Unreadable or corrupt metadata files are skipped, never fatal.
// It returns the number of entries removed.
func PurgeFiles(cacheDir string, match func(host, path string) bool) (int, error) {
	shards, err := os.ReadDir(cacheDir)
	if err != nil {
		return 0, err
	}
	purged := 0
	for _, shard := range shards {
		if !shard.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(cacheDir, shard.Name()))
		if err != nil {
			continue
		}
		for _, f := range files {
			if f.IsDir() || filepath.Ext(f.Name()) != metaExt {
				continue
			}
			metaPath := filepath.Join(cacheDir, shard.Name(), f.Name())
			host, path, ok := purgeProbe(metaPath)
			if !ok || !match(host, path) {
				continue
			}
			_ = os.Remove(metaPath)
			_ = os.Remove(metaPath[:len(metaPath)-len(metaExt)] + bodyExt)
			purged++
		}
	}
	return purged, nil
}

// purgeProbe reads an entry's host/path from its metadata sidecar.
// It reports false when the file cannot be read or parsed.
func purgeProbe(metaPath string) (host, path string, ok bool) {
	metaFile, err := os.Open(metaPath)
	if err != nil {
		return "", "", false
	}
	defer metaFile.Close()
	_, host, path, _, err = readMetadata(bufio.NewReader(metaFile))
	if err != nil {
		return "", "", false
	}
	return host, path, true
}

// PurgeFilesByHost removes every cached entry for host from cacheDir.
func PurgeFilesByHost(cacheDir, host string) (int, error) {
	return PurgeFiles(cacheDir, func(h, _ string) bool { return h == host })
}

// PurgeFilesByPath removes the single cached entry for host+path.
// An empty host matches any host, so one object can be purged across sites.
func PurgeFilesByPath(cacheDir, host, path string) (int, error) {
	return PurgeFiles(cacheDir, func(h, p string) bool {
		return p == path && (host == "" || h == host)
	})
}

// PurgeFilesByPrefix removes cached entries for host whose path starts with
// prefix — i.e. a whole location subtree. An empty host matches any host.
func PurgeFilesByPrefix(cacheDir, host, prefix string) (int, error) {
	return PurgeFiles(cacheDir, func(h, p string) bool {
		return strings.HasPrefix(p, prefix) && (host == "" || h == host)
	})
}

// PurgeFilesAll removes every cached entry from cacheDir.
func PurgeFilesAll(cacheDir string) (int, error) {
	return PurgeFiles(cacheDir, func(_, _ string) bool { return true })
}
