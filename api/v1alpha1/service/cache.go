package service

import (
	"context"

	diskcache "github.com/cinvat/peretum/internal/cache/disk"
	"github.com/cinvat/peretum/internal/cluster"
)

// defaultCacheDir is the disk-cache root purged by the cache service.
var defaultCacheDir = "./cache"

// defaultNATSURI, when set, switches purges from local disk to a broadcast:
// the event is published on the cache.purge subject and every edge purges
// its own live cache. Empty means standalone mode (purge local files).
var defaultNATSURI = ""

// SetCacheDir overrides the disk-cache root used by the cache service,
// e.g. from the API server's --cache-dir flag. It must point at the same
// directory the proxy serves from.
func SetCacheDir(dir string) {
	defaultCacheDir = dir
}

// SetNATSURI switches the cache service to cluster mode: purges are
// published to every edge instead of applied to the local disk.
func SetNATSURI(uri string) {
	defaultNATSURI = uri
}

// PurgeResult describes what a purge did. Cluster mode publishes to all
// edges (Published) without waiting for per-edge counts; standalone mode
// purges local files and reports how many entries were removed (Purged).
type PurgeResult struct {
	Purged    int  `json:"purged"`
	Published bool `json:"published,omitempty"`
}

// PurgeCache removes cached entries from the disk cache. Exactly one scope
// selects the entries:
//
//	host only            — every entry for the site
//	host + path          — a single object
//	host + prefix        — a location subtree (path prefix)
//	all=true             — the whole cache (host/path/prefix must be empty)
func PurgeCache(host, path, prefix string, all bool) (PurgeResult, error) {
	event := cluster.PurgeEvent{Host: host, Path: path, Prefix: prefix, All: all}
	if err := event.Validate(); err != nil {
		return PurgeResult{}, err
	}
	if defaultNATSURI != "" {
		if err := cluster.PublishPurgeEvent(context.Background(), defaultNATSURI, event); err != nil {
			return PurgeResult{}, err
		}
		return PurgeResult{Published: true}, nil
	}
	switch {
	case all:
		n, err := diskcache.PurgeFilesAll(defaultCacheDir)
		return PurgeResult{Purged: n}, err
	case path != "":
		n, err := diskcache.PurgeFilesByPath(defaultCacheDir, host, path)
		return PurgeResult{Purged: n}, err
	case prefix != "":
		n, err := diskcache.PurgeFilesByPrefix(defaultCacheDir, host, prefix)
		return PurgeResult{Purged: n}, err
	default:
		n, err := diskcache.PurgeFilesByHost(defaultCacheDir, host)
		return PurgeResult{Purged: n}, err
	}
}
