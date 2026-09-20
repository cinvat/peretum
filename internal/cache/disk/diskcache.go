// Package diskcache provides a high-performance, concurrent-safe disk-backed
// HTTP response cache with sharded LRU eviction for CDN-scale workloads.
package diskcache

import (
	"bufio"
	"container/list"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"
)

const (
	// defaultMaxEntries is the initial capacity hint for the LRU map.
	defaultMaxEntries = 10000

	// defaultSweepInterval is how often the background goroutine scans
	// for and removes expired cache entries.
	defaultSweepInterval = 5 * time.Minute

	// defaultWriteWorkers is the maximum number of concurrent cache
	// write goroutines.
	defaultWriteWorkers = 8

	// shardCount is the number of LRU shards for lock striping.
	// Must be a power of 2.
	shardCount = 32

	// metaExt is the file extension for metadata sidecar files.
	metaExt = ".meta"

	// bodyExt is the file extension for response body files.
	bodyExt = ".body"
)

// entryMeta tracks per-entry metadata for LRU eviction and expiration.
type entryMeta struct {
	size      int64         // total bytes (meta + body) on disk
	lastUsed  time.Time     // last time this entry was read or written
	expiresAt time.Time     // when this entry should be evicted (zero = never)
	element   *list.Element // for O(1) LRU movement
}

// lruShard is a shard of the LRU cache with its own lock.
type lruShard struct {
	mu      sync.Mutex
	lru     map[string]*entryMeta
	lruList *list.List // for O(1) LRU operations
	size    int64      // total size in this shard
}

func newLRUShard() *lruShard {
	return &lruShard{
		lru:     make(map[string]*entryMeta, 256),
		lruList: list.New(),
	}
}

func (s *lruShard) get(key string) (*entryMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.lru[key]; ok {
		s.lruList.MoveToFront(e.element)
		return e, true
	}
	return nil, false
}

func (s *lruShard) put(key string, size int64, expiresAt time.Time) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.lru[key]; ok {
		delta := size - e.size
		s.size += delta
		e.size = size
		e.lastUsed = time.Now()
		e.expiresAt = expiresAt
		s.lruList.MoveToFront(e.element)
		return delta
	}

	e := &entryMeta{
		size:      size,
		lastUsed:  time.Now(),
		expiresAt: expiresAt,
	}
	e.element = s.lruList.PushFront(key)
	s.lru[key] = e
	s.size += size
	return size
}

func (s *lruShard) remove(key string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.lru[key]; ok {
		s.size -= e.size
		s.lruList.Remove(e.element)
		delete(s.lru, key)
		return e.size, true
	}
	return 0, false
}

func (s *lruShard) evict() (string, int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	elem := s.lruList.Back()
	if elem == nil {
		return "", 0, false
	}
	key := elem.Value.(string)
	e := s.lru[key]
	s.size -= e.size
	s.lruList.Remove(elem)
	delete(s.lru, key)
	return key, e.size, true
}

func (s *lruShard) findExpired() ([]string, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []string
	var totalSize int64
	now := time.Now()
	for k, e := range s.lru {
		if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
			expired = append(expired, k)
			totalSize += e.size
		}
	}
	for _, k := range expired {
		e := s.lru[k]
		s.size -= e.size
		s.lruList.Remove(e.element)
		delete(s.lru, k)
	}
	return expired, totalSize
}

func (s *lruShard) sizeBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

func (s *lruShard) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.lru)
}

// DiskCache is a concurrent-safe, sharded LRU-evicting disk cache for HTTP responses.
//
// Each cached response is stored as two files under CacheDir:
//   - <key>.meta — binary-encoded status code and headers
//   - <key>.body — raw response body bytes
//
// Keys are flat SHA-256 hex strings (see CacheKey), so no nested directories
// are created under CacheDir itself.
//
// The cache enforces a maximum total size (MaxSize) by evicting the
// least-recently-used entry when a write would exceed the limit. Entries
// are also expired by a background sweep if MaxAge > 0.
//
// DiskCache is safe for concurrent use by multiple goroutines.
type DiskCache struct {
	// CacheDir is the filesystem directory where cached response files
	// are stored. Set at construction time; do not modify.
	CacheDir string

	// MaxSize is the maximum total size in bytes of all cached entries.
	// When a write would exceed this limit, the LRU entry is evicted.
	MaxSize int64

	// MaxAge is the duration after which a cached entry is considered
	// stale. Set to 0 to disable time-based expiration.
	MaxAge time.Duration

	// CurrentSize tracks the total bytes of all entries currently in
	// the cache. Updated atomically for lock-free reads.
	CurrentSize atomic.Int64

	shards []*lruShard

	// dirs tracks which directories have already been created,
	// avoiding redundant os.MkdirAll syscalls.
	dirs   map[string]struct{}
	dirsMu sync.RWMutex

	// writeSem is a buffered channel that limits the number of
	// concurrent cache write goroutines.
	writeSem chan struct{}

	// stopSweep is closed to signal the background sweep goroutine to exit.
	stopSweep chan struct{}

	// Stats
	stats struct {
		hits        atomic.Int64
		misses      atomic.Int64
		evictions   atomic.Int64
		expirations atomic.Int64
	}
}

// New creates a new DiskCache rooted at cacheDir with the given size and
// age limits. It creates cacheDir if it does not exist and starts a
// background goroutine to periodically sweep expired entries.
//
// Parameters:
//   - cacheDir: filesystem path for cached files (created if missing)
//   - maxSize: maximum total cache size in bytes (0 = unlimited)
//   - maxAge: maximum entry lifetime (0 = no expiration)
//
// The caller must call Close when done to stop the background sweep.
func New(cacheDir string, maxSize int64, maxAge time.Duration) (*DiskCache, error) {
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, err
	}

	d := &DiskCache{
		CacheDir:  cacheDir,
		MaxSize:   maxSize,
		MaxAge:    maxAge,
		shards:    make([]*lruShard, shardCount),
		dirs:      make(map[string]struct{}),
		writeSem:  make(chan struct{}, defaultWriteWorkers),
		stopSweep: make(chan struct{}),
	}

	for i := range d.shards {
		d.shards[i] = newLRUShard()
	}

	go d.sweepLoop(defaultSweepInterval)

	return d, nil
}

func (d *DiskCache) shard(key string) *lruShard {
	// Use first 2 bytes of hash for sharding (consistent with CachePath)
	h := key[:2]
	var sum uint32
	for i := 0; i < len(h); i++ {
		sum = sum*31 + uint32(h[i])
	}
	return d.shards[sum&(shardCount-1)]
}

// Close stops the background sweep goroutine. The cache can no longer
// perform automatic expiration after Close is called, but read/write
// operations remain functional.
func (d *DiskCache) Close() {
	close(d.stopSweep)
}

// touch updates or inserts LRU metadata for the given key. If the key
// already exists, its size and lastUsed time are updated; otherwise a
// new entry is created. Size tracking is maintained atomically via
// CurrentSize.
func (d *DiskCache) touch(key string, size int64) {
	var expiresAt time.Time
	if d.MaxAge > 0 {
		expiresAt = time.Now().Add(d.MaxAge)
	}
	// put returns the size delta (new - old), or size if new entry
	delta := d.shard(key).put(key, size, expiresAt)
	d.CurrentSize.Add(delta)
}

// remove deletes the LRU tracking entry for the given key and decrements
// CurrentSize accordingly. It does not remove files from disk.
func (d *DiskCache) remove(key string) {
	if size, ok := d.shard(key).remove(key); ok {
		d.CurrentSize.Add(-size)
	}
}

// evict removes the least-recently-used entry from the LRU map and
// decrements CurrentSize. This only removes the tracking entry; disk
// files are not deleted by evict.
func (d *DiskCache) evict() bool {
	for _, s := range d.shards {
		if key, size, ok := s.evict(); ok {
			d.CurrentSize.Add(-size)
			d.stats.evictions.Add(1)
			_ = os.Remove(filepath.Join(d.CacheDir, key[:2], key[2:]+metaExt))
			_ = os.Remove(filepath.Join(d.CacheDir, key[:2], key[2:]+bodyExt))
			klog.V(2).Infof("evicted LRU entry: %s", key)
			return true
		}
	}
	return false
}

// ensureSpace ensures there is enough space for a new entry of the given size.
// Returns true if space is available or made available.
func (d *DiskCache) ensureSpace(size int64) bool {
	if d.MaxSize == 0 {
		return true
	}
	for d.CurrentSize.Load()+size > d.MaxSize {
		if !d.evict() {
			return false
		}
	}
	return true
}

// sweepLoop runs on the given interval, calling sweep to remove
// expired entries until stopSweep is closed.
func (d *DiskCache) sweepLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-d.stopSweep:
			return
		case <-ticker.C:
			d.sweep()
		}
	}
}

// sweep scans the LRU map for expired entries and removes their
// disk files and tracking metadata.
func (d *DiskCache) sweep() {
	for _, s := range d.shards {
		expired, size := s.findExpired()
		for _, k := range expired {
			_ = os.Remove(filepath.Join(d.CacheDir, k[:2], k[2:]+metaExt))
			_ = os.Remove(filepath.Join(d.CacheDir, k[2:]+bodyExt))
			d.stats.expirations.Add(1)
			klog.V(2).Infof("swept expired cache entry: %s", k)
		}
		if size > 0 {
			d.CurrentSize.Add(-size)
		}
	}
}

// ensureDir creates the given directory if it does not exist. The result
// is cached to avoid redundant os.MkdirAll calls on subsequent writes,
// but the cache is only trusted when the directory actually exists, so a
// directory removed out from under the cache (e.g. an ops rmtree while
// the proxy is running) is transparently recreated.
func (d *DiskCache) ensureDir(dir string) {
	d.dirsMu.RLock()
	_, ok := d.dirs[dir]
	d.dirsMu.RUnlock()
	if ok && dirExists(dir) {
		return
	}
	os.MkdirAll(dir, 0755)
	d.dirsMu.Lock()
	d.dirs[dir] = struct{}{}
	d.dirsMu.Unlock()
}

func dirExists(dir string) bool {
	info, err := os.Stat(dir)
	return err == nil && info.IsDir()
}

// Stats returns cache statistics.
func (d *DiskCache) Stats() (hits, misses, evictions, expirations int64, size int64) {
	return d.stats.hits.Load(), d.stats.misses.Load(), d.stats.evictions.Load(), d.stats.expirations.Load(), d.CurrentSize.Load()
}

// RecordHit records a cache hit.
func (d *DiskCache) RecordHit() {
	d.stats.hits.Add(1)
}

// RecordMiss records a cache miss.
func (d *DiskCache) RecordMiss() {
	d.stats.misses.Add(1)
}

// PurgeByHost removes all cache entries for the given host.
// Returns the number of entries purged.
func (d *DiskCache) PurgeByHost(host string) int {
	purged := 0
	for _, s := range d.shards {
		s.mu.Lock()
		var toRemove []string
		for k := range s.lru {
			metaPath := filepath.Join(d.CacheDir, k[:2], k[2:]+metaExt)
			metaFile, err := os.Open(metaPath)
			if err != nil {
				continue
			}
			_, entryHost, _, _, err := readMetadata(bufio.NewReader(metaFile))
			metaFile.Close()
			if err != nil {
				continue
			}
			if entryHost == host {
				toRemove = append(toRemove, k)
			}
		}
		for _, k := range toRemove {
			s.size -= s.lru[k].size
			s.lruList.Remove(s.lru[k].element)
			delete(s.lru, k)
			purged++
		}
		s.mu.Unlock()
	}
	d.CurrentSize.Add(-int64(purged))
	return purged
}

// PurgeByPath removes all cache entries matching the exact path.
// Returns the number of entries purged.
func (d *DiskCache) PurgeByPath(path string) int {
	return d.purgeByPathPrefix(path, false)
}

// PurgeByPrefix removes all cache entries where the path starts with the given prefix.
// Returns the number of entries purged.
func (d *DiskCache) PurgeByPrefix(prefix string) int {
	return d.purgeByPathPrefix(prefix, true)
}

func (d *DiskCache) purgeByPathPrefix(path string, isPrefix bool) int {
	purged := 0
	for _, s := range d.shards {
		s.mu.Lock()
		var toRemove []string
		for k := range s.lru {
			metaPath := filepath.Join(d.CacheDir, k[:2], k[2:]+metaExt)
			metaFile, err := os.Open(metaPath)
			if err != nil {
				continue
			}
			_, _, entryPath, _, err := readMetadata(bufio.NewReader(metaFile))
			metaFile.Close()
			if err != nil {
				continue
			}
			match := false
			if isPrefix {
				match = strings.HasPrefix(entryPath, path)
			} else {
				match = entryPath == path
			}
			if match {
				toRemove = append(toRemove, k)
			}
		}
		for _, k := range toRemove {
			s.size -= s.lru[k].size
			s.lruList.Remove(s.lru[k].element)
			delete(s.lru, k)
			purged++
		}
		s.mu.Unlock()
		if len(toRemove) > 0 {
			d.CurrentSize.Add(-int64(len(toRemove)))
		}
	}
	return purged
}

// PurgeAll removes all cache entries.
// Returns the number of entries purged.
func (d *DiskCache) PurgeAll() int {
	purged := 0
	for _, s := range d.shards {
		s.mu.Lock()
		for k := range s.lru {
			_ = os.Remove(filepath.Join(d.CacheDir, k[:2], k[2:]+metaExt))
			_ = os.Remove(filepath.Join(d.CacheDir, k[:2], k[2:]+bodyExt))
			purged++
		}
		s.lru = make(map[string]*entryMeta, 256)
		s.lruList.Init()
		s.size = 0
		s.mu.Unlock()
	}
	d.CurrentSize.Store(0)
	klog.Infof("purged all cache entries: %d removed", purged)
	return purged
}
