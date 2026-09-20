package diskcache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

const (
	// metaVersion is the current metadata format version.
	// v1: [status][numHeaders][headers...]
	// v2: [version=2][status][host][path][numHeaders][headers...]
	metaVersion = 2
)

// writeMetadata encodes the HTTP status code, host, path, and headers into a compact
// binary format written to w. The format is:
//
//	[uint8 version] [uint16 status] [uint32 hostLen] [host bytes] [uint32 pathLen] [path bytes] [uint32 numHeaders] [header...]
//
// Each header is encoded as:
//
//	[uint32 keyLen] [key bytes] [uint32 valLen] [val bytes]
//
// Headers with multiple values are joined with ", " before encoding.
func writeMetadata(w io.Writer, statusCode int, host, path string, headers http.Header) error {
	if err := binary.Write(w, binary.LittleEndian, uint8(metaVersion)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint16(statusCode)); err != nil {
		return err
	}

	if err := writeString(w, host); err != nil {
		return err
	}
	if err := writeString(w, path); err != nil {
		return err
	}

	if err := binary.Write(w, binary.LittleEndian, uint32(len(headers))); err != nil {
		return err
	}

	for k, vals := range headers {
		if err := writeString(w, k); err != nil {
			return err
		}
		joined := strings.Join(vals, ", ")
		if err := writeString(w, joined); err != nil {
			return err
		}
	}
	return nil
}

// writeString writes a length-prefixed string to w in binary format:
//
//	[uint32 len] [bytes]
func writeString(w io.Writer, s string) error {
	b := []byte(s)
	if err := binary.Write(w, binary.LittleEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

// readMetadata decodes a binary-encoded status code, host, path, and headers from r.
// It supports both v1 (legacy) and v2 (current) formats. Returns the status code, host, path, parsed
// headers, or an error if the data is malformed.
func readMetadata(r *bufio.Reader) (int, string, string, http.Header, error) {
	// Peek first byte to detect version
	firstByte, err := r.Peek(1)
	if err != nil {
		return 0, "", "", nil, err
	}

	version := firstByte[0]

	// v1 format: [uint16 status][uint32 numHeaders][headers...] (no version byte)
	// v2 format: [uint8 version][uint16 status][host][path][uint32 numHeaders][headers...]
	if version == 1 || version == 2 {
		// Consume version byte. Peek above guaranteed the byte is already
		// buffered, so Discard cannot fail.
		r.Discard(1)
		return readMetadataV2(r)
	}

	// Legacy v1 format (no version byte) - first 2 bytes are status code
	return readMetadataV1(r)
}

func readMetadataV1(r *bufio.Reader) (int, string, string, http.Header, error) {
	var statusCode uint16
	if err := binary.Read(r, binary.LittleEndian, &statusCode); err != nil {
		return 0, "", "", nil, err
	}

	var numHeaders uint32
	if err := binary.Read(r, binary.LittleEndian, &numHeaders); err != nil {
		return 0, "", "", nil, err
	}

	headers := make(http.Header, numHeaders)
	for i := uint32(0); i < numHeaders; i++ {
		key, err := readString(r)
		if err != nil {
			return 0, "", "", nil, err
		}
		val, err := readString(r)
		if err != nil {
			return 0, "", "", nil, err
		}
		headers[key] = []string{val}
	}

	return int(statusCode), "", "", headers, nil
}

func readMetadataV2(r *bufio.Reader) (int, string, string, http.Header, error) {
	var statusCode uint16
	if err := binary.Read(r, binary.LittleEndian, &statusCode); err != nil {
		return 0, "", "", nil, err
	}

	host, err := readString(r)
	if err != nil {
		return 0, "", "", nil, err
	}

	path, err := readString(r)
	if err != nil {
		return 0, "", "", nil, err
	}

	var numHeaders uint32
	if err := binary.Read(r, binary.LittleEndian, &numHeaders); err != nil {
		return 0, "", "", nil, err
	}

	headers := make(http.Header, numHeaders)
	for i := uint32(0); i < numHeaders; i++ {
		key, err := readString(r)
		if err != nil {
			return 0, "", "", nil, err
		}
		val, err := readString(r)
		if err != nil {
			return 0, "", "", nil, err
		}
		headers[key] = []string{val}
	}

	return int(statusCode), host, path, headers, nil
}

// readString reads a length-prefixed string from r. It is the inverse
// of writeString.
func readString(r *bufio.Reader) (string, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// tempFile is the minimal interface writeAtomic needs from its temporary
// file. It is satisfied by *os.File and by test fakes that inject I/O
// failures.
type tempFile interface {
	Name() string
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// createTempFile is a test seam that defaults to os.CreateTemp. Tests swap
// it for a failing implementation to exercise writeAtomic's error paths.
var createTempFile = func(dir, pattern string) (tempFile, error) {
	return os.CreateTemp(dir, pattern)
}

// writeAtomic writes data to path atomically by first writing to a
// temporary file in the same directory, calling fsync, then renaming
// the temp file to the target path. This prevents partial writes from
// corrupting cached entries if the process crashes mid-write.
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := createTempFile(dir, ".cache-tmp-")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, path)
}

// writeMetadataFn is a test seam that defaults to writeMetadata. Tests swap
// it for a failing implementation to exercise the metadata-write error path
// in SetResponseToCache deterministically.
var writeMetadataFn = writeMetadata

// SetResponseToCache stores an HTTP response in the disk cache under the
// given key. Only 2xx responses with a non-empty body are cached.
//
// The response is stored as two files:
//   - <CacheDir>/<key>.meta — binary-encoded status code, host, path, and headers
//   - <CacheDir>/<key>.body — raw response body bytes
//
// If writing would cause CurrentSize to exceed MaxSize, the LRU entry
// is evicted first. Files are written atomically using writeAtomic.
//
// This method is safe for concurrent use.
func (d *DiskCache) SetResponseToCache(key, host, path string, statusCode int, headers http.Header, body []byte) error {
	if statusCode < 200 || statusCode >= 300 || len(body) == 0 {
		return nil
	}

	bodyPath := CachePath(d.CacheDir, key, bodyExt)
	metaPath := CachePath(d.CacheDir, key, metaExt)

	d.ensureDir(filepath.Dir(bodyPath))

	var metaBuf bytes.Buffer
	if err := writeMetadataFn(&metaBuf, statusCode, host, path, headers); err != nil {
		return err
	}

	totalSize := int64(metaBuf.Len() + len(body))

	if !d.ensureSpace(totalSize) {
		return fmt.Errorf("cache full, cannot store entry")
	}

	if err := writeAtomic(metaPath, metaBuf.Bytes()); err != nil {
		return err
	}
	if err := writeAtomic(bodyPath, body); err != nil {
		os.Remove(metaPath)
		return err
	}

	d.touch(key, totalSize)
	klog.Infof("cached: %s (%d bytes, status %d)", key, totalSize, statusCode)
	return nil
}

// openMetaFile is a test seam that defaults to os.Open. Tests swap it to
// force the metadata-open failure path in StreamCachedResponse.
var openMetaFile = os.Open

// StreamCachedResponse serves a cached HTTP response directly to w. It
// reads the .meta file to reconstruct status and headers, adds
// X-Cache: HIT and X-Cache-Age diagnostic headers, then streams the
// .body file directly to the response writer.
//
// Returns nil on cache hit (response fully written to w), or an error
// on cache miss, expiration, or I/O failure.
//
// If the entry is expired (per MaxAge), both the .meta and .body files
// are deleted and the LRU entry is removed.
//
// This method is safe for concurrent use.
func (d *DiskCache) StreamCachedResponse(w http.ResponseWriter, key string) error {
	return d.streamCachedResponse(w, key, 0)
}

// StreamCachedResponseTTL is like StreamCachedResponse but enforces a
// per-entry TTL instead of the cache's global MaxAge. A TTL of 0 falls back
// to d.MaxAge (the behavior of StreamCachedResponse).
func (d *DiskCache) StreamCachedResponseTTL(w http.ResponseWriter, key string, ttl time.Duration) error {
	return d.streamCachedResponse(w, key, ttl)
}

// Peek reports whether a fresh cached entry exists for key, mirroring the
// freshness (TTL/MaxAge) check used by streamCachedResponse but without
// writing anything or expiring the entry. It lets callers set up response
// headers before committing to a cache hit.
func (d *DiskCache) Peek(key string, ttl time.Duration) bool {
	info, err := os.Stat(CachePath(d.CacheDir, key, metaExt))
	if err != nil {
		return false
	}
	age := d.MaxAge
	if ttl > 0 {
		age = ttl
	}
	return age <= 0 || time.Since(info.ModTime()) <= age
}

func (d *DiskCache) streamCachedResponse(w http.ResponseWriter, key string, ttl time.Duration) error {
	metaPath := CachePath(d.CacheDir, key, metaExt)
	bodyPath := CachePath(d.CacheDir, key, bodyExt)

	info, err := os.Stat(metaPath)
	if err != nil {
		return err
	}

	age := d.MaxAge
	if ttl > 0 {
		age = ttl
	}
	if age > 0 && time.Since(info.ModTime()) > age {
		os.Remove(metaPath)
		os.Remove(bodyPath)
		d.remove(key)
		return io.ErrUnexpectedEOF
	}

	metaFile, err := openMetaFile(metaPath)
	if err != nil {
		return err
	}
	defer metaFile.Close()

	statusCode, _, _, headers, err := readMetadata(bufio.NewReader(metaFile))
	if err != nil {
		return err
	}

	for k, vals := range headers {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}

	w.Header().Set("X-Cache", "HIT")
	w.Header().Set("X-Cache-Age", time.Since(info.ModTime()).String())

	w.WriteHeader(statusCode)

	bodyFile, err := os.Open(bodyPath)
	if err != nil {
		return err
	}
	defer bodyFile.Close()

	_, err = io.Copy(w, bodyFile)
	if err != nil {
		return err
	}

	d.touch(key, info.Size())
	return nil
}

// ParseCacheStatus reads the full body from an http.Response and returns
// the status code, headers, and body bytes. This is a convenience helper
// for extracting response data before passing it to SetResponseToCache.
func ParseCacheStatus(r *http.Response) (int, http.Header, []byte, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return 0, nil, nil, err
	}
	r.Body.Close()

	return r.StatusCode, r.Header, body, nil
}
