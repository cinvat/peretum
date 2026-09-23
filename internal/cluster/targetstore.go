package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble"
)

const (
	hostKeyPrefix    = "h/" // hostname -> target YAML
	versionKeyPrefix = "v/" // hostname -> version
	globalKey        = "g"
)

// ErrNotFound is returned when a key is not found in the store.
var ErrNotFound = errors.New("not found")

// TargetStore is the local persistent store on an edge node. Target configs
// live on disk (backed by Pebble) instead of RAM, which lets a single edge
// node hold 10M+ cold configs without eating memory. Compiled handlers are
// materialized on first request and held in a bounded LRU on top of this
// store (see router.LazyHandler).
//
// Key layout:
//
//	g/            -> global config JSON
//	h/<hostname>  -> raw YAML of the target config (keyed by hostname)
//	v/<hostname>  -> version string for the target
//
// The hostname is the key, enabling O(1) lookup by Host header.
// A single target can be stored under multiple hostnames.
type TargetStore struct {
	db *pebble.DB
}

// OpenTargetStore opens (or creates) the Pebble-backed store at dir.
func OpenTargetStore(dir string) (*TargetStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create target store dir: %w", err)
	}
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open target store: %w", err)
	}
	return &TargetStore{db: db}, nil
}

// Close flushes and closes the store.
func (ts *TargetStore) Close() error {
	return ts.db.Close()
}

// PutGlobal stores the global config blob.
func (ts *TargetStore) PutGlobal(ctx context.Context, data []byte) error {
	_ = ctx
	return ts.db.Set([]byte(globalKey), data, pebble.NoSync)
}

// GetGlobal returns the stored global config and whether it exists.
func (ts *TargetStore) GetGlobal(ctx context.Context) ([]byte, bool, error) {
	_ = ctx // Pebble doesn't support context cancellation yet
	return ts.get(globalKey)
}

// PutTargetByHost stores a target config blob under a hostname key.
// A target can be stored under multiple hostnames.
func (ts *TargetStore) PutTargetByHost(ctx context.Context, hostname, version string, data []byte) error {
	_ = ctx
	b := ts.db.NewBatch()
	defer b.Close()
	if err := b.Set([]byte(hostKeyPrefix+hostname), data, pebble.NoSync); err != nil {
		return err
	}
	if err := b.Set([]byte(versionKeyPrefix+hostname), []byte(version), pebble.NoSync); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}

// GetTargetByHost returns the version and YAML blob for a target by hostname.
func (ts *TargetStore) GetTargetByHost(ctx context.Context, hostname string) (version string, data []byte, ok bool, err error) {
	_ = ctx

	data, ok, err = ts.get(hostKeyPrefix + hostname)
	if err != nil || !ok {
		return "", data, ok, err
	}
	ver, _, verr := ts.get(versionKeyPrefix + hostname)
	if verr != nil {
		return "", data, ok, verr
	}
	return strings.TrimSpace(string(ver)), data, ok, nil
}

// DeleteTargetByHost removes a target by hostname.
func (ts *TargetStore) DeleteTargetByHost(ctx context.Context, hostname string) error {
	_ = ctx
	b := ts.db.NewBatch()
	defer b.Close()
	if err := b.Delete([]byte(hostKeyPrefix+hostname), pebble.NoSync); err != nil {
		return err
	}
	if err := b.Delete([]byte(versionKeyPrefix+hostname), pebble.NoSync); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}

// ListHosts returns every stored hostname mapped to its version.
// Walks only the version keys for fast restart index rebuild.
func (ts *TargetStore) ListHosts(ctx context.Context) (map[string]string, error) {
	_ = ctx

	iter, err := ts.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	hosts := make(map[string]string)
	prefix := []byte(versionKeyPrefix)
	for valid := iter.SeekGE(prefix); valid; valid = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		host := string(key[len(prefix):])
		hosts[host] = strings.TrimSpace(string(iter.Value()))
	}
	return hosts, nil
}

// HostCount returns the number of stored hostnames.
func (ts *TargetStore) HostCount(ctx context.Context) (int, error) {
	_ = ctx

	iter, err := ts.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	count := 0
	prefix := []byte(versionKeyPrefix)
	for valid := iter.SeekGE(prefix); valid; valid = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		count++
	}
	return count, nil
}

// get retrieves a value by key. The returned slice is a copy safe to use after
// the closer is closed.
func (ts *TargetStore) get(key string) ([]byte, bool, error) {
	value, closer, err := ts.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	out := make([]byte, len(value))
	copy(out, value)
	return out, true, nil
}
