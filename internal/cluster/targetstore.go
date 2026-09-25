package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
)

const (
	hostKeyPrefix = "h/" // hostname -> target YAML
	globalKey     = "g"  // global config JSON
)

// TargetStore is the local persistent store on an edge node. Target configs
// live on disk (backed by Pebble) instead of RAM, which lets a single edge
// node hold 10M+ cold configs without eating memory. Compiled handlers are
// materialized on first request and held in a bounded LRU on top of this
// store (see router.LazyHandler).
//
// Key layout:
//
//	g            -> global config JSON
//	h/<hostname> -> raw YAML of the target config (keyed by hostname)
//
// The hostname is the key, enabling O(1) lookup by Host header. A single
// target can be stored under multiple hostnames. There are deliberately no
// version or hash keys: the control plane publishes the current state of each
// target, and the newest event wins.
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
func (ts *TargetStore) PutTargetByHost(ctx context.Context, hostname string, data []byte) error {
	_ = ctx
	return ts.db.Set([]byte(hostKeyPrefix+hostname), data, pebble.NoSync)
}

// GetTargetByHost returns the YAML blob for a target by hostname.
func (ts *TargetStore) GetTargetByHost(ctx context.Context, hostname string) (data []byte, ok bool, err error) {
	_ = ctx
	return ts.get(hostKeyPrefix + hostname)
}

// DeleteTargetByHost removes a target by hostname.
func (ts *TargetStore) DeleteTargetByHost(ctx context.Context, hostname string) error {
	_ = ctx
	return ts.db.Delete([]byte(hostKeyPrefix+hostname), pebble.NoSync)
}

// ListHosts returns every stored hostname.
func (ts *TargetStore) ListHosts(ctx context.Context) ([]string, error) {
	_ = ctx
	iter, err := ts.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	hosts := make([]string, 0, 1024)
	prefix := []byte(hostKeyPrefix)
	for valid := iter.SeekGE(prefix); valid; valid = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		hosts = append(hosts, string(key[len(prefix):]))
	}
	return hosts, nil
}

// HostCount returns the number of stored hostnames.
func (ts *TargetStore) HostCount(ctx context.Context) (int, error) {
	hosts, err := ts.ListHosts(ctx)
	if err != nil {
		return 0, err
	}
	return len(hosts), nil
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
