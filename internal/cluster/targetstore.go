package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"

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

// HasTargetByHost reports whether a target exists for a hostname without
// copying the config body out of the store. Existence checks are on the request
// path for a store-backed router, so copying every config just to test a
// prefix would be wasteful at CDN scale.
func (ts *TargetStore) HasTargetByHost(ctx context.Context, hostname string) (bool, error) {
	_ = ctx
	_, closer, err := ts.db.Get([]byte(hostKeyPrefix + hostname))
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

// inHostPrefix reports whether key belongs to the hostname space rather than
// being the global config key or any other bookkeeping key.
func inHostPrefix(key []byte) bool {
	return bytes.HasPrefix(key, []byte(hostKeyPrefix))
}

// HasAnyTarget reports whether the store holds at least one target, without
// enumerating the keyspace. Startup uses this to tell a fresh store from a
// provisioned one; ListHosts would allocate one string per target to answer a
// yes/no question.
func (ts *TargetStore) HasAnyTarget(ctx context.Context) (bool, error) {
	_ = ctx
	iter, err := ts.db.NewIter(nil)
	if err != nil {
		return false, err
	}
	defer iter.Close()

	// The global config key sorts before the hostname prefix, so seek to the
	// prefix rather than starting at the first key in the store.
	return iter.SeekGE([]byte(hostKeyPrefix)) && inHostPrefix(iter.Key()), nil
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
	for valid := iter.SeekGE([]byte(hostKeyPrefix)); valid && inHostPrefix(iter.Key()); valid = iter.Next() {
		hosts = append(hosts, strings.TrimPrefix(string(iter.Key()), hostKeyPrefix))
	}
	return hosts, nil
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
