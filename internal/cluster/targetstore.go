package cluster

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
)

const (
	targetKeyPrefix  = "t/"
	versionKeyPrefix = "v/"
	globalKey        = "g"
)

// TargetStore is the local persistent store on an edge node. Target configs
// live on disk (backed by Pebble) instead of RAM, which lets a single edge
// node hold 10M+ cold configs without eating memory. Compiled handlers are
// materialized on first request and held in a bounded LRU on top of this
// store (see router.LazyHandler).
//
// Key layout:
//
//	g/            -> global config JSON
//	t/<name>      -> raw YAML of the target config
//	v/<name>      -> version string for the target
//
// The separate version key allows a fast key-only scan to rebuild the
// name -> version index after a restart.
type TargetStore struct {
	db *pebble.DB
	mu sync.Mutex
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
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.db.Close()
}

// PutGlobal stores the global config blob.
func (ts *TargetStore) PutGlobal(data []byte) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.db.Set([]byte(globalKey), data, pebble.NoSync)
}

// GetGlobal returns the stored global config and whether it exists.
func (ts *TargetStore) GetGlobal() ([]byte, bool, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.get(globalKey)
}

// PutTarget stores a target config blob and its version atomically.
func (ts *TargetStore) PutTarget(name, version string, data []byte) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	b := ts.db.NewBatch()
	defer b.Close()
	if err := b.Set([]byte(targetKeyPrefix+name), data, pebble.NoSync); err != nil {
		return err
	}
	if err := b.Set([]byte(versionKeyPrefix+name), []byte(version), pebble.NoSync); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}

// GetTarget returns the version and YAML blob for a target.
func (ts *TargetStore) GetTarget(name string) (version string, data []byte, ok bool, err error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	data, ok, err = ts.get(targetKeyPrefix + name)
	if err != nil || !ok {
		return "", data, ok, err
	}
	ver, _, verr := ts.get(versionKeyPrefix + name)
	if verr != nil {
		return "", data, ok, verr
	}
	return strings.TrimSpace(string(ver)), data, ok, nil
}

// DeleteTarget removes a target and its version.
func (ts *TargetStore) DeleteTarget(name string) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	b := ts.db.NewBatch()
	defer b.Close()
	if err := b.Delete([]byte(targetKeyPrefix+name), pebble.NoSync); err != nil {
		return err
	}
	if err := b.Delete([]byte(versionKeyPrefix+name), pebble.NoSync); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}

// ListTargets returns every stored target name mapped to its version. It
// walks only the version keys, so restart index rebuilds stay fast even with
// millions of configs.
func (ts *TargetStore) ListTargets() (map[string]string, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	names := make(map[string]string)
	iter, err := ts.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	prefix := []byte(versionKeyPrefix)
	for valid := iter.SeekGE(prefix); valid; valid = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		name := string(key[len(prefix):])
		names[name] = strings.TrimSpace(string(iter.Value()))
	}
	return names, nil
}

// TargetCount returns the number of stored targets.
func (ts *TargetStore) TargetCount() (int, error) {
	names, err := ts.ListTargets()
	if err != nil {
		return 0, err
	}
	return len(names), nil
}

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
