package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble"
	"gopkg.in/yaml.v3"
)

const (
	targetKeyPrefix  = "t/"
	versionKeyPrefix = "v/"
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
//	t/<name>      -> raw YAML of the target config
//	v/<name>      -> version string for the target
//
// The separate version key allows a fast key-only scan to rebuild the
// name -> version index after a restart.
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

// PutTarget stores a target config blob and its version atomically.
func (ts *TargetStore) PutTarget(ctx context.Context, name, version string, data []byte) error {
	_ = ctx
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
func (ts *TargetStore) GetTarget(ctx context.Context, name string) (version string, data []byte, ok bool, err error) {
	_ = ctx

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

// GetTargetListen returns the listen field for a target without full unmarshal.
// Returns empty string if not set or on error.
func (ts *TargetStore) GetTargetListen(ctx context.Context, name string) (string, error) {
	_ = ctx
	_, yamlData, ok, err := ts.GetTarget(ctx, name)
	if err != nil || !ok {
		return "", err
	}
	// Quick YAML parse for just the listen field
	var cfg struct {
		Listen string `yaml:"listen"`
	}
	if err := yaml.Unmarshal(yamlData, &cfg); err != nil {
		return "", err
	}
	return cfg.Listen, nil
}

// DeleteTarget removes a target and its version.
func (ts *TargetStore) DeleteTarget(ctx context.Context, name string) error {
	_ = ctx
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
func (ts *TargetStore) ListTargets(ctx context.Context) (map[string]string, error) {
	_ = ctx

	iter, err := ts.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	names := make(map[string]string)
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

// TargetCount returns the number of stored targets without allocating the full map.
func (ts *TargetStore) TargetCount(ctx context.Context) (int, error) {
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
