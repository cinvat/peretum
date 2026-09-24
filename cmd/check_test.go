package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckConfig(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), runConfigYAML(":8081", filepath.Join(dir, "cache"), false))
		tdir := filepath.Join(dir, "targets")
		os.Mkdir(tdir, 0o700)
		writeFile(t, filepath.Join(tdir, "t.yaml"), "server_name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n")
		if err := checkConfig(filepath.Join(dir, "config.yaml"), tdir); err != nil {
			t.Fatalf("checkConfig: %v", err)
		}
	})
	t.Run("ProxyLoadError", func(t *testing.T) {
		if err := checkConfig("/nonexistent.yaml", t.TempDir()); err == nil || !strings.Contains(err.Error(), "proxy config") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("BadCacheSize", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), "listen: :8081\nmax_cache_size: bogus\n")
		if err := checkConfig(filepath.Join(dir, "config.yaml"), dir); err == nil || !strings.Contains(err.Error(), "max_cache_size") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("BadCacheAge", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), "listen: :8081\nmax_cache_size: 1MB\nmax_cache_age: nonsense\n")
		if err := checkConfig(filepath.Join(dir, "config.yaml"), dir); err == nil || !strings.Contains(err.Error(), "max_cache_age") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("BadResponseBodySize", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), "listen: :8081\nmax_response_body_size: bogus\n")
		if err := checkConfig(filepath.Join(dir, "config.yaml"), dir); err == nil || !strings.Contains(err.Error(), "max_response_body_size") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("ResponseBodySizeInfinityOk", func(t *testing.T) {
		dir := t.TempDir()
		tdir := filepath.Join(dir, "targets")
		os.Mkdir(tdir, 0o700)
		writeFile(t, filepath.Join(tdir, "t.yaml"), "server_name: tg\nlocations:\n  - path: /\n")
		writeFile(t, filepath.Join(dir, "config.yaml"), "listen: :8081\nmax_write_workers: -1\nmax_response_body_size: \"-1\"\n")
		if err := checkConfig(filepath.Join(dir, "config.yaml"), tdir); err != nil {
			t.Fatalf("checkConfig: %v", err)
		}
	})
	t.Run("BadWriteWorkers", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), "listen: :8081\nmax_write_workers: -2\n")
		if err := checkConfig(filepath.Join(dir, "config.yaml"), dir); err == nil || !strings.Contains(err.Error(), "max_write_workers") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("ListenersOk", func(t *testing.T) {
		dir := t.TempDir()
		tdir := filepath.Join(dir, "targets")
		os.Mkdir(tdir, 0o700)
		writeFile(t, filepath.Join(tdir, "t.yaml"), "server_name: tg\nlocations:\n  - path: /\n")
		writeFile(t, filepath.Join(dir, "config.yaml"), "listen: :8081\nlisteners:\n  - \":80 h2 http3\"\n  - \":443 ssl h2 h3\"\n")
		if err := checkConfig(filepath.Join(dir, "config.yaml"), tdir); err != nil {
			t.Fatalf("checkConfig: %v", err)
		}
	})
	t.Run("BadListeners", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), "listen: :8081\nlisteners:\n  - \":80 ssl\"\n  - \":443 h2c ssl\"\n")
		if err := checkConfig(filepath.Join(dir, "config.yaml"), dir); err == nil || !strings.Contains(err.Error(), "listeners") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("TargetsLoadError", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), runConfigYAML(":8081", filepath.Join(dir, "cache"), false))
		if err := checkConfig(filepath.Join(dir, "config.yaml"), filepath.Join(dir, "nonexistent-targets")); err == nil || !strings.Contains(err.Error(), "targets") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("BadUpstream", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), runConfigYAML(":8081", filepath.Join(dir, "cache"), false))
		tdir := filepath.Join(dir, "targets")
		os.Mkdir(tdir, 0o700)
		writeFile(t, filepath.Join(tdir, "t.yaml"), "server_name: tg\nupstreams:\n  - url: hostonly\nlocations:\n  - path: /\n")
		if err := checkConfig(filepath.Join(dir, "config.yaml"), tdir); err == nil || !strings.Contains(err.Error(), "target tg") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("BadCacheTTL", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), runConfigYAML(":8081", filepath.Join(dir, "cache"), false))
		tdir := filepath.Join(dir, "targets")
		os.Mkdir(tdir, 0o700)
		writeFile(t, filepath.Join(tdir, "t.yaml"), "server_name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n    cache_ttl: bogus\n")
		if err := checkConfig(filepath.Join(dir, "config.yaml"), tdir); err == nil || !strings.Contains(err.Error(), "cache_ttl") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("EmptySizeOk", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), "listen: :8081\n")
		tdir := filepath.Join(dir, "targets")
		os.Mkdir(tdir, 0o700)
		writeFile(t, filepath.Join(tdir, "t.yaml"), "server_name: tg\nlocations:\n  - path: /\n")
		if err := checkConfig(filepath.Join(dir, "config.yaml"), tdir); err != nil {
			t.Fatalf("checkConfig: %v", err)
		}
	})
}
