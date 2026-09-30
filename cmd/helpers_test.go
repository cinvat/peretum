package cmd

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func runConfigYAML(listen, cacheDir string, useJSON bool) string {
	if useJSON {
		return fmt.Sprintf(`{"listeners": ["%s"], "cache_dir": "%s"}`, listen, cacheDir)
	}
	return fmt.Sprintf("listeners:\n  - \"%s\"\ncache_dir: \"%s\"\n", listen, cacheDir)
}

func waitForServer(t *testing.T, url string) *http.Response {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			return resp
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", url)
	return nil
}
