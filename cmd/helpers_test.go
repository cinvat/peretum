package cmd

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	disk "github.com/cinvat/peretum/internal/cache/disk"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free udp port: %v", err)
	}
	defer pc.Close()
	return pc.LocalAddr().(*net.UDPAddr).Port
}

func runConfigYAML(listen, cacheDir string, useJSON bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "listen: %q\ncache_dir: %q\nmax_cache_size: \"100MB\"\nmax_cache_age: \"24h\"\n", listen, cacheDir)
	if useJSON {
		fmt.Fprintf(&b, "json_log:\n  enabled: true\n  access_log: %q\n  error_log: %q\n  stdout: false\n",
			filepath.Join(cacheDir, "access.jsonl"), filepath.Join(cacheDir, "error.jsonl"))
	}
	return b.String()
}

func newDiskCache(t *testing.T, dir string) *disk.DiskCache {
	t.Helper()
	dc, err := disk.New(dir, 100<<20, 24*time.Hour)
	if err != nil {
		t.Fatalf("disk.New: %v", err)
	}
	return dc
}

func waitForServer(t *testing.T, url string) *http.Response {
	t.Helper()
	var resp *http.Response
	var err error
	for i := 0; i < 100; i++ {
		resp, err = http.Get(url)
		if err == nil {
			return resp
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server at %s never came up: %v", url, err)
	return nil
}
