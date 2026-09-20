package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRunProxy(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, targetsDir+"/t.yaml", "name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n")
	writeFile(t, cfgPath, runConfigYAML(fmt.Sprintf("127.0.0.1:%d", freePort(t)), filepath.Join(dir, "cache"), false))

	pidFile := filepath.Join(dir, "peretum.pid")
	startCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runProxy(startCtx, cfgPath, targetsDir, pidFile) }()

	var statErr error
	for i := 0; i < 100; i++ {
		if _, statErr = os.Stat(pidFile); statErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if statErr != nil {
		t.Fatalf("expected pid file written: %v", statErr)
	}
	data, err := os.ReadFile(pidFile)
	if err != nil || strings.TrimSpace(string(data)) != strconv.Itoa(os.Getpid()) {
		t.Fatalf("pid file content: %q (err=%v)", string(data), err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runProxy returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runProxy never returned")
	}
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Fatalf("expected pid file removed, got %v", err)
	}
}

func TestRunProxyPidWriteError(t *testing.T) {
	err := runProxy(context.Background(), "nope.yaml", "nope.d", filepath.Join(t.TempDir(), "missing", "peretum.pid"))
	if err == nil || !strings.Contains(err.Error(), "write pid file") {
		t.Fatalf("err = %v", err)
	}
}
