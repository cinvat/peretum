package cmd

import (
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReloadProxy(t *testing.T) {
	t.Run("MissingFile", func(t *testing.T) {
		err := reloadProxy(filepath.Join(t.TempDir(), "nope.pid"))
		if err == nil || !strings.Contains(err.Error(), "read pid file") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("BadPid", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "p.pid")
		writeFile(t, f, "not-a-pid")
		if err := reloadProxy(f); err == nil || !strings.Contains(err.Error(), "parse pid") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("NoSuchProcess", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "p.pid")
		writeFile(t, f, "2147483647")
		if err := reloadProxy(f); err == nil || !strings.Contains(err.Error(), "signal pid") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("Success", func(t *testing.T) {
		// Spawn a sleeper we can legally signal without killing the test binary.
		child := exec.Command("sh", "-c", "trap '' HUP; sleep 30")
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = child.Process.Kill()
			_ = child.Wait()
		}()
		time.Sleep(100 * time.Millisecond)
		f := filepath.Join(t.TempDir(), "p.pid")
		writeFile(t, f, strconv.Itoa(child.Process.Pid))
		if err := reloadProxy(f); err != nil {
			t.Fatalf("reloadProxy: %v", err)
		}
	})
}
