package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestCheckSyntaxFlag(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().BoolP("test", "t", false, "")
	if checkSyntaxFlag(cmd) {
		t.Fatal("expected false when flag unset")
	}
	if err := cmd.Flags().Set("test", "true"); err != nil {
		t.Fatal(err)
	}
	if !checkSyntaxFlag(cmd) {
		t.Fatal("expected true when flag set")
	}
}

func TestReloadFlag(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().BoolP("reload", "r", false, "")
	if reloadFlag(cmd) {
		t.Fatal("expected false when flag unset")
	}
	if err := cmd.Flags().Set("reload", "true"); err != nil {
		t.Fatal(err)
	}
	if !reloadFlag(cmd) {
		t.Fatal("expected true when flag set")
	}
}

func TestExecute(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), runConfigYAML(":8081", filepath.Join(dir, "cache"), false))
		rootCmd.SetArgs([]string{"-t", "--config", filepath.Join(dir, "config.yaml"), "--targets", dir})
		if err := Execute(); err != nil {
			t.Fatalf("Execute: %v", err)
		}
	})
	t.Run("Error", func(t *testing.T) {
		rootCmd.SetArgs([]string{"-t", "--config", "/nonexistent.yaml", "--targets", t.TempDir()})
		if err := Execute(); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestRootCommand(t *testing.T) {
	t.Run("CheckSyntaxArg", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "config.yaml"), runConfigYAML(":8081", filepath.Join(dir, "cache"), false))
		cmd := newRootCommand()
		cmd.SetArgs([]string{"-t", "--config", filepath.Join(dir, "config.yaml"), "--targets", dir})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("Execute: %v", err)
		}
	})
	t.Run("CheckSyntaxBadConfig", func(t *testing.T) {
		cmd := newRootCommand()
		cmd.SetArgs([]string{"-t", "--config", "/nonexistent.yaml", "--targets", t.TempDir()})
		if err := cmd.Execute(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("ReloadArg", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "p.pid")
		writeFile(t, f, "not-a-pid")
		cmd := newRootCommand()
		cmd.SetArgs([]string{"-r", "--pid-file", f})
		err := cmd.Execute()
		if err == nil || !strings.Contains(err.Error(), "parse pid") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("ReloadShorthandLong", func(t *testing.T) {
		cmd := newRootCommand()
		cmd.SetArgs([]string{"--reload", "--pid-file", filepath.Join(t.TempDir(), "nope.pid")})
		err := cmd.Execute()
		if err == nil || !strings.Contains(err.Error(), "read pid file") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("RunDefault", func(t *testing.T) {
		dir := t.TempDir()
		tdir := filepath.Join(dir, "targets")
		os.Mkdir(tdir, 0o700)
		writeFile(t, filepath.Join(tdir, "t.yaml"), "name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n")
		cfgp := filepath.Join(dir, "config.yaml")
		writeFile(t, cfgp, runConfigYAML(fmt.Sprintf("127.0.0.1:%d", freePort(t)), filepath.Join(dir, "cache"), false))
		ctx, cancel := context.WithCancel(context.Background())
		cmd := newRootCommand()
		cmd.SetContext(ctx)
		cmd.SetArgs([]string{"--config", cfgp, "--targets", tdir, "--pid-file", filepath.Join(dir, "p.pid")})
		done := make(chan error, 1)
		go func() { done <- cmd.Execute() }()
		var statErr error
		for i := 0; i < 100; i++ {
			if _, statErr = os.Stat(filepath.Join(dir, "p.pid")); statErr == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		cancel()
		if statErr != nil {
			t.Fatalf("expected pid file after start: %v", statErr)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Execute returned %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Execute never returned after cancel")
		}
	})
}
