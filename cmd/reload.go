package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// reloadProxy sends SIGHUP to the pid stored in pidFile, triggering the running
// proxy's config reload handler.
func reloadProxy(pidFile string) error {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return fmt.Errorf("read pid file %s: %w", pidFile, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return fmt.Errorf("parse pid from %s: %w", pidFile, err)
	}
	if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
		return fmt.Errorf("signal pid %d: %w", pid, err)
	}
	return nil
}
