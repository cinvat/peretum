package cmd

import (
	"context"
	"fmt"
	"os"
	"strconv"
)

// runProxy writes a pid file (so --reload can target this instance), runs the
// proxy, and removes the pid file on exit.
func runProxy(ctx context.Context, cfgPath, targetsDir, pidFile string) error {
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		return fmt.Errorf("write pid file %s: %w", pidFile, err)
	}
	defer os.Remove(pidFile)
	return run(ctx, cfgPath, targetsDir)
}
