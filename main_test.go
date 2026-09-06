package main

import "testing"

// TestMainPanicsOnConfigError runs main() (which delegates to cmd.Execute) in
// an empty directory so every stage of the default run fails, and verifies the
// binary-entry convention of panicking with the error.
func TestMainPanicsOnConfigError(t *testing.T) {
	t.Chdir(t.TempDir()) // no config.yaml/config.d -> run fails, main panics
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected main() to panic")
		}
	}()
	main()
}
