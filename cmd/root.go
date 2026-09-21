// Command cmd holds the peretum command-line interface (cobra-cli style:
// root command in root.go, one file per feature).
package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// rootCmd is the singleton command executed by main().
var rootCmd = newRootCommand()

// Execute runs the root command, returning an error on failure.
func Execute() error {
	return rootCmd.Execute()
}

// newRootCommand wires the peretum CLI:
//   - default  : run the proxy
//   - -t       : check configuration syntax, then exit
//   - -r       : reload the running proxy (sends SIGHUP to the pid in --pid-file)
func newRootCommand() *cobra.Command {
	var cfgPath string
	var targetsDir string
	var pidFile string

	cmd := &cobra.Command{
		Use:           "peretum",
		Short:         "cinvat peretum, api gateway, proxy cache.",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			switch {
			case checkSyntaxFlag(cmd):
				return checkConfig(cfgPath, targetsDir)
			case reloadFlag(cmd):
				return reloadProxy(pidFile)
			default:
				ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
				defer stop()
				return runProxy(ctx, cfgPath, targetsDir, pidFile)
			}
		},
	}

	cmd.PersistentFlags().StringVar(&cfgPath, "config", "config.yaml", "path to the proxy configuration file")
	cmd.PersistentFlags().StringVar(&targetsDir, "targets", "config.d", "directory of per-target configuration files")
	cmd.PersistentFlags().StringVar(&pidFile, "pid-file", "peretum.pid", "path to the pid file used by --reload")
	cmd.PersistentFlags().BoolP("test", "t", false, "check configuration syntax and exit")
	cmd.PersistentFlags().BoolP("reload", "r", false, "reload the running proxy (send SIGHUP)")

	// Add subcommands
	cmd.AddCommand(newControlPlaneCommand())

	return cmd
}

func checkSyntaxFlag(cmd *cobra.Command) bool {
	v, _ := cmd.Flags().GetBool("test")
	return v
}

func reloadFlag(cmd *cobra.Command) bool {
	v, _ := cmd.Flags().GetBool("reload")
	return v
}
