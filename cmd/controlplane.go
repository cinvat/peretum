package cmd

import (
	"github.com/cinvat/peretum/internal/controlplane"
	"github.com/spf13/cobra"
)

func newControlPlaneCommand() *cobra.Command {
	var natsURLs string
	var httpAddr string
	var configDir string

	cmd := &cobra.Command{
		Use:   "controlplane",
		Short: "Run the CDN control plane for config distribution via NATS JetStream",
		Long: `Start the NATS JetStream control plane.

The control plane is the single source of truth for target configuration. It
watches a directory of target YAML files and publishes the current state of
each target to a NATS JetStream event store.

The stream is configured as an event store: it keeps exactly one message per
target subject, so the newest event for a target is always its current state.
Edge nodes replay the stream from the beginning to obtain the full
configuration, then follow live updates. There is no separate snapshot API and
no configuration versioning.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return controlplane.Run(cmd.Context(), natsURLs, httpAddr, configDir)
		},
	}

	cmd.Flags().StringVar(&natsURLs, "nats", "nats://localhost:4222", "NATS JetStream URL(s) (comma-separated for cluster)")
	cmd.Flags().StringVar(&httpAddr, "http", ":9001", "HTTP listen address for health checks")
	cmd.Flags().StringVar(&configDir, "config-dir", "config.d", "directory of target config files to watch")

	return cmd
}
