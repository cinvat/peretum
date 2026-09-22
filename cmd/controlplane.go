package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cinvat/peretum/internal/cluster"
	"github.com/spf13/cobra"
)

func newControlPlaneCommand() *cobra.Command {
	var listenAddr string
	var configDir string
	var dataDir string

	cmd := &cobra.Command{
		Use:   "controlplane",
		Short: "Run the CDN control plane for config distribution",
		Long: `Start the HTTP control plane that serves config snapshots to edge nodes.
Edges pull the full config from /sync on first launch and receive streaming
updates from their configured control plane address.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runControlPlane(cmd.Context(), listenAddr, configDir, dataDir)
		},
	}

	cmd.Flags().StringVar(&listenAddr, "listen", ":9001", "HTTP listen address")
	cmd.Flags().StringVar(&configDir, "config-dir", "config.d", "directory of target config files to watch")
	cmd.Flags().StringVar(&dataDir, "data-dir", "./controlplane-data", "directory for persistent data (snapshots, state)")

	return cmd
}

func runControlPlane(ctx context.Context, listenAddr, configDir, dataDir string) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("failed to create data dir %s: %w", dataDir, err)
	}

	// The control plane is by definition the leader with no HA peers unless
	// the caller provides a comma-separated control_plane list; here a single
	// node always serves snapshots.
	configStore := cluster.NewConfigVersionStore(10000)
	cph := cluster.NewControlPlaneHTTP("", configDir, configStore, nil)
	cph.SetLeader(true)

	// Load the initial snapshot and keep it in sync with config.d changes.
	if _, _, err := configStore.LoadTargets(configDir); err != nil {
		fmt.Fprintf(os.Stderr, "warn: no target configs loaded yet: %v\n", err)
	}

	// Keep the snapshot store in sync with config.d changes.
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
			}
			if _, _, err := configStore.LoadTargets(configDir); err != nil {
				fmt.Fprintf(os.Stderr, "warn: config reload failed: %v\n", err)
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", cph.Health)
	mux.HandleFunc("/health/leader", cph.HealthLeader)
	mux.HandleFunc("/sync", cph.SyncConfig)
	mux.HandleFunc("/sync/target", cph.SyncTarget)
	mux.HandleFunc("/stats", cph.Stats)

	server := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	go func() {
		if err := server.Serve(lis); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "control plane HTTP server error: %v\n", err)
		}
	}()

	fmt.Fprintf(os.Stderr, "Control plane listening on %s\n", listenAddr)
	fmt.Fprintf(os.Stderr, "Watching config directory: %s\n", configDir)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case <-ctx.Done():
	case s := <-sigCh:
		fmt.Fprintf(os.Stderr, "Received signal %v, shutting down...\n", s)
	}

	// Graceful shutdown
	shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return nil
}
