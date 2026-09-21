package cmd

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cinvat/peretum/internal/cluster"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func newControlPlaneCommand() *cobra.Command {
	var listenAddr string
	var configDir string
	var dataDir string

	cmd := &cobra.Command{
		Use:   "controlplane",
		Short: "Run the CDN control plane for config distribution",
		Long: `Start the gRPC control plane that serves config to edge nodes.
It watches the config directory for changes and streams updates to connected edges.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runControlPlane(cmd.Context(), listenAddr, configDir, dataDir)
		},
	}

	cmd.Flags().StringVar(&listenAddr, "listen", ":9001", "gRPC listen address")
	cmd.Flags().StringVar(&configDir, "config-dir", "config.d", "directory of target config files to watch")
	cmd.Flags().StringVar(&dataDir, "data-dir", "./controlplane-data", "directory for persistent data (snapshots, state)")

	return cmd
}

func runControlPlane(ctx context.Context, listenAddr, configDir, dataDir string) error {
	// Create control plane server
	cp := cluster.NewControlPlaneServer(configDir, dataDir)

	// Create gRPC server
	grpcServer := grpc.NewServer()
	// In production: pb.RegisterConfigStreamServiceServer(grpcServer, cp)

	// Start config file watcher
	go cp.WatchConfigChanges(ctx)

	// Start gRPC listener
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	// Start gRPC server
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			fmt.Fprintf(os.Stderr, "gRPC server error: %v\n", err)
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
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	grpcServer.GracefulStop()

	// Wait for actual shutdown
	select {
	case <-shutdownCtx.Done():
	case <-time.After(10 * time.Second):
		grpcServer.Stop()
	}

	return nil
}
