package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/cinvat/peretum/internal/cluster"
	"github.com/fsnotify/fsnotify"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
)

func newControlPlaneCommand() *cobra.Command {
	var natsURLs string
	var httpAddr string
	var configDir string
	var dataDir string

	cmd := &cobra.Command{
		Use:   "controlplane",
		Short: "Run the CDN control plane for config distribution via NATS JetStream",
		Long: `Start the NATS JetStream control plane that watches config.d/ and publishes
config updates to NATS JetStream. Edges subscribe to config.target.updated.* and
config.target.deleted.* for real-time updates, and can request full snapshots via
config.snapshot.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runControlPlane(cmd.Context(), natsURLs, httpAddr, configDir, dataDir)
		},
	}

	cmd.Flags().StringVar(&natsURLs, "nats", "nats://localhost:4222", "NATS JetStream URL(s) (comma-separated for cluster)")
	cmd.Flags().StringVar(&httpAddr, "http", ":9001", "HTTP listen address for snapshot API")
	cmd.Flags().StringVar(&configDir, "config-dir", "config.d", "directory of target config files to watch")
	cmd.Flags().StringVar(&dataDir, "data-dir", "./controlplane-data", "directory for persistent data (snapshots, state)")

	return cmd
}

func runControlPlane(ctx context.Context, natsURLs, httpAddr, configDir, dataDir string) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("failed to create data dir %s: %w", dataDir, err)
	}

	if err := os.MkdirAll(configDir, 0o755); err != nil {
		return fmt.Errorf("failed to create config dir %s: %w", configDir, err)
	}

	// Connect to NATS
	nc, err := nats.Connect(natsURLs,
		nats.ReconnectWait(5*time.Second),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("create jetstream context: %w", err)
	}

	// Create or update the stream
	streamCfg := cluster.DefaultStreamConfig()
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              streamCfg.Name,
		Subjects:          streamCfg.Subjects,
		Retention:         jetstream.LimitsPolicy,
		MaxMsgs:           -1,
		MaxAge:            streamCfg.MaxAge,
		MaxBytes:          streamCfg.MaxBytes,
		MaxMsgsPerSubject: int64(streamCfg.MaxMsgsPerSubject),
		Discard:           streamCfg.DiscardPolicy,
		Storage:           streamCfg.StorageType,
		Replicas:          streamCfg.Replicas,
	})
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	// Load initial snapshot into JetStream
	configStore := cluster.NewConfigVersionStore(10000)
	if _, _, err := configStore.LoadTargets(configDir); err != nil {
		fmt.Fprintf(os.Stderr, "warn: no target configs loaded yet: %v\n", err)
	} else {
		// Publish initial targets to JetStream
		allTargets := configStore.GetAllTargets()
		for name, target := range allTargets {
			data, _ := json.Marshal(target.Target)
			js.Publish(ctx, "config.target.updated."+name, data)
		}
		fmt.Fprintf(os.Stderr, "published %d initial targets to JetStream\n", len(allTargets))
	}

	// Watch config directory for changes
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		fmt.Fprintf(os.Stderr, "warn: failed to create fsnotify watcher: %v; falling back to polling\n", err)
		go pollConfigChanges(ctx, js, configDir)
	} else {
		go watchConfigChanges(ctx, watcher, configDir, js)
	}

	// Start HTTP server for snapshot requests and health checks
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		// Full snapshot request
		allTargets := configStore.GetAllTargets()
		resp := map[string]interface{}{
			"version": time.Now().Unix(),
			"targets": make(map[string]interface{}),
		}
		targetsMap := resp["targets"].(map[string]interface{})
		for name, target := range allTargets {
			targetsMap[name] = map[string]interface{}{
				"target":    target.Target,
				"version":   target.Version,
				"loaded_at": target.LoadedAt.Unix(),
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/hot-targets", func(w http.ResponseWriter, r *http.Request) {
		// Return list of hot targets (those with high access count)
		stats := configStore.GetStats()
		hotTargets := stats["hot_targets"].([]string)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"server_names": hotTargets,
			"version":      time.Now().Unix(),
		})
	})
	mux.HandleFunc("/sync/target", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ServerName string `json:"server_name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if req.ServerName == "" {
			http.Error(w, "missing server_name", http.StatusBadRequest)
			return
		}

		target, ok := configStore.GetTarget(req.ServerName)
		if !ok {
			http.Error(w, "target not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"target":    target.Target,
			"version":   target.Version,
			"loaded_at": target.LoadedAt.Unix(),
		})
	})

	server := &http.Server{
		Addr:         httpAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "control plane HTTP server error: %v\n", err)
		}
	}()

	fmt.Fprintf(os.Stderr, "Control plane listening on NATS: %s\n", natsURLs)
	fmt.Fprintf(os.Stderr, "Control plane HTTP on %s\n", httpAddr)
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

	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}

	return nil
}

func watchConfigChanges(ctx context.Context, watcher *fsnotify.Watcher, configDir string, js jetstream.JetStream) {
	if err := watcher.Add(configDir); err != nil {
		fmt.Fprintf(os.Stderr, "warn: failed to watch config dir: %v\n", err)
		return
	}

	debounce := time.NewTimer(500 * time.Millisecond)
	if !debounce.Stop() {
		<-debounce.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
				if filepath.Ext(event.Name) == ".yaml" || filepath.Ext(event.Name) == ".yml" {
					debounce.Reset(500 * time.Millisecond)
				}
			}
		case <-watcher.Errors:
			// Log error but continue watching
		case <-debounce.C:
			// Reload config and publish changes
			// This is simplified - in production you'd want to diff and only publish changes
			fmt.Fprintf(os.Stderr, "config changed, publishing updates...\n")
			// TODO: implement diff and publish individual target updates
		}
	}
}

func pollConfigChanges(ctx context.Context, js jetstream.JetStream, configDir string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// TODO: implement periodic polling
		}
	}
}
