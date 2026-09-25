package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/cinvat/peretum/internal/cluster"
	"github.com/cinvat/peretum/internal/config"
	"github.com/fsnotify/fsnotify"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
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
			return runControlPlane(cmd.Context(), natsURLs, httpAddr, configDir)
		},
	}

	cmd.Flags().StringVar(&natsURLs, "nats", "nats://localhost:4222", "NATS JetStream URL(s) (comma-separated for cluster)")
	cmd.Flags().StringVar(&httpAddr, "http", ":9001", "HTTP listen address for health checks")
	cmd.Flags().StringVar(&configDir, "config-dir", "config.d", "directory of target config files to watch")

	return cmd
}

func runControlPlane(ctx context.Context, natsURLs, httpAddr, configDir string) error {
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		return fmt.Errorf("create config dir %s: %w", configDir, err)
	}

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

	streamCfg := cluster.DefaultStreamConfig()
	if err := cluster.EnsureStream(ctx, js, streamCfg); err != nil {
		return fmt.Errorf("create config stream: %w", err)
	}

	// Publish the initial state, then keep it in sync with the directory.
	pub := &configPublisher{
		js:     js,
		dir:    configDir,
		byName: make(map[string][]byte),
	}
	if err := pub.syncAll(ctx); err != nil {
		return fmt.Errorf("initial config publish: %w", err)
	}
	klog.Infof("published %d targets from %s", len(pub.byName), configDir)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create fsnotify watcher: %w", err)
	}
	defer watcher.Close()

	go watchConfigDir(ctx, watcher, configDir, pub)

	// Health endpoint for liveness/readiness probes.
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !nc.IsConnected() {
			http.Error(w, "nats disconnected", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Errorf("control plane HTTP server: %v", err)
		}
	}()

	klog.Infof("control plane ready: nats=%s http=%s config-dir=%s", natsURLs, httpAddr, configDir)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-ctx.Done():
	case s := <-sigCh:
		klog.Infof("received signal %v, shutting down", s)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}

// configPublisher tracks the last state published for each target so it can
// publish only what changed and emit deletions for targets that disappeared.
type configPublisher struct {
	mu     sync.Mutex
	js     jetstream.JetStream
	dir    string
	byName map[string][]byte // server name -> published YAML
}

// syncAll loads the directory and reconciles it with the published state.
func (p *configPublisher) syncAll(ctx context.Context) error {
	targets, err := config.LoadTargets(p.dir)
	if err != nil {
		if len(p.byName) == 0 {
			return err
		}
		// A transient read error should not wipe published state.
		return fmt.Errorf("load %s: %w", p.dir, err)
	}

	// The event payload is JSON so it matches the wire format the edges
	// unmarshal. Edges re-serialize to YAML for their on-disk store.
	next := make(map[string][]byte, len(targets))
	for i := range targets {
		data, err := json.Marshal(&targets[i])
		if err != nil {
			return fmt.Errorf("marshal target %s: %w", targets[i].ServerName, err)
		}
		for _, name := range parseServerNames(targets[i].ServerName) {
			next[name] = data
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for name, data := range next {
		if prev, ok := p.byName[name]; ok && string(prev) == string(data) {
			continue // unchanged
		}
		if err := p.publish(ctx, cluster.TargetEvent{
			ServerName: name,
			Config:     json.RawMessage(data),
			Timestamp:  time.Now().UTC(),
		}); err != nil {
			return err
		}
	}

	for name := range p.byName {
		if _, ok := next[name]; ok {
			continue
		}
		if err := p.publish(ctx, cluster.TargetEvent{
			ServerName: name,
			Deleted:    true,
			Timestamp:  time.Now().UTC(),
		}); err != nil {
			return err
		}
	}

	p.byName = next
	return nil
}

func (p *configPublisher) publish(ctx context.Context, event cluster.TargetEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event for %s: %w", event.ServerName, err)
	}
	if _, err := p.js.Publish(ctx, cluster.TargetSubject(event.ServerName), data); err != nil {
		return fmt.Errorf("publish %s: %w", event.ServerName, err)
	}
	klog.V(2).Infof("published %s (deleted=%t)", event.ServerName, event.Deleted)
	return nil
}

// watchConfigDir debounces filesystem events and reconciles the published
// state whenever a config file changes.
func watchConfigDir(ctx context.Context, watcher *fsnotify.Watcher, dir string, pub *configPublisher) {
	if err := watcher.Add(dir); err != nil {
		klog.Errorf("watch %s: %v", dir, err)
		return
	}

	const debounce = 500 * time.Millisecond
	timer := time.NewTimer(debounce)
	if !timer.Stop() {
		<-timer.C
	}
	dirty := false

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-watcher.Events:
			if !ok {
				return
			}
			switch filepath.Ext(ev.Name) {
			case ".yaml", ".yml":
				if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
					dirty = true
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(debounce)
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			klog.Errorf("fsnotify: %v", err)
		case <-timer.C:
			if !dirty {
				continue
			}
			dirty = false
			if err := pub.syncAll(ctx); err != nil {
				klog.Errorf("config sync: %v", err)
			} else {
				klog.Infof("config synced from %s (%d targets)", dir, len(pub.byName))
			}
		}
	}
}
