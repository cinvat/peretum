package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cinvat/peretum/internal/cluster"
	"github.com/cinvat/peretum/internal/config"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

// collector records target events received from the config event store so a
// test can wait for a specific server name to appear, change or disappear.
type collector struct {
	mu     sync.Mutex
	events map[string]cluster.TargetEvent
	seen   []string
}

func newCollector() *collector {
	return &collector{events: make(map[string]cluster.TargetEvent)}
}

func (c *collector) handle(_ context.Context, ev *cluster.TargetEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events[ev.ServerName] = *ev
	c.seen = append(c.seen, ev.ServerName)
	return nil
}

func (c *collector) get(name string) (cluster.TargetEvent, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ev, ok := c.events[name]
	return ev, ok
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}

// waitFor polls until cond returns true or the deadline expires.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// startTestNATS boots an in-process NATS server with JetStream enabled. It is a
// test harness only; production talks to an external NATS deployment.
func startTestNATS(t *testing.T) string {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      freePort(t),
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create NATS server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS server not ready")
	}
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
}

const apiTargetYAML = `server_name: api
upstreams:
  - url: http://api:1
locations:
  - path: /
`

const webTargetYAML = `server_name: web
upstreams:
  - url: http://web:1
locations:
  - path: /
`

// startControlPlane runs the control plane against the given config dir.
func startControlPlane(t *testing.T, natsURL, cfgDir string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	httpAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	done := make(chan error, 1)
	go func() {
		done <- runControlPlane(ctx, natsURL, httpAddr, cfgDir)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("control plane did not shut down")
		}
	})

	resp := waitForServer(t, "http://"+httpAddr+"/health")
	resp.Body.Close()
}

// startConsumer attaches an edge consumer to the config event store. The
// consumer replays the retained state first, then follows live updates.
func startConsumer(t *testing.T, natsURL string) *collector {
	t.Helper()
	// Use a distinct durable name so each test gets a full replay.
	t.Setenv("PERETUM_EDGE_ID", t.Name())
	sync, err := cluster.NewNATSConfigSync(context.Background(), natsURL, cluster.DefaultStreamConfig())
	if err != nil {
		t.Fatalf("NewNATSConfigSync: %v", err)
	}
	t.Cleanup(func() { sync.Close() })

	c := newCollector()
	if err := sync.Consume(context.Background(), c.handle); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	return c
}

func TestControlPlanePublishesAndTracksConfigDir(t *testing.T) {
	natsURL := startTestNATS(t)

	dir := t.TempDir()
	cfgDir := filepath.Join(dir, "config.d")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(cfgDir, "api.yaml"), apiTargetYAML)

	startControlPlane(t, natsURL, cfgDir)

	// A newly attached edge replays the whole stream, so it sees the existing
	// target without any snapshot endpoint.
	c := startConsumer(t, natsURL)
	waitFor(t, "api target", func() bool {
		ev, ok := c.get("api")
		return ok && !ev.Deleted
	})
	ev, _ := c.get("api")
	var got config.TargetConfig
	if err := json.Unmarshal(ev.Config, &got); err != nil {
		t.Fatalf("event config is not a TargetConfig: %v", err)
	}
	if got.ServerName != "api" {
		t.Fatalf("server_name = %q, want api", got.ServerName)
	}

	// Adding a file publishes the new target.
	writeFile(t, filepath.Join(cfgDir, "web.yaml"), webTargetYAML)
	waitFor(t, "web target", func() bool {
		ev, ok := c.get("web")
		return ok && !ev.Deleted
	})

	// Editing a file republishes that target only.
	before := c.count()
	writeFile(t, filepath.Join(cfgDir, "web.yaml"), webTargetYAML+"  - path: /health\n")
	waitFor(t, "web target update", func() bool {
		ev, ok := c.get("web")
		if !ok {
			return false
		}
		var updated config.TargetConfig
		if err := json.Unmarshal(ev.Config, &updated); err != nil {
			return false
		}
		return len(updated.Locations) == 2
	})
	if got := c.count(); got != before+1 {
		t.Fatalf("publish count = %d, want %d: only the changed target should be republished", got, before+1)
	}

	// Removing a file publishes a deletion so edges drop the target.
	if err := os.Remove(filepath.Join(cfgDir, "web.yaml")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "web deletion", func() bool {
		ev, ok := c.get("web")
		return ok && ev.Deleted
	})
}

func TestControlPlaneHandlesMultiHostnameTarget(t *testing.T) {
	natsURL := startTestNATS(t)

	cfgDir := t.TempDir()
	writeFile(t, filepath.Join(cfgDir, "multi.yaml"), `server_name: "a.example.com, b.example.com"
upstreams:
  - url: http://a:1
locations:
  - path: /
`)
	startControlPlane(t, natsURL, cfgDir)

	c := startConsumer(t, natsURL)
	// One event per hostname, so both are independently addressable in the
	// event store and both resolve to the same config on the edge.
	waitFor(t, "first hostname", func() bool {
		ev, ok := c.get("a.example.com")
		return ok && !ev.Deleted
	})
	waitFor(t, "second hostname", func() bool {
		ev, ok := c.get("b.example.com")
		return ok && !ev.Deleted
	})
}

func TestControlPlaneHealthEndpoint(t *testing.T) {
	natsURL := startTestNATS(t)

	cfgDir := t.TempDir()
	writeFile(t, filepath.Join(cfgDir, "api.yaml"), apiTargetYAML)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	httpAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	done := make(chan error, 1)
	go func() {
		done <- runControlPlane(ctx, natsURL, httpAddr, cfgDir)
	}()
	defer func() {
		cancel()
		<-done
	}()

	resp := waitForServer(t, "http://"+httpAddr+"/health")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want 200", resp.StatusCode)
	}
}
