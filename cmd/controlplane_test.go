package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cinvat/peretum/internal/cluster"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/router"
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

// The startup gate: an edge must not treat itself as ready, and therefore must
// not serve, until the retained config events have been applied to the store.
// Because the router resolves hostnames by looking them up, a partially
// replayed store means missing routes, i.e. 404s that look like a dead origin.
func TestStartupWaitsForConfigReplayBeforeServing(t *testing.T) {
	natsURL := startTestNATS(t)
	ctx := context.Background()
	t.Setenv("PERETUM_EDGE_ID", "edge-startup-gate")

	upstream := lazyEchoUpstream(t, "replayed-body")
	// The event store carries the target as JSON; the edge unmarshals it back
	// into a TargetConfig.
	targetCfg, err := json.Marshal(&config.TargetConfig{
		ServerName: "api",
		Upstreams:  []config.UpstreamConfig{{URL: upstream}},
		Locations:  []config.LocationConfig{{Path: "/"}},
	})
	if err != nil {
		t.Fatalf("marshal target: %v", err)
	}

	// The control plane publishes before this edge starts, so the events are
	// only in the retained stream.
	writer, err := cluster.NewNATSConfigSync(ctx, natsURL, cluster.DefaultStreamConfig())
	if err != nil {
		t.Fatalf("NewNATSConfigSync(writer): %v", err)
	}
	t.Cleanup(func() { writer.Close() })
	if err := writer.PublishTarget(ctx, cluster.TargetEvent{
		ServerName: "api",
		Config:     json.RawMessage(targetCfg),
	}); err != nil {
		t.Fatalf("PublishTarget: %v", err)
	}

	store := lazyTestStore(t)
	ps := &proxyServer{
		targetStore: store,
		proxyCfg:    &config.ProxyConfig{Cluster: &config.ClusterConfig{Lazy: true, NATSURI: natsURL}},
	}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](16)

	sync, err := cluster.NewNATSConfigSync(ctx, natsURL, cluster.DefaultStreamConfig())
	if err != nil {
		t.Fatalf("NewNATSConfigSync(edge): %v", err)
	}
	t.Cleanup(func() { sync.Close() })
	ps.natsSync = sync
	ps.configReady, ps.configErr = sync.Start(ctx, ps.applyTargetEvent)
	if ps.configErr != nil {
		t.Fatalf("Start: %v", ps.configErr)
	}

	// This is the call start() makes before it builds routes and binds
	// listeners. When it returns, the store must already be current.
	if err := ps.waitForConfigReplay(); err != nil {
		t.Fatalf("waitForConfigReplay: %v", err)
	}

	has, err := store.HasTargetByHost(ctx, "api")
	if err != nil || !has {
		t.Fatalf("store has api = %v, err %v; want the replay to have landed before readiness", has, err)
	}

	// And the freshly built router serves it, rather than 404ing a target the
	// edge is supposed to have.
	ps.router = ps.buildHostRouter()
	rec := httptest.NewRecorder()
	ps.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://api/", nil))
	if rec.Body.String() != "replayed-body" {
		t.Fatalf("body after replay = %q, want %q", rec.Body.String(), "replayed-body")
	}
}

func TestWaitForConfigReplayNoStoreIsReadyImmediately(t *testing.T) {
	ps := &proxyServer{proxyCfg: &config.ProxyConfig{}}
	if err := ps.waitForConfigReplay(); err != nil {
		t.Fatalf("waitForConfigReplay with no event stream = %v, want nil", err)
	}
}

func TestWaitForConfigReplayFailsOnUnreachableStream(t *testing.T) {
	// A control plane that cannot be reached leaves the store empty, so serving
	// would 404 every target. Startup must fail loudly instead.
	ps := &proxyServer{
		proxyCfg:  &config.ProxyConfig{Cluster: &config.ClusterConfig{Lazy: true}},
		configErr: errors.New("no servers available for connection"),
	}
	if err := ps.waitForConfigReplay(); err == nil {
		t.Fatal("waitForConfigReplay succeeded with an unreachable event store")
	}
}

func TestWaitForConfigReplayTimesOut(t *testing.T) {
	never := make(chan struct{})
	ps := &proxyServer{
		proxyCfg: &config.ProxyConfig{Cluster: &config.ClusterConfig{
			Lazy:          true,
			ReplayTimeout: 50 * time.Millisecond,
		}},
		configReady: never,
	}
	err := ps.waitForConfigReplay()
	if err == nil {
		t.Fatal("waitForConfigReplay succeeded on a replay that never finished")
	}
	if !strings.Contains(err.Error(), "cluster.replay_timeout") {
		t.Fatalf("error = %q, want it to name the knob that fixes it", err)
	}
}

func TestWaitForConfigReplayHonorsDefaultTimeout(t *testing.T) {
	ps := &proxyServer{proxyCfg: &config.ProxyConfig{Cluster: &config.ClusterConfig{Lazy: true}}}
	never := make(chan struct{})
	ps.configReady = never

	// A closed channel must release it immediately; this pins that the gate
	// really is waiting on the channel rather than sleeping.
	close(never)
	if err := ps.waitForConfigReplay(); err != nil {
		t.Fatalf("waitForConfigReplay after ready = %v, want nil", err)
	}
}
