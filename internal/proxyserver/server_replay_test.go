package proxyserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cinvat/peretum/internal/cluster"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/router"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

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
