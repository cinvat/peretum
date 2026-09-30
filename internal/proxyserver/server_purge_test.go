package proxyserver

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/cinvat/peretum/internal/cluster"
	"github.com/cinvat/peretum/internal/config"
)

func seedEdgeCache(t *testing.T, ps *proxyServer) {
	t.Helper()
	ps.diskCache.SetResponseToCache("k1", "example.com", "/blog/a", 200, http.Header{}, []byte("a"))
	ps.diskCache.SetResponseToCache("k2", "example.com", "/blog/b", 200, http.Header{}, []byte("b"))
	ps.diskCache.SetResponseToCache("k3", "example.com", "/other", 200, http.Header{}, []byte("c"))
}

func TestHandlePurgeEventScopes(t *testing.T) {
	ps := &proxyServer{diskCache: newDiskCache(t, t.TempDir())}
	defer ps.diskCache.Close()
	seedEdgeCache(t, ps)

	if n := ps.handlePurgeEvent(context.Background(), cluster.PurgeEvent{Host: "example.com", Prefix: "/blog/"}); n != 2 {
		t.Fatalf("prefix purge dropped %d, want 2", n)
	}
	if n := ps.handlePurgeEvent(context.Background(), cluster.PurgeEvent{Host: "example.com", Path: "/other"}); n != 1 {
		t.Fatalf("path purge dropped %d, want 1", n)
	}
	if n := ps.handlePurgeEvent(context.Background(), cluster.PurgeEvent{Host: "example.com", Path: "/other"}); n != 0 {
		t.Fatalf("repeat purge dropped %d, want 0", n)
	}
}

func TestHandlePurgeEventNoCache(t *testing.T) {
	ps := &proxyServer{}
	if n := ps.handlePurgeEvent(context.Background(), cluster.PurgeEvent{All: true}); n != 0 {
		t.Fatalf("nil cache purge = %d, want 0", n)
	}
}

// End to end: a purge published like the REST API does reaches a subscribed
// edge and drops the entries from its live cache.
func TestPurgeBroadcastEndToEnd(t *testing.T) {
	natsURL := startTestNATS(t)
	ctx := context.Background()

	ps := &proxyServer{
		diskCache: newDiskCache(t, t.TempDir()),
		proxyCfg:  &config.ProxyConfig{Cluster: &config.ClusterConfig{NATSURI: natsURL}},
	}
	defer ps.diskCache.Close()
	seedEdgeCache(t, ps)

	sync, err := cluster.NewNATSConfigSync(ctx, natsURL, cluster.DefaultStreamConfig())
	if err != nil {
		t.Fatalf("NewNATSConfigSync: %v", err)
	}
	defer sync.Close()
	ps.natsSync = sync
	if _, err := sync.SubscribeCachePurge(ps.handlePurgeEvent); err != nil {
		t.Fatalf("SubscribeCachePurge: %v", err)
	}

	if err := cluster.PublishPurgeEvent(ctx, natsURL, cluster.PurgeEvent{All: true}); err != nil {
		t.Fatalf("PublishPurgeEvent: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		// Peek stats the entry file without mutating anything: it flips to
		// false only once the broadcast has landed and purged k1.
		if !ps.diskCache.Peek("k1", 0) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for broadcast purge to land")
}
