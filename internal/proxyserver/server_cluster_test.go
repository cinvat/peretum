package proxyserver

import (
	"context"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cinvat/peretum/internal/cluster"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/router"
)

func TestLazyModeServesFromStore(t *testing.T) {
	ctx := context.Background()
	store := lazyTestStore(t)
	url := lazyEchoUpstream(t, "lazy-upstream")

	// Target config with listen field that becomes the hostname key
	targetYAML := fmt.Sprintf("server_name: svc.lazy\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n", url)
	if err := store.PutTargetByHost(ctx, "svc.lazy", []byte(targetYAML)); err != nil {
		t.Fatalf("PutTargetByHost: %v", err)
	}

	ps := &proxyServer{targetStore: store}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](16)
	ps.router = ps.buildHostRouter()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://svc.lazy/", nil)
	ps.router.ServeHTTP(rec, req)
	if rec.Body.String() != "lazy-upstream" {
		t.Fatalf("lazy serve body = %q", rec.Body.String())
	}
}

func TestEnsureLazyStoreSeedsFromLocalTargets(t *testing.T) {
	ctx := context.Background()
	store := lazyTestStore(t)
	ps := &proxyServer{
		targetStore: store,
		targets: []config.TargetConfig{{
			ServerName: "t1",
			Upstreams:  []config.UpstreamConfig{{URL: "http://x:1"}},
			Locations:  []config.LocationConfig{{Path: "/"}},
		}},
	}
	if err := ps.ensureLazyStore(); err != nil {
		t.Fatalf("ensureLazyStore: %v", err)
	}
	names, _ := store.ListHosts(ctx)
	if len(names) != 1 || names[0] != "t1" {
		t.Fatalf("seeded store = %v, want [t1]", names)
	}

	// Already provisioned: a second run must leave the store alone even when
	// the local targets slice is now empty.
	ps.targets = nil
	if err := ps.ensureLazyStore(); err != nil {
		t.Fatalf("ensureLazyStore (provisioned): %v", err)
	}
	names, _ = store.ListHosts(ctx)
	if len(names) != 1 {
		t.Fatalf("store changed after re-provision = %v", names)
	}
}

func TestEnsureLazyStoreSkipsWithoutStore(t *testing.T) {
	ps := &proxyServer{}
	if err := ps.ensureLazyStore(); err != nil {
		t.Fatalf("ensureLazyStore with nil store = %v", err)
	}
}

func TestLazyControlPlaneUpdateAndDelete(t *testing.T) {
	ctx := context.Background()
	store := lazyTestStore(t)
	url := lazyEchoUpstream(t, "updated-body")

	ps := &proxyServer{targetStore: store}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](16)
	ps.router = ps.buildHostRouter()

	if err := ps.applyTargetUpdate("svc.upd", &config.TargetConfig{
		ServerName: "svc.upd",
		Upstreams:  []config.UpstreamConfig{{URL: url}},
		Locations:  []config.LocationConfig{{Path: "/"}},
	}); err != nil {
		t.Fatalf("applyTargetUpdate: %v", err)
	}

	data, ok, err := store.GetTargetByHost(ctx, "svc.upd")
	if err != nil || !ok {
		t.Fatalf("store after update = ok %v err %v", ok, err)
	}
	if !strings.Contains(string(data), "svc.upd") {
		t.Fatalf("stored config = %q, want it to mention svc.upd", data)
	}
	// The store is the routing table, so an update must not add a per-host entry.
	if got := len(ps.lazyRouter.GetTargets()); got != 0 {
		t.Fatalf("routing table holds %d entries after update, want 0 (store-backed)", got)
	}

	rec := httptest.NewRecorder()
	ps.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc.upd/", nil))
	if rec.Body.String() != "updated-body" {
		t.Fatalf("updated target body = %q", rec.Body.String())
	}

	ps.applyTargetDelete("svc.upd")
	if _, ok, _ := store.GetTargetByHost(ctx, "svc.upd"); ok {
		t.Fatal("target still in store after delete")
	}
	// Deleting the row is enough to stop routing it; nothing removes an entry
	// because nothing added one.
	rec = httptest.NewRecorder()
	ps.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc.upd/", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status after delete = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestApplyTargetEventUpsert(t *testing.T) {
	ctx := context.Background()
	store := lazyTestStore(t)
	url := lazyEchoUpstream(t, "event-upstream")

	ps := &proxyServer{targetStore: store}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](16)
	ps.router = ps.buildHostRouter()

	tc := config.TargetConfig{
		ServerName: "svc.event",
		Upstreams:  []config.UpstreamConfig{{URL: url}},
		Locations:  []config.LocationConfig{{Path: "/"}},
	}
	// Events carry JSON, because that is what the control plane publishes.
	raw, err := json.Marshal(tc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// A non-delete event must take the upsert path.
	if err := ps.applyTargetEvent(ctx, &cluster.TargetEvent{
		ServerName: "svc.event",
		Config:     raw,
	}); err != nil {
		t.Fatalf("applyTargetEvent upsert: %v", err)
	}

	if _, ok, _ := store.GetTargetByHost(ctx, "svc.event"); !ok {
		t.Fatal("target not persisted after upsert event")
	}

	rec := httptest.NewRecorder()
	ps.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc.event/", nil))
	if rec.Body.String() != "event-upstream" {
		t.Fatalf("served body = %q", rec.Body.String())
	}
}

func TestApplyTargetEventDelete(t *testing.T) {
	ctx := context.Background()
	store := lazyTestStore(t)

	ps := &proxyServer{targetStore: store}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](16)
	ps.router = ps.buildHostRouter()

	if err := ps.applyTargetUpdate("svc.evdel", &config.TargetConfig{
		ServerName: "svc.evdel",
		Upstreams:  []config.UpstreamConfig{{URL: "http://127.0.0.1:1"}},
		Locations:  []config.LocationConfig{{Path: "/"}},
	}); err != nil {
		t.Fatalf("applyTargetUpdate: %v", err)
	}

	// A delete event must remove the target even though it carries no config.
	if err := ps.applyTargetEvent(ctx, &cluster.TargetEvent{
		ServerName: "svc.evdel",
		Deleted:    true,
	}); err != nil {
		t.Fatalf("applyTargetEvent delete: %v", err)
	}

	if _, ok, _ := store.GetTargetByHost(ctx, "svc.evdel"); ok {
		t.Fatal("target still in store after delete event")
	}
}

func TestApplyTargetEventErrors(t *testing.T) {
	store := lazyTestStore(t)
	ps := &proxyServer{targetStore: store}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](16)
	ps.router = ps.buildHostRouter()

	if err := ps.applyTargetEvent(context.Background(), nil); err == nil {
		t.Error("nil event should be rejected")
	}

	if err := ps.applyTargetEvent(context.Background(), &cluster.TargetEvent{
		ServerName: "bad",
		Config:     []byte("{not json"),
	}); err == nil {
		t.Error("malformed config should be rejected")
	}
}

func TestApplyTargetEventFallsBackToEventServerName(t *testing.T) {
	// A config that omits server_name must still be stored under the name the
	// event was published with, otherwise the edge would drop the target.
	store := lazyTestStore(t)
	ps := &proxyServer{targetStore: store}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](16)
	ps.router = ps.buildHostRouter()

	err := ps.applyTargetEvent(context.Background(), &cluster.TargetEvent{
		ServerName: "fallback.host",
		Config:     []byte(`{"upstreams":[{"url":"http://127.0.0.1:1"}],"locations":[{"path":"/"}]}`),
	})
	if err != nil {
		t.Fatalf("applyTargetEvent: %v", err)
	}

	if _, ok, _ := store.GetTargetByHost(context.Background(), "fallback.host"); !ok {
		t.Fatal("target was not stored under the event server name")
	}
}

func TestApplyTargetUpdateMultipleServerNames(t *testing.T) {
	// A target claiming several hostnames must be persisted and routed under
	// every one of them.
	ctx := context.Background()
	store := lazyTestStore(t)
	url := lazyEchoUpstream(t, "multi")

	ps := &proxyServer{targetStore: store}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](16)
	ps.router = ps.buildHostRouter()

	if err := ps.applyTargetUpdate("a.multi", &config.TargetConfig{
		ServerName: "a.multi,b.multi",
		Upstreams:  []config.UpstreamConfig{{URL: url}},
		Locations:  []config.LocationConfig{{Path: "/"}},
	}); err != nil {
		t.Fatalf("applyTargetUpdate: %v", err)
	}

	for _, host := range []string{"a.multi", "b.multi"} {
		if _, ok, _ := store.GetTargetByHost(ctx, host); !ok {
			t.Fatalf("host %q missing from store", host)
		}
		rec := httptest.NewRecorder()
		ps.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://"+host+"/", nil))
		if rec.Body.String() != "multi" {
			t.Fatalf("host %q served %q", host, rec.Body.String())
		}
	}
}

func TestApplyTargetUpdateNoServerName(t *testing.T) {
	ps := &proxyServer{targetStore: lazyTestStore(t)}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](16)
	ps.router = ps.buildHostRouter()

	// Neither the config nor the event name yields a hostname.
	if err := ps.applyTargetUpdate("", &config.TargetConfig{}); err == nil {
		t.Error("target with no server_name should be rejected")
	}
}

func TestLazyRouterScalesWithoutRoutingTable(t *testing.T) {
	ctx := context.Background()
	store := lazyTestStore(t)
	url := lazyEchoUpstream(t, "scaled-body")

	// More targets than the LRU can hold, so a bounded cache is the only way to
	// serve them all.
	const targets = 2000
	const lruCapacity = 32
	for i := 0; i < targets; i++ {
		host := fmt.Sprintf("h%04d.example.com", i)
		data, err := yaml.Marshal(&config.TargetConfig{
			ServerName: host,
			Upstreams:  []config.UpstreamConfig{{URL: url}},
			Locations:  []config.LocationConfig{{Path: "/"}},
		})
		if err != nil {
			t.Fatalf("marshal target %s: %v", host, err)
		}
		if err := store.PutTargetByHost(ctx, host, data); err != nil {
			t.Fatalf("PutTargetByHost(%s): %v", host, err)
		}
	}

	ps := &proxyServer{targetStore: store}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](lruCapacity)
	ps.router = ps.buildHostRouter()

	if got := len(ps.lazyRouter.GetTargets()); got != 0 {
		t.Fatalf("routing table holds %d entries for %d targets, want 0", got, targets)
	}

	// Serve a spread of hosts: the ones that stayed resident and the ones that
	// were evicted after their first request.
	for _, i := range []int{0, 1, targets / 2, targets - 1, 0, targets / 2} {
		host := fmt.Sprintf("h%04d.example.com", i)
		rec := httptest.NewRecorder()
		ps.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://"+host+"/", nil))
		if rec.Body.String() != "scaled-body" {
			t.Fatalf("host %s body = %q, want %q", host, rec.Body.String(), "scaled-body")
		}
	}

	if got := ps.lazyLRU.Len(); got > lruCapacity {
		t.Fatalf("LRU holds %d handlers, want at most %d", got, lruCapacity)
	}

	// An unknown host must 404 without being materialized.
	rec := httptest.NewRecorder()
	ps.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://nope.example.com/", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown host status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// Guards the invariant that makes per-request handlers safe: the router must
// build its handlers on one shared flight table, so concurrent first requests
// for a cold host compile it once.
func TestLazyRouterCoalescesColdRequests(t *testing.T) {
	ctx := context.Background()
	store := lazyTestStore(t)
	url := lazyEchoUpstream(t, "coalesced-body")

	data, err := yaml.Marshal(&config.TargetConfig{
		ServerName: "svc-cold",
		Upstreams:  []config.UpstreamConfig{{URL: url}},
		Locations:  []config.LocationConfig{{Path: "/"}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.PutTargetByHost(ctx, "svc-cold", data); err != nil {
		t.Fatalf("PutTargetByHost: %v", err)
	}

	ps := &proxyServer{targetStore: store}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](16)

	var loads int
	var mu sync.Mutex
	release := make(chan struct{})
	ps.lazyLoad = func(ctx context.Context, hostname string) (*router.TargetConfigHandler, error) {
		mu.Lock()
		loads++
		mu.Unlock()
		<-release
		return ps.materializeTarget(ctx, hostname)
	}
	ps.router = ps.buildHostRouter()

	const callers = 16
	var wg sync.WaitGroup
	bodies := make([]string, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := httptest.NewRecorder()
			ps.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-cold/", nil))
			bodies[i] = rec.Body.String()
		}(i)
	}

	// Wait for the leader to enter the load, then give the followers time to
	// arrive and join the same flight.
	for {
		mu.Lock()
		started := loads
		mu.Unlock()
		if started == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	mu.Lock()
	got := loads
	mu.Unlock()
	if got != 1 {
		t.Fatalf("materialized %d times for %d concurrent cold requests, want 1", got, callers)
	}
	for i, body := range bodies {
		if body != "coalesced-body" {
			t.Fatalf("caller %d body = %q, want %q", i, body, "coalesced-body")
		}
	}
}

// An update to a target that is already resident must be picked up: the
// resolver rebuilds the handler, so the LRU invalidation has to be enough.
func TestLazyRouterServesUpdatedTarget(t *testing.T) {
	store := lazyTestStore(t)
	first := lazyEchoUpstream(t, "v1")
	second := lazyEchoUpstream(t, "v2")

	ps := &proxyServer{targetStore: store}
	ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](16)
	ps.router = ps.buildHostRouter()

	if err := ps.applyTargetUpdate("svc-rot", &config.TargetConfig{
		ServerName: "svc-rot",
		Upstreams:  []config.UpstreamConfig{{URL: first}},
		Locations:  []config.LocationConfig{{Path: "/"}},
	}); err != nil {
		t.Fatalf("applyTargetUpdate: %v", err)
	}

	rec := httptest.NewRecorder()
	ps.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-rot/", nil))
	if rec.Body.String() != "v1" {
		t.Fatalf("body before update = %q, want %q", rec.Body.String(), "v1")
	}

	if err := ps.applyTargetUpdate("svc-rot", &config.TargetConfig{
		ServerName: "svc-rot",
		Upstreams:  []config.UpstreamConfig{{URL: second}},
		Locations:  []config.LocationConfig{{Path: "/"}},
	}); err != nil {
		t.Fatalf("applyTargetUpdate (update): %v", err)
	}

	rec = httptest.NewRecorder()
	ps.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://svc-rot/", nil))
	if rec.Body.String() != "v2" {
		t.Fatalf("body after update = %q, want %q", rec.Body.String(), "v2")
	}
}
