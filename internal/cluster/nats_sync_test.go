package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// freePort reserves an ephemeral port and releases it, so the NATS test server
// can bind to a port that is very likely still free.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return port
}

func startJetStream(t *testing.T) string {
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
	if !ns.ReadyForConnections(15 * time.Second) {
		t.Fatal("NATS server not ready")
	}
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
}

func newTestSync(t *testing.T, url string) *NATSConfigSync {
	t.Helper()
	s, err := NewNATSConfigSync(context.Background(), url, DefaultStreamConfig())
	if err != nil {
		t.Fatalf("NewNATSConfigSync: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func waitUntil(t *testing.T, what string, cond func() bool) {
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

// collector records events delivered by Consume.
type collector struct {
	mu     sync.Mutex
	byName map[string]TargetEvent
	order  []string
}

func newCollector() *collector {
	return &collector{byName: make(map[string]TargetEvent)}
}

func (c *collector) handle(_ context.Context, ev *TargetEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byName[ev.ServerName] = *ev
	c.order = append(c.order, ev.ServerName)
	return nil
}

func (c *collector) get(name string) (TargetEvent, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ev, ok := c.byName[name]
	return ev, ok
}

func TestDefaultStreamConfigIsAnEventStore(t *testing.T) {
	cfg := DefaultStreamConfig()
	if cfg.Name == "" {
		t.Fatal("stream name is empty")
	}
	if cfg.Replicas < 1 {
		t.Fatalf("Replicas = %d, want >= 1", cfg.Replicas)
	}
}

func TestTargetSubject(t *testing.T) {
	if got, want := TargetSubject("api.example.com"), SubjectTargetPrefix+"api.example.com"; got != want {
		t.Fatalf("TargetSubject = %q, want %q", got, want)
	}
}

func TestConsumerNameIsStableAndSanitized(t *testing.T) {
	t.Setenv("PERETUM_EDGE_ID", "edge-01/pod")
	first := ConsumerName()
	if first != ConsumerName() {
		t.Fatal("ConsumerName is not stable for the same edge id")
	}
	if strings.ContainsAny(first, "/ ") {
		t.Fatalf("ConsumerName = %q, must not contain path separators or spaces", first)
	}
	if !strings.HasPrefix(first, "edge-") {
		t.Fatalf("ConsumerName = %q, want an edge- prefix", first)
	}

	// A different edge must get a different consumer, otherwise events would
	// be load-balanced between the two instead of delivered to both.
	t.Setenv("PERETUM_EDGE_ID", "edge-02")
	if other := ConsumerName(); other == first {
		t.Fatalf("two edges share the consumer name %q", other)
	}
}

func TestSanitizeConsumerName(t *testing.T) {
	cases := map[string]string{
		"pod-0":     "pod-0",
		"a/b":       "a-b",
		"host name": "host-name",
		"weird:*>?": "weird----",
		// Dots must be replaced: the NATS server rejects a durable name
		// containing one even though the client-side check allows it.
		"UPPER_case.01":            "UPPER_case-01",
		"host.example.com":         "host-example-com",
		"ip-10-0-1-2.ec2.internal": "ip-10-0-1-2-ec2-internal",
	}
	for in, want := range cases {
		if got := sanitizeConsumerName(in); got != want {
			t.Errorf("sanitizeConsumerName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPublishTargetValidation(t *testing.T) {
	url := startJetStream(t)
	s := newTestSync(t, url)
	ctx := context.Background()

	if err := s.PublishTarget(ctx, TargetEvent{}); err == nil {
		t.Error("PublishTarget with an empty server name should fail")
	}
	if err := s.PublishTarget(ctx, TargetEvent{ServerName: "api"}); err == nil {
		t.Error("PublishTarget with no config should fail")
	}
}

func TestPublishAndConsumeTargetState(t *testing.T) {
	url := startJetStream(t)
	s := newTestSync(t, url)
	ctx := context.Background()

	t.Setenv("PERETUM_EDGE_ID", "edge-a")
	c := newCollector()
	if err := s.Consume(ctx, c.handle); err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if err := s.PublishTarget(ctx, TargetEvent{
		ServerName: "api",
		Config:     json.RawMessage(`{"server_name":"api"}`),
	}); err != nil {
		t.Fatalf("PublishTarget: %v", err)
	}
	waitUntil(t, "api event", func() bool {
		ev, ok := c.get("api")
		return ok && !ev.Deleted
	})

	ev, _ := c.get("api")
	if ev.Timestamp.IsZero() {
		t.Error("PublishTarget should stamp a timestamp")
	}

	// Publishing again replaces the retained state rather than adding to it.
	if err := s.PublishTarget(ctx, TargetEvent{
		ServerName: "api",
		Config:     json.RawMessage(`{"server_name":"api","changed":true}`),
	}); err != nil {
		t.Fatalf("second PublishTarget: %v", err)
	}
	waitUntil(t, "api update", func() bool {
		ev, ok := c.get("api")
		return ok && strings.Contains(string(ev.Config), "changed")
	})

	// A delete is retained too, so edges joining later do not resurrect it.
	if err := s.PublishTargetDelete(ctx, "api"); err != nil {
		t.Fatalf("PublishTargetDelete: %v", err)
	}
	waitUntil(t, "api deletion", func() bool {
		ev, ok := c.get("api")
		return ok && ev.Deleted
	})
}

func TestStreamRetainsOnlyLatestStatePerTarget(t *testing.T) {
	url := startJetStream(t)
	s := newTestSync(t, url)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := s.PublishTarget(ctx, TargetEvent{
			ServerName: "api",
			Config:     json.RawMessage(fmt.Sprintf(`{"server_name":"api","n":%d}`, i)),
		}); err != nil {
			t.Fatalf("PublishTarget(%d): %v", i, err)
		}
	}

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	stream, err := js.Stream(ctx, DefaultStreamConfig().Name)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if got, want := info.Config.MaxMsgsPerSubject, int64(1); got != want {
		t.Fatalf("MaxMsgsPerSubject = %d, want %d", got, want)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream holds %d messages, want 1 (the newest state only)", info.State.Msgs)
	}
}

func TestConsumeReplaysRetainedStateForNewEdge(t *testing.T) {
	url := startJetStream(t)
	pub := newTestSync(t, url)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("svc%d", i)
		if err := pub.PublishTarget(ctx, TargetEvent{
			ServerName: name,
			Config:     json.RawMessage(fmt.Sprintf(`{"server_name":%q}`, name)),
		}); err != nil {
			t.Fatalf("PublishTarget(%s): %v", name, err)
		}
	}

	// A brand new edge replays the whole stream, which is how it obtains the
	// full configuration without a separate snapshot endpoint.
	t.Setenv("PERETUM_EDGE_ID", "fresh-edge")
	s := newTestSync(t, url)
	c := newCollector()
	if err := s.Consume(ctx, c.handle); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	waitUntil(t, "all replayed targets", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.byName) == 3
	})
}

func TestTwoEdgesEachReceiveEveryEvent(t *testing.T) {
	url := startJetStream(t)
	ctx := context.Background()

	t.Setenv("PERETUM_EDGE_ID", "edge-1")
	edge1 := newCollector()
	s1 := newTestSync(t, url)
	if err := s1.Consume(ctx, edge1.handle); err != nil {
		t.Fatalf("edge1 Consume: %v", err)
	}

	t.Setenv("PERETUM_EDGE_ID", "edge-2")
	edge2 := newCollector()
	s2 := newTestSync(t, url)
	if err := s2.Consume(ctx, edge2.handle); err != nil {
		t.Fatalf("edge2 Consume: %v", err)
	}

	publisher := newTestSync(t, url)
	if err := publisher.PublishTarget(ctx, TargetEvent{
		ServerName: "shared",
		Config:     json.RawMessage(`{"server_name":"shared"}`),
	}); err != nil {
		t.Fatalf("PublishTarget: %v", err)
	}

	// Every edge needs the event, not just one of them.
	waitUntil(t, "edge1 to receive", func() bool {
		ev, ok := edge1.get("shared")
		return ok && !ev.Deleted
	})
	waitUntil(t, "edge2 to receive", func() bool {
		ev, ok := edge2.get("shared")
		return ok && !ev.Deleted
	})
}

func TestEnsureStreamIsIdempotent(t *testing.T) {
	url := startJetStream(t)
	s := newTestSync(t, url)
	ctx := context.Background()

	// The stream already exists; reconciling it again must not error.
	if err := EnsureStream(ctx, s.js, DefaultStreamConfig()); err != nil {
		t.Fatalf("EnsureStream on an existing stream: %v", err)
	}
}

func TestNewNATSConfigSyncFailsOnBadURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := NewNATSConfigSync(ctx, "nats://127.0.0.1:1", DefaultStreamConfig()); err == nil {
		t.Error("NewNATSConfigSync should fail when NATS is unreachable")
	}
}

func TestCloseIsSafeOnNilReceiver(t *testing.T) {
	var s *NATSConfigSync
	if err := s.Close(); err != nil {
		t.Fatalf("Close on nil receiver: %v", err)
	}
	if err := os.Setenv("PERETUM_EDGE_ID", "x"); err != nil {
		t.Fatal(err)
	}
}

func TestConsumeRejectsEmptyHandler(t *testing.T) {
	// A nil handler would panic on the first event, so it must be refused up
	// front rather than when traffic arrives.
	s := newTestSync(t, startJetStream(t))
	if err := s.Consume(context.Background(), nil); err == nil {
		t.Fatal("Consume with a nil handler should fail")
	}
}

func TestConsumeTerminatesMalformedEvents(t *testing.T) {
	s := newTestSync(t, startJetStream(t))
	ctx := context.Background()

	// Publish a raw message that is not a TargetEvent.
	if _, err := s.js.Publish(ctx, TargetSubject("broken.example.com"), []byte("{not json")); err != nil {
		t.Fatalf("publish malformed: %v", err)
	}

	got := newCollector()
	if err := s.Consume(ctx, got.handle); err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if err := s.PublishTarget(ctx, TargetEvent{ServerName: "good.example.com", Config: json.RawMessage(`{"server_name":"good.example.com"}`), Timestamp: time.Now().UTC()}); err != nil {
		t.Fatalf("PublishTarget: %v", err)
	}

	// The malformed message must be dropped, not redelivered forever, while the
	// valid event behind it is still delivered.
	waitUntil(t, "valid event after malformed one", func() bool {
		_, ok := got.get("good.example.com")
		return ok
	})

	waitUntil(t, "malformed message to be terminated", func() bool {
		cons, err := s.js.Consumer(ctx, s.stream, ConsumerName())
		if err != nil {
			return false
		}
		info, err := cons.Info(ctx)
		return err == nil && info.NumAckPending == 0 && info.NumPending == 0
	})
}

func TestConsumeNaksHandlerErrors(t *testing.T) {
	s := newTestSync(t, startJetStream(t))
	ctx := context.Background()

	var calls int
	var mu sync.Mutex
	failFirst := func(_ context.Context, ev *TargetEvent) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return fmt.Errorf("transient apply failure")
		}
		return nil
	}

	if err := s.Consume(ctx, failFirst); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := s.PublishTarget(ctx, TargetEvent{ServerName: "flaky.example.com", Config: json.RawMessage(`{"server_name":"flaky.example.com"}`), Timestamp: time.Now().UTC()}); err != nil {
		t.Fatalf("PublishTarget: %v", err)
	}

	// A failed apply must be nacked so it is redelivered, not acked and lost.
	waitUntil(t, "event to be redelivered after a failed apply", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	})
}

func TestCloseIsSafeToCallTwice(t *testing.T) {
	url := startJetStream(t)
	s, err := NewNATSConfigSync(context.Background(), url, DefaultStreamConfig())
	if err != nil {
		t.Fatalf("NewNATSConfigSync: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Close on an already-closed sync must not panic.
	_ = s.Close()
}

func TestNewNATSConfigSyncAcceptsCommaSeparatedClusterURLs(t *testing.T) {
	url := startJetStream(t)
	// A single-node cluster expressed as a comma-separated list must connect.
	s, err := NewNATSConfigSync(context.Background(), url+","+url, DefaultStreamConfig())
	if err != nil {
		t.Fatalf("NewNATSConfigSync with a list of URLs: %v", err)
	}
	defer s.Close()

	if err := EnsureStream(context.Background(), s.js, DefaultStreamConfig()); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
}

func TestNATSConfigSyncRespectsCustomStreamNameAndReplicas(t *testing.T) {
	url := startJetStream(t)
	s, err := NewNATSConfigSync(context.Background(), url, StreamConfig{Name: "custom-store", Replicas: 1})
	if err != nil {
		t.Fatalf("NewNATSConfigSync: %v", err)
	}
	defer s.Close()

	if s.stream != "custom-store" {
		t.Fatalf("stream = %q, want custom-store", s.stream)
	}

	stream, err := s.js.Stream(context.Background(), "custom-store")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	info := stream.CachedInfo()
	if info.Config.MaxMsgsPerSubject != 1 {
		t.Fatalf("MaxMsgsPerSubject = %d, want 1", info.Config.MaxMsgsPerSubject)
	}
	if info.Config.Discard != jetstream.DiscardOld {
		t.Fatalf("Discard = %v, want DiscardOld", info.Config.Discard)
	}
}

// readyGate is a handler that can be held open mid-apply, so a test can prove
// readiness is only signalled once every retained event has actually been
// applied.
type readyGate struct {
	mu      sync.Mutex
	started int
	applied []string
	blocked chan struct{}
}

func newReadyGate(blocked bool) *readyGate {
	g := &readyGate{blocked: make(chan struct{})}
	if !blocked {
		close(g.blocked)
	}
	return g
}

func (g *readyGate) handle(_ context.Context, ev *TargetEvent) error {
	g.mu.Lock()
	g.started++
	g.mu.Unlock()
	// Hold the apply open so readiness provably cannot be reached while an
	// event is still being applied.
	<-g.blocked
	g.mu.Lock()
	g.applied = append(g.applied, ev.ServerName)
	g.mu.Unlock()
	return nil
}

func (g *readyGate) unblock() {
	select {
	case <-g.blocked:
	default:
		close(g.blocked)
	}
}

func (g *readyGate) startedCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.started
}

func (g *readyGate) appliedNames() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.applied...)
}

func (g *readyGate) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.applied)
}

// The regression this fixes: a fresh edge must not report ready until the
// retained state has been applied, because a store-backed router has no route
// for a target whose event it has not replayed yet.
func TestStartDoesNotSignalReadyUntilBacklogApplied(t *testing.T) {
	url := startJetStream(t)
	ctx := context.Background()
	t.Setenv("PERETUM_EDGE_ID", "edge-replay")

	// A second sync publishes the retained state before this edge joins.
	writer := newTestSync(t, url)
	for _, name := range []string{"a", "b", "c"} {
		if err := writer.PublishTarget(ctx, TargetEvent{
			ServerName: name,
			Config:     json.RawMessage(`{"server_name":"` + name + `"}`),
		}); err != nil {
			t.Fatalf("PublishTarget(%s): %v", name, err)
		}
	}

	s := newTestSync(t, url)
	g := newReadyGate(true) // every apply blocks until told to proceed

	// Start is synchronous, so run it in the background to be able to observe
	// that readiness is still pending while events are mid-apply.
	type outcome struct {
		ready <-chan struct{}
		err   error
	}
	res := make(chan outcome, 1)
	go func() {
		ready, err := s.Start(ctx, g.handle)
		res <- outcome{ready, err}
	}()

	waitUntil(t, "an event to start applying", func() bool { return g.startedCount() >= 1 })
	if got := g.appliedNames(); len(got) != 0 {
		t.Fatalf("applied %v while every apply is blocked, want none", got)
	}
	select {
	case out := <-res:
		t.Fatalf("Start returned while the backlog was still being applied (ready=%v err=%v)", out.ready != nil, out.err)
	default:
	}

	g.unblock()
	out := <-res
	if out.err != nil {
		t.Fatalf("Start: %v", out.err)
	}
	select {
	case <-out.ready:
	case <-time.After(15 * time.Second):
		t.Fatal("ready was never signalled")
	}

	applied := g.appliedNames()
	want := []string{"a", "b", "c"}
	if len(applied) != len(want) {
		t.Fatalf("applied %v, want %v", applied, want)
	}
	for i := range want {
		if applied[i] != want[i] {
			t.Fatalf("applied %v, want %v (order matters: last write per target wins)", applied, want)
		}
	}
}

// After Start, live events must still arrive on the same durable, and an event
// published in the handoff window must not be lost.
func TestStartFollowsLiveUpdatesAfterCatchUp(t *testing.T) {
	url := startJetStream(t)
	ctx := context.Background()
	t.Setenv("PERETUM_EDGE_ID", "edge-live")

	writer := newTestSync(t, url)
	if err := writer.PublishTarget(ctx, TargetEvent{
		ServerName: "retained",
		Config:     json.RawMessage(`{"server_name":"retained"}`),
	}); err != nil {
		t.Fatalf("PublishTarget: %v", err)
	}

	s := newTestSync(t, url)
	c := newCollector()
	ready, err := s.Start(ctx, c.handle)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(15 * time.Second):
		t.Fatal("ready was never signalled")
	}

	// The retained event came through the catch-up drain.
	waitUntil(t, "retained event to be caught up", func() bool {
		_, ok := c.get("retained")
		return ok
	})

	// And the live consumer keeps working afterwards.
	if err := s.PublishTarget(ctx, TargetEvent{
		ServerName: "live",
		Config:     json.RawMessage(`{"server_name":"live"}`),
	}); err != nil {
		t.Fatalf("PublishTarget(live): %v", err)
	}
	waitUntil(t, "live event", func() bool {
		_, ok := c.get("live")
		return ok
	})
}

// The catch-up drain and the live consumer must not double-apply: a retained
// event has to be applied exactly once even though it passes the same durable
// on the way from the fetch to the consumer.
func TestStartAppliesEachRetainedEventOnce(t *testing.T) {
	url := startJetStream(t)
	ctx := context.Background()
	t.Setenv("PERETUM_EDGE_ID", "edge-once")

	writer := newTestSync(t, url)
	const targets = 40
	for i := 0; i < targets; i++ {
		name := fmt.Sprintf("svc-%02d", i)
		if err := writer.PublishTarget(ctx, TargetEvent{
			ServerName: name,
			Config:     json.RawMessage(`{"server_name":"` + name + `"}`),
		}); err != nil {
			t.Fatalf("PublishTarget(%s): %v", name, err)
		}
	}

	s := newTestSync(t, url)
	c := newCollector()
	ready, err := s.Start(ctx, c.handle)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(15 * time.Second):
		t.Fatal("ready was never signalled")
	}

	c.mu.Lock()
	order := append([]string(nil), c.order...)
	c.mu.Unlock()

	if len(order) != targets {
		t.Fatalf("applied %d events, want %d: %v", len(order), targets, order)
	}
	seen := make(map[string]int, targets)
	for _, name := range order {
		seen[name]++
	}
	for name, n := range seen {
		if n != 1 {
			t.Fatalf("event %s applied %d times, want 1", name, n)
		}
	}
}

// An event published while the drain is finishing must still be delivered: the
// live consumer is attached to the same durable the drain used, so it picks up
// anything that was not acked.
func TestStartDoesNotLoseEventPublishedDuringCatchUp(t *testing.T) {
	url := startJetStream(t)
	ctx := context.Background()
	t.Setenv("PERETUM_EDGE_ID", "edge-race")

	writer := newTestSync(t, url)
	if err := writer.PublishTarget(ctx, TargetEvent{
		ServerName: "early",
		Config:     json.RawMessage(`{"server_name":"early"}`),
	}); err != nil {
		t.Fatalf("PublishTarget(early): %v", err)
	}

	s := newTestSync(t, url)
	c := newCollector()

	// Publish concurrently with the drain so the event lands around the moment
	// the drain reports itself empty.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = writer.PublishTarget(ctx, TargetEvent{
			ServerName: "racing",
			Config:     json.RawMessage(`{"server_name":"racing"}`),
		})
	}()

	ready, err := s.Start(ctx, c.handle)
	wg.Wait()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(15 * time.Second):
		t.Fatal("ready was never signalled")
	}

	// Whether the event was drained or delivered live, it must arrive exactly
	// once; losing it is the failure mode this guards.
	waitUntil(t, "both events", func() bool {
		_, a := c.get("early")
		_, b := c.get("racing")
		return a && b
	})
	c.mu.Lock()
	order := append([]string(nil), c.order...)
	c.mu.Unlock()
	if len(order) != 2 {
		t.Fatalf("applied %v, want exactly one event per target", order)
	}
}

// CatchUp alone must leave the durable positioned so a later Start does not
// replay everything again.
func TestCatchUpThenStartDoesNotReplayTwice(t *testing.T) {
	url := startJetStream(t)
	ctx := context.Background()
	t.Setenv("PERETUM_EDGE_ID", "edge-catchup")

	writer := newTestSync(t, url)
	for _, name := range []string{"x", "y"} {
		if err := writer.PublishTarget(ctx, TargetEvent{
			ServerName: name,
			Config:     json.RawMessage(`{"server_name":"` + name + `"}`),
		}); err != nil {
			t.Fatalf("PublishTarget(%s): %v", name, err)
		}
	}

	s := newTestSync(t, url)
	c := newCollector()
	if err := s.CatchUp(ctx, c.handle); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	if c.count() != 2 {
		t.Fatalf("CatchUp applied %d events, want 2", c.count())
	}

	ready, err := s.Start(ctx, c.handle)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(15 * time.Second):
		t.Fatal("ready was never signalled")
	}

	if got := c.count(); got != 2 {
		t.Fatalf("applied %d events overall, want 2; Start re-replayed the backlog", got)
	}
}

// A canceled catch-up must surface the cancellation rather than reporting ready.
func TestStartCanceledDuringCatchUp(t *testing.T) {
	url := startJetStream(t)
	t.Setenv("PERETUM_EDGE_ID", "edge-cancel")

	writer := newTestSync(t, url)
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("svc-%02d", i)
		if err := writer.PublishTarget(context.Background(), TargetEvent{
			ServerName: name,
			Config:     json.RawMessage(`{"server_name":"` + name + `"}`),
		}); err != nil {
			t.Fatalf("PublishTarget(%s): %v", name, err)
		}
	}

	s := newTestSync(t, url)
	ctx, cancel := context.WithCancel(context.Background())
	g := newReadyGate(false)
	done := make(chan error, 1)
	go func() {
		_, err := s.Start(ctx, g.handle)
		done <- err
	}()

	// Cancel while the drain is running.
	waitUntil(t, "drain to start", func() bool { return g.count() > 0 })
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Start succeeded on a canceled context")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Start did not return after its context was canceled")
	}
}

func TestStartAndCatchUpRejectNilHandler(t *testing.T) {
	url := startJetStream(t)
	s := newTestSync(t, url)
	ctx := context.Background()

	if _, err := s.Start(ctx, nil); err == nil {
		t.Fatal("Start accepted a nil handler")
	}
	if err := s.CatchUp(ctx, nil); err == nil {
		t.Fatal("CatchUp accepted a nil handler")
	}
}

// count reports how many events the collector has applied.
func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.order)
}
