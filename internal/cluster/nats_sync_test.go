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
