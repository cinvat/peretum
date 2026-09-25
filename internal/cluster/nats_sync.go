package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/klog/v2"
)

// Subject layout. There is exactly one subject per target, so the stream
// retains exactly one message per target: the current state.
const (
	// SubjectTargetPrefix is followed by the server name. A single message on
	// this subject is the full current state of that target.
	SubjectTargetPrefix = "config.target."
)

// TargetEvent is the single retained message per target.
//
// The stream keeps only the newest message per subject, so no version, hash or
// sequence number is needed: whichever event is retained IS the current state.
// Replaying the whole stream therefore yields the complete configuration.
type TargetEvent struct {
	// ServerName is the target's hostname key. It also forms the subject.
	ServerName string `json:"server_name"`
	// Deleted marks the target as removed. The message is retained so that
	// edges joining later learn about the deletion instead of resurrecting it.
	Deleted bool `json:"deleted,omitempty"`
	// Config is the marshalled config.TargetConfig, absent when Deleted.
	Config json.RawMessage `json:"config,omitempty"`
	// Timestamp is informational only; ordering comes from the stream.
	Timestamp time.Time `json:"timestamp"`
}

// StreamConfig holds the JetStream configuration for the config event store.
type StreamConfig struct {
	Name string
	// Replicas is the number of stream replicas. 1 means single-node, which
	// the NATS server rejects on a cluster; use 3 for a clustered deployment.
	Replicas int
}

// DefaultStreamConfig returns the config event-store defaults.
//
// The stream is an event store, not a log: MaxMsgsPerSubject=1 with
// DiscardOld and LimitsPolicy keeps only the latest state per target, and the
// size/age limits are unbounded so state is never silently expired. Edges get
// their initial configuration by replaying the stream from the beginning.
func DefaultStreamConfig() StreamConfig {
	return StreamConfig{
		Name:     "config-sync",
		Replicas: 1,
	}
}

// NATSConfigSync consumes and publishes target state over a NATS JetStream
// event store.
type NATSConfigSync struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	stream string
}

// NewNATSConfigSync connects to NATS, ensures the config stream exists, and
// returns a sync client. The stream is created if missing and reconciled if
// its configuration has drifted.
func NewNATSConfigSync(ctx context.Context, natsURL string, cfg StreamConfig) (*NATSConfigSync, error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}

	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              cfg.Name,
		Subjects:          []string{SubjectTargetPrefix + ">"},
		Retention:         jetstream.LimitsPolicy,
		Storage:           jetstream.FileStorage,
		MaxMsgs:           -1,
		MaxAge:            0, // never expire current state
		MaxBytes:          -1,
		MaxMsgsPerSubject: 1, // keep only the newest event per target
		Discard:           jetstream.DiscardOld,
		Replicas:          cfg.Replicas,
	}); err != nil {
		nc.Close()
		return nil, fmt.Errorf("create config stream %q: %w", cfg.Name, err)
	}

	return &NATSConfigSync{nc: nc, js: js, stream: cfg.Name}, nil
}

// EnsureStream makes sure the config stream exists. Control planes use this on
// startup; they do not need to consume events.
func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg StreamConfig) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              cfg.Name,
		Subjects:          []string{SubjectTargetPrefix + ">"},
		Retention:         jetstream.LimitsPolicy,
		Storage:           jetstream.FileStorage,
		MaxMsgs:           -1,
		MaxAge:            0,
		MaxBytes:          -1,
		MaxMsgsPerSubject: 1,
		Discard:           jetstream.DiscardOld,
		Replicas:          cfg.Replicas,
	})
	return err
}

// TargetSubject returns the subject carrying a target's current state.
func TargetSubject(serverName string) string {
	return SubjectTargetPrefix + serverName
}

// PublishTarget writes a target's current state. Because the stream retains
// only the newest message per subject, this is an idempotent "set".
func (s *NATSConfigSync) PublishTarget(ctx context.Context, event TargetEvent) error {
	if event.ServerName == "" {
		return fmt.Errorf("publish target: empty server name")
	}
	if !event.Deleted && len(event.Config) == 0 {
		return fmt.Errorf("publish target %q: no config", event.ServerName)
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event for %s: %w", event.ServerName, err)
	}
	if _, err := s.js.Publish(ctx, TargetSubject(event.ServerName), data); err != nil {
		return fmt.Errorf("publish event for %s: %w", event.ServerName, err)
	}
	return nil
}

// PublishTargetDelete marks a target as removed.
func (s *NATSConfigSync) PublishTargetDelete(ctx context.Context, serverName string) error {
	return s.PublishTarget(ctx, TargetEvent{
		ServerName: serverName,
		Deleted:    true,
		Timestamp:  time.Now().UTC(),
	})
}

// ConsumerName returns the durable consumer name for this edge node.
//
// Every edge needs its own durable consumer: a shared durable would
// load-balance events between edges instead of delivering each event to all of
// them. The name is derived from the node identity so that a restarted edge
// resumes from its last acknowledged position.
//
// Set PERETUM_EDGE_ID to pin the identity explicitly, which is the usual
// choice on Kubernetes (the StatefulSet pod name, for example).
func ConsumerName() string {
	id := os.Getenv("PERETUM_EDGE_ID")
	if id == "" {
		host, err := os.Hostname()
		if err != nil {
			klog.Warningf("config sync: cannot determine hostname, falling back to a random edge id: %v", err)
		}
		id = host
	}
	if id == "" {
		id = fmt.Sprintf("edge-%d", os.Getpid())
	}
	return "edge-" + sanitizeConsumerName(id)
}

// maxConsumerNameLen bounds the generated durable name. The NATS server caps
// consumer names, and an over-long PERETUM_EDGE_ID would otherwise be rejected
// at Consume time rather than at startup.
const maxConsumerNameLen = 200

// sanitizeConsumerName reduces an edge id to a durable consumer name the NATS
// server accepts.
//
// The client-side validator only rejects "*>./ \t\r\n", but the *server* is
// stricter and also rejects '.', which is the token separator in a NATS subject
// (the consumer is addressed as stream.DURABLE.consumer). Hostnames very often
// contain a dot — "host.example.com", "ip-10-0-1-2.ec2.internal" — so keeping
// dots here made Consume fail outright on those hosts. Only [A-Za-z0-9_-] is
// safe; everything else collapses to a dash.
func sanitizeConsumerName(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}
	name := string(out)
	if len(name) > maxConsumerNameLen {
		name = name[:maxConsumerNameLen]
	}
	return name
}

// EventHandler applies one target event to the edge. Returning an error causes
// the event to be NAKed for redelivery.
type EventHandler func(context.Context, *TargetEvent) error

// Start applies the retained config events to handle and then follows live
// updates on the same durable consumer. The returned channel is closed once the
// retained backlog has been fully applied and the live consumer is attached,
// so a caller can hold off serving until the edge is actually caught up.
//
// Catch-up is synchronous and bounded for two reasons. The stream doubles as
// the initial snapshot, so an edge that starts serving before the backlog is
// applied answers 404 for every target it has not replayed yet, which looks
// exactly like a misconfigured origin. And because a store-backed router
// resolves hostnames by looking them up, there is no longer a prebuilt table
// that would have made a partially replayed store merely incomplete; missing
// rows now mean missing routes.
//
// The live consumer is attached *before* ready is signalled, and it reuses the
// same durable as the catch-up fetch. An event published in the window between
// the last empty batch and the consumer attaching stays pending on that durable
// and is delivered to it, so the handoff neither loses nor double-applies an
// event. Applying an event twice is harmless in any case: they are idempotent
// writes keyed by hostname.
func (s *NATSConfigSync) Start(ctx context.Context, handle EventHandler) (<-chan struct{}, error) {
	if handle == nil {
		return nil, fmt.Errorf("start: nil handler")
	}

	durable := ConsumerName()
	cons, err := s.js.CreateOrUpdateConsumer(ctx, s.stream, jetstream.ConsumerConfig{
		Durable:   durable,
		AckPolicy: jetstream.AckExplicitPolicy,
		// Replay the full retained state, then follow live updates. This is
		// what makes the stream double as the initial snapshot.
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		BackOff:       []time.Duration{time.Second, 5 * time.Second, 30 * time.Second},
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer %q: %w", durable, err)
	}

	ready := make(chan struct{})
	if err := s.catchUp(ctx, cons, handle); err != nil {
		return nil, err
	}

	if _, err := cons.Consume(func(msg jetstream.Msg) {
		applyMessage(ctx, handle, msg)
	}); err != nil {
		return nil, fmt.Errorf("consume %q: %w", durable, err)
	}
	close(ready)

	return ready, nil
}

// CatchUp is Start without the live consumer: it applies the retained backlog
// and returns when the consumer has caught up. It is the same durable Start
// uses, so a caller can catch up and later attach live updates on the same
// consumer without replaying anything twice.
func (s *NATSConfigSync) CatchUp(ctx context.Context, handle EventHandler) error {
	if handle == nil {
		return fmt.Errorf("catch up: nil handler")
	}
	cons, err := s.ensureConsumer(ctx)
	if err != nil {
		return err
	}
	return s.catchUp(ctx, cons, handle)
}

// Consume delivers live target events to handle on the edge's durable consumer,
// starting from the beginning of the stream so that a newly started edge builds
// the same state as every other edge, then following updates.
//
// Unlike Start it returns as soon as the consumer is attached, so the caller
// cannot tell when the retained state has been applied. Prefer Start: only an
// asynchronous consumer is correct where serving before the replay completes is
// harmless.
func (s *NATSConfigSync) Consume(ctx context.Context, handle EventHandler) error {
	if handle == nil {
		return fmt.Errorf("consume: nil handler")
	}

	cons, err := s.ensureConsumer(ctx)
	if err != nil {
		return err
	}

	if _, err := cons.Consume(func(msg jetstream.Msg) {
		applyMessage(ctx, handle, msg)
	}); err != nil {
		return fmt.Errorf("consume %q: %w", cons.CachedInfo().Name, err)
	}

	return nil
}

// ensureConsumer creates or reopens the edge's durable consumer over the full
// retained stream.
func (s *NATSConfigSync) ensureConsumer(ctx context.Context) (jetstream.Consumer, error) {
	durable := ConsumerName()
	cons, err := s.js.CreateOrUpdateConsumer(ctx, s.stream, jetstream.ConsumerConfig{
		Durable:   durable,
		AckPolicy: jetstream.AckExplicitPolicy,
		// Replay the full retained state, then follow live updates. This is
		// what makes the stream double as the initial snapshot.
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		BackOff:       []time.Duration{time.Second, 5 * time.Second, 30 * time.Second},
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer %q: %w", durable, err)
	}
	return cons, nil
}

// catchUp applies every retained event to handle and returns once a fetch comes
// back empty, which means the consumer has delivered everything that was in the
// stream when the drain began.
func (s *NATSConfigSync) catchUp(ctx context.Context, cons jetstream.Consumer, handle EventHandler) error {
	for {
		applied, err := s.fetchBatch(ctx, cons, handle)
		if err != nil {
			return err
		}
		if applied == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

// fetchBatch applies one batch and reports how many events it applied. A fetch
// that times out with nothing available yields an empty batch and no error,
// which is how the drain knows it has reached the end of the retained state.
func (s *NATSConfigSync) fetchBatch(ctx context.Context, cons jetstream.Consumer, handle EventHandler) (int, error) {
	batch, err := cons.Fetch(catchUpBatchSize, jetstream.FetchMaxWait(catchUpMaxWait))
	if err != nil {
		return 0, fmt.Errorf("fetch %q: %w", cons.CachedInfo().Name, err)
	}

	applied := 0
	for msg := range batch.Messages() {
		applyMessage(ctx, handle, msg)
		applied++
	}
	if err := batch.Error(); err != nil {
		return applied, fmt.Errorf("fetch %q: %w", cons.CachedInfo().Name, err)
	}
	return applied, nil
}

// applyMessage applies one event and settles the message.
//
// Messages are acknowledged only after handle returns nil, so an edge that
// crashes mid-apply replays the affected events on restart. A malformed event
// can never succeed, so it is terminated rather than left to block the consumer
// with a poison message.
func applyMessage(ctx context.Context, handle EventHandler, msg jetstream.Msg) {
	var event TargetEvent
	if err := json.Unmarshal(msg.Data(), &event); err != nil {
		klog.Errorf("config sync: dropping malformed event: %v", err)
		_ = msg.Term()
		return
	}
	if err := handle(ctx, &event); err != nil {
		klog.Errorf("config sync: apply %s: %v", event.ServerName, err)
		_ = msg.Nak()
		return
	}
	_ = msg.Ack()
}

const (
	// catchUpBatchSize is how many events one fetch request asks for. Large
	// enough to keep the replay round trips low, small enough that a failed
	// batch is cheaply redelivered.
	catchUpBatchSize = 256

	// catchUpMaxWait is how long a fetch waits for the batch to fill before
	// returning what it has. It bounds the empty fetch that ends the drain, so
	// it is also the floor on how long catching up takes when there is nothing
	// to replay.
	catchUpMaxWait = 250 * time.Millisecond
)

// Close drains and closes the NATS connection.
func (s *NATSConfigSync) Close() error {
	if s == nil || s.nc == nil {
		return nil
	}
	if err := s.nc.Drain(); err != nil {
		s.nc.Close()
	}
	return nil
}
