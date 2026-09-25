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

// Consume delivers every target event to handle, starting from the beginning
// of the stream so that a newly started edge builds the same state as every
// other edge, then following live updates.
//
// Messages are acknowledged only after handle returns nil, so an edge that
// crashes mid-apply replays the affected events on restart. Because events are
// idempotent writes keyed by hostname, replay is safe.
func (s *NATSConfigSync) Consume(ctx context.Context, handle func(context.Context, *TargetEvent) error) error {
	if handle == nil {
		return fmt.Errorf("consume: nil handler")
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
		return fmt.Errorf("create consumer %q: %w", durable, err)
	}

	if _, err := cons.Consume(func(msg jetstream.Msg) {
		var event TargetEvent
		if err := json.Unmarshal(msg.Data(), &event); err != nil {
			// A malformed event will never succeed; drop it rather than
			// blocking the consumer on a poison message.
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
	}); err != nil {
		return fmt.Errorf("consume %q: %w", durable, err)
	}

	return nil
}

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
