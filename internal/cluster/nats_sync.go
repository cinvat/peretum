package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATSConfigSync handles config synchronization via NATS JetStream.
type NATSConfigSync struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	mu       sync.Mutex
	handlers map[string]func(ctx context.Context, msg jetstream.Msg) error
}

// StreamConfig holds NATS JetStream configuration.
type StreamConfig struct {
	Name              string
	Subjects          []string
	Replicas          int
	MaxMsgsPerSubject int
	MaxAge            time.Duration
	MaxBytes          int64
	DiscardPolicy     jetstream.DiscardPolicy
	StorageType       jetstream.StorageType
}

// DefaultStreamConfig returns a sensible default for config sync.
func DefaultStreamConfig() StreamConfig {
	return StreamConfig{
		Name:              "config-sync",
		Subjects:          []string{"config.target.>", "config.snapshot", "config.hot-targets"},
		Replicas:          1,
		MaxMsgsPerSubject: 1, // Keep only latest per subject
		MaxAge:            24 * time.Hour,
		MaxBytes:          -1,
		DiscardPolicy:     jetstream.DiscardOld,
		StorageType:       jetstream.FileStorage,
	}
}

// NewNATSConfigSync creates a new NATS config sync client.
func NewNATSConfigSync(ctx context.Context, natsURL string, streamCfg StreamConfig) (*NATSConfigSync, error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}

	// Create or update stream
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              streamCfg.Name,
		Subjects:          streamCfg.Subjects,
		Retention:         jetstream.LimitsPolicy,
		MaxMsgs:           -1,
		MaxAge:            streamCfg.MaxAge,
		MaxBytes:          streamCfg.MaxBytes,
		MaxMsgsPerSubject: int64(streamCfg.MaxMsgsPerSubject),
		Discard:           streamCfg.DiscardPolicy,
		Storage:           streamCfg.StorageType,
		Replicas:          streamCfg.Replicas,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create stream: %w", err)
	}

	return &NATSConfigSync{
		nc:       nc,
		js:       js,
		handlers: make(map[string]func(ctx context.Context, msg jetstream.Msg) error),
	}, nil
}

// Subject constants
const (
	SubjectTargetUpdated = "config.target.updated."
	SubjectTargetDeleted = "config.target.deleted."
	SubjectSnapshot      = "config.snapshot"
	SubjectHotTargets    = "config.hot-targets"
)

// TargetUpdateEvent represents a target config update.
type TargetUpdateEvent struct {
	ServerName string          `json:"server_name"`
	Version    string          `json:"version"`
	Config     json.RawMessage `json:"config"`
	Timestamp  time.Time       `json:"timestamp"`
}

// TargetDeleteEvent represents a target deletion.
type TargetDeleteEvent struct {
	ServerName string    `json:"server_name"`
	Timestamp  time.Time `json:"timestamp"`
}

// SnapshotRequest represents a request for full snapshot.
type SnapshotRequest struct {
	ReplyTo string `json:"reply_to"`
}

// SnapshotResponse represents a full config snapshot.
type SnapshotResponse struct {
	Targets map[string]TargetUpdateEvent `json:"targets"`
	Version int64                        `json:"version"`
}

// HotTargetsResponse represents the list of hot targets.
type HotTargetsResponse struct {
	ServerNames []string `json:"server_names"`
	Version     int64    `json:"version"`
}

// RegisterHandler registers a message handler for a subject.
func (s *NATSConfigSync) RegisterHandler(subject string, handler func(ctx context.Context, msg jetstream.Msg) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[subject] = handler
}

// Subscribe subscribes to target update/deleted events.
func (s *NATSConfigSync) Subscribe(ctx context.Context) error {
	s.mu.Lock()
	handlers := make(map[string]func(ctx context.Context, msg jetstream.Msg) error)
	for k, v := range s.handlers {
		handlers[k] = v
	}
	s.mu.Unlock()

	// Subscribe to target updates
	if _, err := s.js.CreateConsumer(ctx, s.streamName(), jetstream.ConsumerConfig{
		Durable:       "edge-updates",
		FilterSubject: SubjectTargetUpdated + "*",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    3,
	}); err != nil && !isConsumerExists(err) {
		return fmt.Errorf("create updates consumer: %w", err)
	}

	// Subscribe to target deletes
	if _, err := s.js.CreateConsumer(ctx, s.streamName(), jetstream.ConsumerConfig{
		Durable:       "edge-deletes",
		FilterSubject: SubjectTargetDeleted + "*",
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    3,
	}); err != nil && !isConsumerExists(err) {
		return fmt.Errorf("create deletes consumer: %w", err)
	}

	return nil
}

// streamName returns the stream name (hardcoded for now, could be configurable)
func (s *NATSConfigSync) streamName() string {
	return "config-sync"
}

// StartConsuming starts consuming messages for target updates and deletes.
func (s *NATSConfigSync) StartConsuming(ctx context.Context) error {
	s.mu.Lock()
	handlers := make(map[string]func(ctx context.Context, msg jetstream.Msg) error)
	for k, v := range s.handlers {
		handlers[k] = v
	}
	s.mu.Unlock()

	// Consume target updates
	updatesConsumer, err := s.js.Consumer(ctx, s.streamName(), "edge-updates")
	if err != nil {
		return fmt.Errorf("get updates consumer: %w", err)
	}

	_, err = updatesConsumer.Consume(func(msg jetstream.Msg) {
		ctx := context.Background()
		if handler, ok := handlers[SubjectTargetUpdated]; ok {
			if err := handler(ctx, msg); err != nil {
				msg.Nak()
				return
			}
		}
		msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("consume updates: %w", err)
	}

	// Consume target deletes
	deletesConsumer, err := s.js.Consumer(ctx, s.streamName(), "edge-deletes")
	if err != nil {
		return fmt.Errorf("get deletes consumer: %w", err)
	}

	_, err = deletesConsumer.Consume(func(msg jetstream.Msg) {
		ctx := context.Background()
		if handler, ok := handlers[SubjectTargetDeleted]; ok {
			if err := handler(ctx, msg); err != nil {
				msg.Nak()
				return
			}
		}
		msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("consume deletes: %w", err)
	}

	return nil
}

// PublishTargetUpdate publishes a target update event.
func (s *NATSConfigSync) PublishTargetUpdate(ctx context.Context, event TargetUpdateEvent) error {
	subject := SubjectTargetUpdated + event.ServerName
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if _, err := s.js.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	return nil
}

// PublishTargetDelete publishes a target delete event.
func (s *NATSConfigSync) PublishTargetDelete(ctx context.Context, event TargetDeleteEvent) error {
	subject := SubjectTargetDeleted + event.ServerName
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if _, err := s.js.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	return nil
}

// RequestSnapshot requests a full snapshot from the control plane.
func (s *NATSConfigSync) RequestSnapshot(ctx context.Context) (*SnapshotResponse, error) {
	replyTo := nats.NewInbox()
	req := SnapshotRequest{ReplyTo: replyTo}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	msg, err := s.nc.RequestWithContext(ctx, SubjectSnapshot, data)
	if err != nil {
		return nil, fmt.Errorf("request snapshot: %w", err)
	}

	var resp SnapshotResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return &resp, nil
}

// RequestHotTargets requests the list of hot targets from control plane.
func (s *NATSConfigSync) RequestHotTargets(ctx context.Context) (*HotTargetsResponse, error) {
	replyTo := nats.NewInbox()
	data, _ := json.Marshal(map[string]string{"reply_to": replyTo})

	var msg *nats.Msg
	var err error
	msg, err = s.nc.RequestWithContext(ctx, SubjectHotTargets, data)
	if err != nil {
		return nil, fmt.Errorf("request hot targets: %w", err)
	}

	var resp HotTargetsResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return &resp, nil
}

// Close closes the NATS connection.
func (s *NATSConfigSync) Close() error {
	s.nc.Drain()
	return nil
}

// Helper to check if consumer already exists
func isConsumerExists(err error) bool {
	return err != nil && (err.Error() == "consumer already exists" || err.Error() == "consumer already exists with different config")
}
