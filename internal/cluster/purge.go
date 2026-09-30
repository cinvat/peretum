package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"k8s.io/klog/v2"
)

const (
	// SubjectCachePurge carries cache purge broadcasts to every edge.
	// Unlike the config event store it is ephemeral core NATS, not JetStream:
	// a purge needs no replay, and an edge that was offline refills the
	// affected entries from origin on demand.
	SubjectCachePurge = "cache.purge"
)

// PurgeEvent selects the cache entries every edge must drop. Exactly one
// scope is set: Host only (whole site), Host+Path (single object),
// Host+Prefix or Prefix only (location subtree), or All (whole cache).
type PurgeEvent struct {
	Host     string    `json:"host,omitempty"`
	Path     string    `json:"path,omitempty"`
	Prefix   string    `json:"prefix,omitempty"`
	All      bool      `json:"all,omitempty"`
	IssuedAt time.Time `json:"issued_at"`
}

// Validate reports whether the event selects exactly one purge scope.
func (e PurgeEvent) Validate() error {
	switch {
	case e.All:
		if e.Host != "" || e.Path != "" || e.Prefix != "" {
			return fmt.Errorf("all=true is exclusive: drop host/path/prefix")
		}
		return nil
	case e.Path != "" && e.Prefix != "":
		return fmt.Errorf("path and prefix are mutually exclusive")
	case e.Path != "":
		if e.Host == "" {
			return fmt.Errorf("path purge requires host")
		}
		return nil
	case e.Prefix != "" || e.Host != "":
		return nil
	default:
		return fmt.Errorf("nothing to purge: pass host, host+path, host+prefix, or all=true")
	}
}

// PublishPurgeEvent connects to NATS, broadcasts one purge event to every
// edge, and disconnects. Purges are rare admin operations, so a dedicated
// connection per call is cheaper than holding one open.
func PublishPurgeEvent(ctx context.Context, natsURI string, event PurgeEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	event.IssuedAt = time.Now().UTC()
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal purge event: %w", err)
	}
	nc, err := nats.Connect(natsURI)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	defer nc.Close()
	if err := nc.Publish(SubjectCachePurge, data); err != nil {
		return fmt.Errorf("publish purge: %w", err)
	}
	nc.Flush()
	return nil
}

// PurgeHandler applies a received purge event (e.g. against the edge's live
// disk cache) and reports how many entries were dropped.
type PurgeHandler func(context.Context, PurgeEvent) int

// SubscribeCachePurge subscribes to purge broadcasts. Malformed events are
// logged and ignored — core NATS has no redelivery, so there is no poison
// message to settle. The subscription lives as long as the connection;
// Close drains it.
func (s *NATSConfigSync) SubscribeCachePurge(handle PurgeHandler) (*nats.Subscription, error) {
	if s == nil || s.nc == nil {
		return nil, fmt.Errorf("NATS connection is not established")
	}
	sub, err := s.nc.Subscribe(SubjectCachePurge, func(msg *nats.Msg) {
		var event PurgeEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			klog.Errorf("cache purge: dropping malformed event: %v", err)
			return
		}
		if err := event.Validate(); err != nil {
			klog.Errorf("cache purge: dropping invalid event: %v", err)
			return
		}
		dropped := handle(context.Background(), event)
		klog.Infof("cache purge applied %+v: %d entries dropped", event, dropped)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe %s: %w", SubjectCachePurge, err)
	}
	return sub, nil
}
