package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPurgeEventValidate(t *testing.T) {
	valid := []PurgeEvent{
		{Host: "h"},
		{Host: "h", Path: "/a"},
		{Host: "h", Prefix: "/b/"},
		{Prefix: "/b/"},
		{All: true},
	}
	for i, ev := range valid {
		if err := ev.Validate(); err != nil {
			t.Fatalf("valid[%d] = %v", i, err)
		}
	}
	invalid := []PurgeEvent{
		{},
		{Path: "/a"},
		{Host: "h", Path: "/a", Prefix: "/b/"},
		{Host: "h", All: true},
		{Path: "/a", All: true},
	}
	for i, ev := range invalid {
		if err := ev.Validate(); err == nil {
			t.Fatalf("invalid[%d] accepted: %+v", i, ev)
		}
	}
}

func TestPublishSubscribePurge(t *testing.T) {
	url := startJetStream(t)
	sync := newTestSync(t, url)

	var got atomic.Value
	sub, err := sync.SubscribeCachePurge(func(_ context.Context, ev PurgeEvent) int {
		got.Store(ev)
		return 7
	})
	if err != nil {
		t.Fatalf("SubscribeCachePurge: %v", err)
	}
	_ = sub // drained by Close

	event := PurgeEvent{Host: "example.com", Prefix: "/blog/"}
	if err := PublishPurgeEvent(context.Background(), url, event); err != nil {
		t.Fatalf("PublishPurgeEvent: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if v := got.Load(); v != nil {
			ev := v.(PurgeEvent)
			if ev.Host != "example.com" || ev.Prefix != "/blog/" {
				t.Fatalf("event = %+v", ev)
			}
			if ev.IssuedAt.IsZero() {
				t.Fatal("IssuedAt not stamped")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for purge event")
}

func TestPublishPurgeEventValidation(t *testing.T) {
	if err := PublishPurgeEvent(context.Background(), "nats://127.0.0.1:1", PurgeEvent{}); err == nil {
		t.Fatal("expected validation error before connecting")
	}
}

func TestSubscribeCachePurgeNoConn(t *testing.T) {
	s := &NATSConfigSync{}
	if _, err := s.SubscribeCachePurge(func(context.Context, PurgeEvent) int { return 0 }); err == nil {
		t.Fatal("expected error without a connection")
	}
	var nilSync *NATSConfigSync
	if _, err := nilSync.SubscribeCachePurge(func(context.Context, PurgeEvent) int { return 0 }); err == nil {
		t.Fatal("expected error on nil sync")
	}
}
