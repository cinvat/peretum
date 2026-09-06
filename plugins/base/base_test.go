package base

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"testing"
)

func TestNewBasePlugin(t *testing.T) {
	bp := NewBasePlugin("testing")
	if bp.name != "testing" {
		t.Fatalf("expected name testing, got %s", bp.name)
	}
	if got := bp.Name(); got != "testing" {
		t.Fatalf("Name() = %s", got)
	}
	if bp.config != nil {
		t.Fatalf("expected nil config")
	}

	cfg := map[string]any{"enabled": true}
	if err := bp.Init(cfg); err != nil {
		t.Fatalf("Init() unexpected error: %v", err)
	}
	if bp.config == nil {
		t.Fatalf("Init() did not store config")
	}

	if err := bp.Start(context.Background()); err != nil {
		t.Fatalf("Start() unexpected error: %v", err)
	}
	if err := bp.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() unexpected error: %v", err)
	}
}

func newTestRequest() *http.Request {
	return &http.Request{
		Header: http.Header{},
	}
}

func TestEnsureRequestIDExisting(t *testing.T) {
	r := newTestRequest()
	r.Header.Set("X-Request-ID", "existing-id")
	out, id := EnsureRequestID(r)
	if id != "existing-id" {
		t.Fatalf("expected existing id, got %s", id)
	}
	if RequestID(out) != "existing-id" {
		t.Fatalf("RequestID() = %s", RequestID(out))
	}
}

func TestEnsureRequestIDGenerated(t *testing.T) {
	out, id := EnsureRequestID(newTestRequest())
	if id == "" {
		t.Fatalf("expected generated id")
	}
	if RequestID(out) != id {
		t.Fatalf("RequestID() = %s, want %s", RequestID(out), id)
	}
}

func TestRequestIDMissing(t *testing.T) {
	if got := RequestID(newTestRequest()); got != "" {
		t.Fatalf("RequestID() = %q, want empty", got)
	}
}

func TestNewRequestIDRandomFallback(t *testing.T) {
	orig := readRandom
	defer func() { readRandom = orig }()

	readRandom = func(b []byte) (int, error) {
		return 0, errors.New("no entropy")
	}
	id := newRequestID()
	if _, err := strconv.ParseInt(id, 10, 64); err != nil {
		t.Fatalf("expected numeric fallback id, got %q: %v", id, err)
	}
}

func TestNewRequestIDSuccess(t *testing.T) {
	id := newRequestID()
	if len(id) != 16 {
		t.Fatalf("expected 16-char hex id, got %q", id)
	}
	// Two calls should differ.
	if id2 := newRequestID(); id2 == id {
		t.Fatalf("expected distinct ids, both %q", id)
	}
}
