package waf

import (
	"context"
	"net/http/httptest"
	"testing"
)

func TestRateLimitActionEnforced(t *testing.T) {
	p := NewWAFPlugin()
	err := p.Init(map[string]any{
		"enabled": true,
		"locations": []any{map[string]any{
			"target": "t", "location": "l",
			"waf": map[string]any{
				"enabled": true,
				"rules": []any{map[string]any{
					"id":     "rl",
					"action": map[string]any{"type": "rate_limit", "rps": 1.0, "burst": 1},
					"conditions": []any{[]any{
						map[string]any{"param": "path", "operator": "startswith", "value": "/"},
					}},
				}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Stop(context.Background())

	// First request passes, second from same IP is 429.
	for i, wantBlocked := range []bool{false, true} {
		w := httptest.NewRecorder()
		r := requestFor("9.9.9.9:1", "t", "ua", "GET")
		r.URL.Path = "/api"
		err := p.BeforeProxy(w, r, "t", "l")
		if !wantBlocked && err != nil {
			t.Fatalf("req %d: expected pass, got %v", i, err)
		}
		if wantBlocked {
			if err == nil {
				t.Fatalf("req %d: expected 429 block", i)
			}
			if w.Code != 429 {
				t.Fatalf("req %d: expected 429, got %d", i, w.Code)
			}
			if w.Header().Get("Retry-After") == "" {
				t.Fatalf("req %d: missing Retry-After", i)
			}
		}
	}
}
