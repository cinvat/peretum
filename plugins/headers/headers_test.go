package headers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newHeadersPlugin(t *testing.T, cfg map[string]any) *HeadersPlugin {
	t.Helper()
	p := NewHeadersPlugin()
	if err := p.Init(cfg); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	return p
}

func fullCfg() map[string]any {
	return map[string]any{
		"request_add": map[string]any{
			"X-Forwarded-By": "tinyws",
			"X-Num":          42,
		},
		"request_remove": []any{"X-Internal-Token", 99},
		"response_add": map[string]any{
			"X-Powered-By": "tinyws",
			"X-Other":      123,
		},
		"response_remove": []any{"Server", true},
	}
}

func TestInit(t *testing.T) {
	p := newHeadersPlugin(t, fullCfg())
	if p.config.RequestAdd["X-Forwarded-By"] != "tinyws" {
		t.Errorf("RequestAdd = %v", p.config.RequestAdd)
	}
	if _, ok := p.config.RequestAdd["X-Num"]; ok {
		t.Error("non-string request_add value should be skipped")
	}
	if len(p.config.RequestRemove) != 1 || p.config.RequestRemove[0] != "X-Internal-Token" {
		t.Errorf("RequestRemove = %v", p.config.RequestRemove)
	}
	if p.config.ResponseAdd["X-Powered-By"] != "tinyws" {
		t.Errorf("ResponseAdd = %v", p.config.ResponseAdd)
	}
	if _, ok := p.config.ResponseAdd["X-Other"]; ok {
		t.Error("non-string response_add value should be skipped")
	}
	if len(p.config.ResponseRemove) != 1 || p.config.ResponseRemove[0] != "Server" {
		t.Errorf("ResponseRemove = %v", p.config.ResponseRemove)
	}
}

func TestInitEmptyConfig(t *testing.T) {
	p := newHeadersPlugin(t, map[string]any{})
	if p.config.RequestAdd != nil || p.config.RequestRemove != nil ||
		p.config.ResponseAdd != nil || p.config.ResponseRemove != nil {
		t.Errorf("expected nil maps, got %+v", p.config)
	}
}

func TestStartStop(t *testing.T) {
	p := newHeadersPlugin(t, fullCfg())
	if err := p.Start(context.Background()); err != nil {
		t.Errorf("Start: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestBeforeProxy(t *testing.T) {
	p := newHeadersPlugin(t, fullCfg())
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Internal-Token", "secret")
	req.Header.Set("Keep-Me", "yes")
	if err := p.BeforeProxy(httptest.NewRecorder(), req, "t", "l"); err != nil {
		t.Errorf("BeforeProxy: %v", err)
	}
	if req.Header.Get("X-Forwarded-By") != "tinyws" {
		t.Error("request_add not applied")
	}
	if req.Header.Get("X-Internal-Token") != "" {
		t.Error("request_remove not applied")
	}
	if req.Header.Get("Keep-Me") != "yes" {
		t.Error("unrelated header removed")
	}
}

func TestBeforeProxyNilMaps(t *testing.T) {
	p := newHeadersPlugin(t, map[string]any{})
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Keep", "v")
	if err := p.BeforeProxy(httptest.NewRecorder(), req, "t", "l"); err != nil {
		t.Errorf("BeforeProxy nil: %v", err)
	}
	if req.Header.Get("X-Keep") != "v" {
		t.Error("nil request_add should not touch headers")
	}
}

func TestAfterProxy(t *testing.T) {
	p := newHeadersPlugin(t, fullCfg())
	w := httptest.NewRecorder()
	w.Header().Set("Server", "nginx")
	w.Header().Set("Content-Type", "text/plain")
	var rw http.ResponseWriter = w
	resp := &http.Response{StatusCode: 200}
	if err := p.AfterProxy(w, &rw, "t", "l", resp); err != nil {
		t.Errorf("AfterProxy: %v", err)
	}
	if w.Header().Get("X-Powered-By") != "tinyws" {
		t.Error("response_add not applied")
	}
	if w.Header().Get("Server") != "" {
		t.Error("response_remove not applied")
	}
	if w.Header().Get("Content-Type") != "text/plain" {
		t.Error("unrelated header removed")
	}
}

func TestAfterProxyNilMaps(t *testing.T) {
	p := newHeadersPlugin(t, map[string]any{})
	w := httptest.NewRecorder()
	w.Header().Set("X-Keep", "v")
	var rw http.ResponseWriter = w
	if err := p.AfterProxy(w, &rw, "t", "l", &http.Response{StatusCode: 200}); err != nil {
		t.Errorf("AfterProxy nil: %v", err)
	}
	if w.Header().Get("X-Keep") != "v" {
		t.Error("nil response_add should not touch headers")
	}
}
