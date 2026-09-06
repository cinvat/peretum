package cors

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newCORSPlugin(t *testing.T, cfg map[string]any) *CORSPlugin {
	t.Helper()
	p := NewCORSPlugin()
	if err := p.Init(cfg); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	return p
}

func fullCfg() map[string]any {
	return map[string]any{
		"enabled":           true,
		"allow_origins":     []any{"https://a.example.com", 123},
		"allow_methods":     []any{"GET", "POST"},
		"allow_headers":     []string{"X-Foo", "X-Bar"},
		"expose_headers":    []any{"X-Count"},
		"allow_credentials": true,
		"max_age":           600,
	}
}

func TestInit(t *testing.T) {
	p := newCORSPlugin(t, fullCfg())
	if !p.config.Enabled {
		t.Error("enabled not set")
	}
	if len(p.config.AllowOrigins) != 1 || p.config.AllowOrigins[0] != "https://a.example.com" {
		t.Errorf("AllowOrigins = %v", p.config.AllowOrigins)
	}
	if len(p.config.AllowMethods) != 2 || p.config.AllowMethods[0] != "GET" {
		t.Errorf("AllowMethods = %v", p.config.AllowMethods)
	}
	if len(p.config.AllowHeaders) != 2 {
		t.Errorf("AllowHeaders = %v", p.config.AllowHeaders)
	}
	if len(p.config.ExposeHeaders) != 1 || p.config.ExposeHeaders[0] != "X-Count" {
		t.Errorf("ExposeHeaders = %v", p.config.ExposeHeaders)
	}
	if !p.config.AllowCredentials {
		t.Error("AllowCredentials not set")
	}
	if p.config.MaxAge != 600 {
		t.Errorf("MaxAge = %d", p.config.MaxAge)
	}
}

func TestInitDefaultMethods(t *testing.T) {
	cfg := fullCfg()
	delete(cfg, "allow_methods")
	p := newCORSPlugin(t, cfg)
	if len(p.config.AllowMethods) == 0 {
		t.Error("default methods not applied")
	}
	if p.config.AllowMethods[0] != "GET" {
		t.Errorf("unexpected default methods: %v", p.config.AllowMethods)
	}
}

func TestInitStringSlice(t *testing.T) {
	cfg := fullCfg()
	// absent expose_headers -> getStringSlice returns nil (skip assignment)
	p := newCORSPlugin(t, cfg)
	_ = p

	p2 := newCORSPlugin(t, cfg)
	got := p2.getStringSlice(map[string]any{"k": []string{"a", "b"}}, "k")
	if len(got) != 2 {
		t.Errorf("string slice branch: %v", got)
	}
	got = p2.getStringSlice(map[string]any{"k": nil}, "k")
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestStartStop(t *testing.T) {
	p := newCORSPlugin(t, fullCfg())
	if err := p.Start(context.Background()); err != nil {
		t.Errorf("Start: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestAfterProxy(t *testing.T) {
	p := newCORSPlugin(t, fullCfg())
	if err := p.AfterProxy(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "t", "l", &http.Response{StatusCode: 200}); err != nil {
		t.Errorf("AfterProxy: %v", err)
	}
}

func TestBeforeProxyDisabled(t *testing.T) {
	cfg := fullCfg()
	cfg["enabled"] = false
	p := newCORSPlugin(t, cfg)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "https://a.example.com")
	if err := p.BeforeProxy(rec, req, "t", "l"); err != nil {
		t.Errorf("BeforeProxy disabled: %v", err)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("no CORS headers expected")
	}
}

func TestBeforeProxyNoOrigin(t *testing.T) {
	p := newCORSPlugin(t, fullCfg())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	if err := p.BeforeProxy(rec, req, "t", "l"); err != nil {
		t.Errorf("BeforeProxy no origin: %v", err)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("no CORS headers expected")
	}
}

func TestBeforeProxyOriginNotAllowed(t *testing.T) {
	p := newCORSPlugin(t, fullCfg())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	if err := p.BeforeProxy(rec, req, "t", "l"); err != nil {
		t.Errorf("BeforeProxy forbidden origin: %v", err)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("no CORS headers expected")
	}
}

func TestBeforeProxyAllowed(t *testing.T) {
	p := newCORSPlugin(t, fullCfg())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "https://a.example.com")
	if err := p.BeforeProxy(rec, req, "t", "l"); err != nil {
		t.Errorf("BeforeProxy allowed: %v", err)
	}
	h := rec.Header()
	if h.Get("Access-Control-Allow-Origin") != "https://a.example.com" {
		t.Errorf("missing allow-origin: %v", h)
	}
	if h.Get("Access-Control-Allow-Credentials") != "true" {
		t.Errorf("missing credentials header")
	}
	if h.Get("Access-Control-Allow-Methods") != "GET, POST" {
		t.Errorf("missing methods header: %q", h.Get("Access-Control-Allow-Methods"))
	}
	if h.Get("Access-Control-Allow-Headers") != "X-Foo, X-Bar" {
		t.Errorf("missing allow-headers header")
	}
	if h.Get("Access-Control-Expose-Headers") != "X-Count" {
		t.Errorf("missing expose-headers header")
	}
	if h.Get("Access-Control-Max-Age") != "600" {
		t.Errorf("missing max-age header")
	}
}

func TestBeforeProxyWildcardOrigin(t *testing.T) {
	cfg := fullCfg()
	cfg["allow_origins"] = []any{"*"}
	p := newCORSPlugin(t, cfg)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "https://any.example.com")
	if err := p.BeforeProxy(rec, req, "t", "l"); err != nil {
		t.Errorf("BeforeProxy wildcard: %v", err)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://any.example.com" {
		t.Error("wildcard origin not honored")
	}
}

func TestBeforeProxyPreflight(t *testing.T) {
	p := newCORSPlugin(t, fullCfg())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://a.example.com")
	err := p.BeforeProxy(rec, req, "t", "l")
	if err == nil || err.Error() != "cors_preflight" {
		t.Errorf("expected cors_preflight error, got %v", err)
	}
	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rec.Code)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://a.example.com" {
		t.Error("preflight missing allow-origin")
	}
}

func TestBeforeProxyNoAllowHeadersEcho(t *testing.T) {
	cfg := fullCfg()
	delete(cfg, "allow_headers")
	p := newCORSPlugin(t, cfg)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "https://a.example.com")
	req.Header.Set("Access-Control-Request-Headers", "X-Custom, X-Other")
	if err := p.BeforeProxy(rec, req, "t", "l"); err != nil {
		t.Errorf("BeforeProxy echo: %v", err)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "X-Custom, X-Other" {
		t.Errorf("expected echoed headers, got %q", got)
	}
}

func TestBeforeProxyNoRequestHeaders(t *testing.T) {
	cfg := fullCfg()
	delete(cfg, "allow_headers")
	p := newCORSPlugin(t, cfg)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "https://a.example.com")
	if err := p.BeforeProxy(rec, req, "t", "l"); err != nil {
		t.Errorf("BeforeProxy no req headers: %v", err)
	}
	if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "" {
		t.Errorf("unexpected allow-headers: %q", got)
	}
}
