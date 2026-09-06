package rewrite

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newRewritePlugin(t *testing.T, cfg map[string]any) *RewritePlugin {
	t.Helper()
	p := NewRewritePlugin()
	if err := p.Init(cfg); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	return p
}

func rulesCfg() map[string]any {
	return map[string]any{
		"rules": []any{
			map[string]any{
				"pattern":     `^/api/v1/(.*)`,
				"replacement": "/v2/$1",
				"break":       false,
			},
			"not a map",
			map[string]any{
				"pattern": "^/only-pattern$",
			},
			map[string]any{
				"replacement": "/no-pattern",
			},
			map[string]any{
				"pattern":     `^/bad/[`,
				"replacement": "/x/$1",
			},
			map[string]any{
				"pattern":     `^/legacy/(.*)`,
				"replacement": "/new/$1",
				"redirect":    "permanent",
			},
			map[string]any{
				"pattern":     `^/tmp/(.*)`,
				"replacement": "/temp/$1",
				"break":       true,
				"redirect":    "redirect",
			},
		},
	}
}

func TestInit(t *testing.T) {
	p := newRewritePlugin(t, rulesCfg())
	// valid rules registered: rules 0, 5, 6 (others skipped)
	if _, ok := p.rules["rule_0"]; !ok {
		t.Error("rule_0 missing")
	}
	if _, ok := p.rules["rule_5"]; !ok {
		t.Error("rule_5 missing")
	}
	if _, ok := p.rules["rule_6"]; !ok {
		t.Error("rule_6 missing")
	}
	for _, i := range []string{"rule_1", "rule_2", "rule_3", "rule_4"} {
		if _, ok := p.rules[i]; ok {
			t.Errorf("%s should not be registered", i)
		}
	}
	if p.rules["rule_0"].Replacement != "/v2/$1" {
		t.Error("rule_0 replacement mismatch")
	}
	if p.rules["rule_6"].Redirect != "redirect" {
		t.Error("rule_6 redirect mismatch")
	}
}

func TestInitNoRules(t *testing.T) {
	p := newRewritePlugin(t, map[string]any{})
	if len(p.rules) != 0 {
		t.Errorf("expected no rules, got %v", p.rules)
	}
}

func TestStartStop(t *testing.T) {
	p := newRewritePlugin(t, rulesCfg())
	if err := p.Start(context.Background()); err != nil {
		t.Errorf("Start: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestAfterProxy(t *testing.T) {
	p := newRewritePlugin(t, rulesCfg())
	var rw http.ResponseWriter
	if err := p.AfterProxy(httptest.NewRecorder(), &rw, "t", "l", &http.Response{StatusCode: 200}); err != nil {
		t.Errorf("AfterProxy: %v", err)
	}
}

func TestBeforeProxyRewrite(t *testing.T) {
	p := newRewritePlugin(t, map[string]any{
		"rules": []any{
			map[string]any{
				"pattern":     `^/api/v1/(.*)`,
				"replacement": "/v2/$1",
				"break":       false,
			},
		},
	})
	r := httptest.NewRequest("GET", "/api/v1/users/1", nil)
	if err := p.BeforeProxy(httptest.NewRecorder(), r, "t", "l"); err != nil {
		t.Errorf("BeforeProxy rewrite: %v", err)
	}
	if r.URL.Path != "/v2/users/1" {
		t.Errorf("path = %q", r.URL.Path)
	}
}

func TestBeforeProxyNoMatch(t *testing.T) {
	p := newRewritePlugin(t, map[string]any{
		"rules": []any{
			map[string]any{
				"pattern":     `^/api/v1/(.*)`,
				"replacement": "/v2/$1",
			},
		},
	})
	r := httptest.NewRequest("GET", "/other", nil)
	if err := p.BeforeProxy(httptest.NewRecorder(), r, "t", "l"); err != nil {
		t.Errorf("BeforeProxy no-match: %v", err)
	}
	if r.URL.Path != "/other" {
		t.Errorf("path changed: %q", r.URL.Path)
	}
}

func TestBeforeProxyIdentityReplacement(t *testing.T) {
	p := newRewritePlugin(t, map[string]any{
		"rules": []any{
			map[string]any{
				"pattern":     `^/same$`,
				"replacement": "/same",
			},
		},
	})
	r := httptest.NewRequest("GET", "/same", nil)
	if err := p.BeforeProxy(httptest.NewRecorder(), r, "t", "l"); err != nil {
		t.Errorf("BeforeProxy identity: %v", err)
	}
	if r.URL.Path != "/same" {
		t.Errorf("path should be unchanged: %q", r.URL.Path)
	}
}

func TestBeforeProxyBreak(t *testing.T) {
	p := newRewritePlugin(t, map[string]any{
		"rules": []any{
			map[string]any{
				"pattern":     `^/break/(.*)`,
				"replacement": "/broke/$1",
				"break":       true,
			},
		},
	})
	r := httptest.NewRequest("GET", "/break/x", nil)
	if err := p.BeforeProxy(httptest.NewRecorder(), r, "t", "l"); err != nil {
		t.Errorf("BeforeProxy break: %v", err)
	}
	if r.URL.Path != "/broke/x" {
		t.Errorf("path = %q", r.URL.Path)
	}
}

func TestBeforeProxyRedirectPermanent(t *testing.T) {
	p := newRewritePlugin(t, map[string]any{
		"rules": []any{
			map[string]any{
				"pattern":     `^/legacy/(.*)`,
				"replacement": "/new/$1",
				"redirect":    "permanent",
			},
		},
	})
	rec := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/legacy/foo", nil)
	err := p.BeforeProxy(rec, r, "t", "l")
	if err == nil || err.Error() != "redirect" {
		t.Fatalf("expected redirect error, got %v", err)
	}
	if rec.Code != http.StatusMovedPermanently {
		t.Errorf("status = %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/new/foo" {
		t.Errorf("Location = %q", loc)
	}
}

func TestBeforeProxyRedirectFoundWithQuery(t *testing.T) {
	p := newRewritePlugin(t, map[string]any{
		"rules": []any{
			map[string]any{
				"pattern":     `^/tmp/(.*)`,
				"replacement": "/temp/$1",
				"redirect":    "redirect",
			},
		},
	})
	rec := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/tmp/a?x=1&y=2", nil)
	err := p.BeforeProxy(rec, r, "t", "l")
	if err == nil || err.Error() != "redirect" {
		t.Fatalf("expected redirect error, got %v", err)
	}
	if rec.Code != http.StatusFound {
		t.Errorf("status = %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/temp/a?x=1&y=2" {
		t.Errorf("Location = %q", loc)
	}
	if r.URL.Path != "/tmp/a" {
		t.Errorf("original path should be preserved, got %q", r.URL.Path)
	}
}
