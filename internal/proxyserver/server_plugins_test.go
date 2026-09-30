package proxyserver

import (
	"testing"

	"github.com/cinvat/peretum/internal/config"
)

func TestBuildPluginConfigs(t *testing.T) {
	proxyCfg := &config.ProxyConfig{
		MetricsAddr: ":9090",
		JSONLog:     &config.JSONLogConfig{Enabled: true, AccessLog: "/a", ErrorLog: "/e", Stdout: true},
		ErrorPage:   &config.ErrorPageConfig{Enabled: true, Statuses: []int{404, 502}},
	}
	targets := []config.TargetConfig{
		{
			ServerName: "lean",
			Locations: []config.LocationConfig{
				{
					Path:        "/l",
					Compression: &config.CompressionConfig{Enabled: false},
					Optimize:    &config.OptimizeConfig{Enabled: true},
					Rewrite:     &config.RewriteConfig{},
					CORS:        &config.CORSConfig{Enabled: false},
					Headers:     &config.HeadersConfig{},
				},
				{Path: "/d", Optimize: &config.OptimizeConfig{Enabled: false}},
			},
		},
		{
			ServerName: "full",
			Locations: []config.LocationConfig{
				{
					Path:        "/f",
					Compression: &config.CompressionConfig{Enabled: true, Level: 5, MinLength: 100, Types: []string{"text/html"}},
					Optimize: &config.OptimizeConfig{
						Enabled: true, MinifyCSS: true, MinifyJS: true, UglifyJS: true,
						Images: &config.ImageOptimizeConfig{Enabled: true, MaxWidth: 800},
					},
					Rewrite: &config.RewriteConfig{Pattern: "^/x", Replacement: "/y", Break: true, Redirect: "permanent"},
					CORS:    &config.CORSConfig{Enabled: true, AllowOrigins: []string{"*"}},
					Headers: &config.HeadersConfig{
						RequestAdd:     map[string]string{"A": "b"},
						RequestRemove:  []string{"RA"},
						ResponseAdd:    map[string]string{"C": "d"},
						ResponseRemove: []string{"RC"},
					},
				},
			},
		},
	}

	configs := buildPluginConfigs(proxyCfg, targets)

	if v, ok := configs["prometheus_exporter"]; !ok || v["enabled"] != true || v["listen"] != ":9090" {
		t.Fatalf("prometheus config = %v", configs["prometheus_exporter"])
	}
	if v, ok := configs["jsonlog"]; !ok || v["enabled"] != true || v["access_log"] != "/a" || v["error_log"] != "/e" || v["stdout"] != true {
		t.Fatalf("jsonlog config = %v", configs["jsonlog"])
	}

	if v, ok := configs["compression"]; !ok || v["enabled"] != true || v["level"] != 5 || v["min_length"] != 100 {
		t.Fatalf("compression config = %v", configs["compression"])
	}
	if v, ok := configs["optimizer"]; !ok || v["enabled"] != true || v["minify_css"] != true || v["uglify_js"] != true {
		t.Fatalf("optimizer config = %v", configs["optimizer"])
	}
	im, ok := configs["optimizer"]["images"].(map[string]any)
	if !ok || im["max_width"] != 800 {
		t.Fatalf("optimizer images = %v", configs["optimizer"]["images"])
	}
	if v, ok := configs["rewrite"]; !ok || v["enabled"] != true {
		t.Fatalf("rewrite config = %v", configs["rewrite"])
	} else {
		rules, ok := v["rules"].([]map[string]any)
		if !ok || len(rules) != 1 || rules[0]["break"] != true || rules[0]["redirect"] != "permanent" ||
			rules[0]["pattern"] != "^/x" || rules[0]["replacement"] != "/y" {
			t.Fatalf("rewrite rules = %v", v["rules"])
		}
	}
	if v, ok := configs["cors"]; !ok || v["enabled"] != true || len(v["allow_origins"].([]string)) != 1 {
		t.Fatalf("cors config = %v", configs["cors"])
	}
	if v, ok := configs["headers"]; !ok || v["enabled"] != true ||
		v["request_add"] == nil || len(v["request_remove"].([]string)) != 1 ||
		v["response_add"] == nil || len(v["response_remove"].([]string)) != 1 {
		t.Fatalf("headers config = %v", configs["headers"])
	}

	if v, ok := configs["error_page"]; !ok || v["enabled"] != true {
		t.Fatalf("error_page config = %v", configs["error_page"])
	} else {
		st, ok := v["statuses"].([]int)
		if !ok || len(st) != 2 || st[0] != 404 || st[1] != 502 {
			t.Fatalf("error_page statuses = %v", v["statuses"])
		}
	}
}

func TestBuildPluginConfigsDisabledPrometheus(t *testing.T) {
	proxyCfg := &config.ProxyConfig{}
	targets := []config.TargetConfig{{ServerName: "x", Locations: []config.LocationConfig{{Path: "/"}}}}
	configs := buildPluginConfigs(proxyCfg, targets)
	if v, ok := configs["prometheus_exporter"]; !ok || v["enabled"] != false {
		t.Fatalf("prometheus disabled config = %v", configs["prometheus_exporter"])
	}
	if _, ok := configs["jsonlog"]; ok {
		t.Fatalf("jsonlog should be absent, got %v", configs["jsonlog"])
	}
	if _, ok := configs["error_page"]; ok {
		t.Fatalf("error_page should be absent, got %v", configs["error_page"])
	}
}

func TestBuildPluginConfigsErrorPageDefaults(t *testing.T) {
	proxyCfg := &config.ProxyConfig{ErrorPage: &config.ErrorPageConfig{Enabled: false}}
	targets := []config.TargetConfig{}
	configs := buildPluginConfigs(proxyCfg, targets)
	v, ok := configs["error_page"]
	if !ok {
		t.Fatalf("error_page config missing: %v", configs)
	}
	if v["enabled"] != false {
		t.Fatalf("error_page enabled = %v, want false", v["enabled"])
	}
	if _, hasStatuses := v["statuses"]; hasStatuses {
		t.Fatalf("error_page with no statuses must not set key, got %v", v)
	}
}

func TestBuildPluginConfigsWAF(t *testing.T) {
	enabled := true
	proxyCfg := &config.ProxyConfig{
		WAF: &config.WAFConfig{Enabled: true, GeoLiteDir: "/var/lib/geolite"},
	}
	targets := []config.TargetConfig{
		{
			ServerName: "full",
			Locations: []config.LocationConfig{
				{
					Path: "/f",
					WAF: &config.WAFLocationConfig{
						Enabled: true,
						Rules: []config.WAFRule{
							{
								ID: "r1", Name: "name1", Enabled: &enabled,
								Action: config.WAFRuleAction{Type: "deny", Code: 451, Message: "msg1"},
								Conditions: [][]config.WAFCondition{
									{{Param: "header", ParamName: "Content-Length", Operator: "gt", Value: "100"}},
									{{Param: "user_agent", Operator: "contains", Value: "safari"}},
								},
							},
							{ID: "r2"},
						},
					},
				},
				{Path: "/plain"}, // no WAF: must not appear in locations
			},
		},
		{
			ServerName: "lean",
			Locations: []config.LocationConfig{
				{Path: "/l", WAF: &config.WAFLocationConfig{Enabled: false}},
			},
		},
	}

	configs := buildPluginConfigs(proxyCfg, targets)
	v, ok := configs["waf"]
	if !ok {
		t.Fatalf("waf config missing: %v", configs)
	}
	if v["enabled"] != true || v["geolite_dir"] != "/var/lib/geolite" {
		t.Fatalf("waf global = %v", v)
	}

	locs, ok := v["locations"].([]any)
	if !ok || len(locs) != 2 {
		t.Fatalf("waf locations = %v", v["locations"])
	}

	first := locs[0].(map[string]any)
	if first["target"] != "full" || first["location"] != "/f" {
		t.Fatalf("first location = %v", first)
	}
	wm := first["waf"].(map[string]any)
	if wm["enabled"] != true {
		t.Fatalf("waf location = %v", wm)
	}
	rules, ok := wm["rules"].([]any)
	if !ok || len(rules) != 2 {
		t.Fatalf("waf rules = %v", wm["rules"])
	}
	r1, _ := rules[0].(map[string]any)
	if r1["id"] != "r1" || r1["name"] != "name1" || r1["enabled"] != true {
		t.Fatalf("r1 = %v", r1)
	}
	am, _ := r1["action"].(map[string]any)
	if am["type"] != "deny" || am["code"] != 451 || am["message"] != "msg1" {
		t.Fatalf("r1 action = %v", am)
	}
	cg, ok := r1["conditions"].([]any)
	if !ok || len(cg) != 2 {
		t.Fatalf("r1 groups = %v", r1["conditions"])
	}
	cond0, _ := cg[0].([]any)
	cm0, _ := cond0[0].(map[string]any)
	if cm0["param"] != "header" || cm0["param_name"] != "Content-Length" || cm0["operator"] != "gt" || cm0["value"] != "100" {
		t.Fatalf("r1 cond = %v", cm0)
	}
	cond1, _ := cg[1].([]any)
	cm1, _ := cond1[0].(map[string]any)
	if _, has := cm1["param_name"]; has {
		t.Fatalf("empty param_name must be omitted, got %v", cm1)
	}

	// Disabled location still gets a waf block (buildRuleSet skips it later).
	second := locs[1].(map[string]any)
	if second["target"] != "lean" || second["location"] != "/l" {
		t.Fatalf("second location = %v", second)
	}
	sm, _ := second["waf"].(map[string]any)
	if sm["enabled"] != false {
		t.Fatalf("disabled location waf = %v", sm)
	}
}

func TestBuildPluginConfigsWAFNoGlobal(t *testing.T) {
	// Global waf block absent but a location declares a policy: the plugin is
	// considered enabled with an empty geolite directory.
	targets := []config.TargetConfig{{
		ServerName: "x",
		Locations: []config.LocationConfig{
			{Path: "/", WAF: &config.WAFLocationConfig{Enabled: true, Rules: []config.WAFRule{}}},
		},
	}}
	configs := buildPluginConfigs(&config.ProxyConfig{}, targets)
	v, ok := configs["waf"]
	if !ok {
		t.Fatalf("waf config missing: %v", configs)
	}
	if v["enabled"] != true || v["geolite_dir"] != "" {
		t.Fatalf("waf no-global = %v", v)
	}
	if locs := v["locations"].([]any); len(locs) != 1 {
		t.Fatalf("waf locations = %v", v["locations"])
	}
}

func TestBuildPluginConfigsWAFNone(t *testing.T) {
	targets := []config.TargetConfig{{ServerName: "x", Locations: []config.LocationConfig{{Path: "/"}}}}
	configs := buildPluginConfigs(&config.ProxyConfig{}, targets)
	v, ok := configs["waf"]
	if !ok {
		t.Fatalf("waf config missing: %v", configs)
	}
	if v["enabled"] != false {
		t.Fatalf("waf none = %v", v)
	}
	if _, has := v["locations"]; has {
		t.Fatalf("no locations expected, got %v", v)
	}
}

func TestStructToMap(t *testing.T) {
	if m := structToMap(42); m != nil {
		t.Fatalf("expected nil for unsupported type, got %v", m)
	}
	if m := structToMap(&config.UpstreamConfig{URL: "http://x"}); m != nil {
		t.Fatalf("expected nil for pointer-to-unsupported type, got %v", m)
	}
	if m := structToMap(&config.OptimizeConfig{Enabled: false}); m == nil || m["enabled"] != false {
		t.Fatalf("optimize disabled = %v", m)
	}
	if m := structToMap(&config.RewriteConfig{}); m == nil {
		t.Fatal("rewrite empty map expected")
	} else if m["enabled"] != nil {
		t.Fatalf("rewrite should have no enabled key, got %v", m)
	}
	if m := structToMap(&config.HeadersConfig{}); m == nil {
		t.Fatalf("headers empty map expected, got nil")
	}
}

func TestBuildPluginConfigsRateLimit(t *testing.T) {
	proxyCfg := &config.ProxyConfig{RateLimit: &config.RateLimitConfig{Enabled: true, RPS: 10, Burst: 20}}
	configs := buildPluginConfigs(proxyCfg, nil)
	v, ok := configs["ratelimit"]
	if !ok {
		t.Fatalf("ratelimit config missing: %v", configs)
	}
	if v["enabled"] != true || v["rps"] != 10.0 || v["burst"] != 20 {
		t.Fatalf("ratelimit = %v, want enabled/rps=10/burst=20", v)
	}

	configs = buildPluginConfigs(&config.ProxyConfig{}, nil)
	if v := configs["ratelimit"]; v["enabled"] != false {
		t.Fatalf("absent rate_limit must be disabled, got %v", v)
	}
}

func TestWAFLocationsPassRateLimitParams(t *testing.T) {
	rps := 5.0
	targets := []config.TargetConfig{{
		ServerName: "t",
		Locations: []config.LocationConfig{{
			Path: "/",
			WAF: &config.WAFLocationConfig{Enabled: true, Rules: []config.WAFRule{{
				ID:         "rl",
				Action:     config.WAFRuleAction{Type: "rate_limit", RPS: rps, Burst: 7},
				Conditions: [][]config.WAFCondition{{{Param: "path", Operator: "startswith", Value: "/"}}},
			}}},
		}},
	}}
	locs := buildWAFLocations(targets)
	if len(locs) != 1 {
		t.Fatalf("locations = %d, want 1", len(locs))
	}
	rule := locs[0].(map[string]any)["waf"].(map[string]any)["rules"].([]any)[0].(map[string]any)
	am := rule["action"].(map[string]any)
	if am["type"] != "rate_limit" || am["rps"] != rps || am["burst"] != 7 {
		t.Fatalf("action = %v, want rate_limit/rps=5/burst=7", am)
	}
}
