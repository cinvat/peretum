package proxyserver

import (
	"github.com/cinvat/peretum/internal/config"
	"k8s.io/klog/v2"
)

func buildPluginConfigs(proxyCfg *config.ProxyConfig, targets []config.TargetConfig) map[string]map[string]any {
	configs := make(map[string]map[string]any)

	// Prometheus exporter
	if proxyCfg.MetricsAddr != "" {
		configs["prometheus_exporter"] = map[string]any{
			"enabled": true,
			"listen":  proxyCfg.MetricsAddr,
		}
	} else {
		// Default disabled
		configs["prometheus_exporter"] = map[string]any{
			"enabled": false,
		}
	}

	// JSON log plugin (nginx-style access + error logs)
	if proxyCfg.JSONLog != nil {
		configs["jsonlog"] = map[string]any{
			"enabled":    proxyCfg.JSONLog.Enabled,
			"access_log": proxyCfg.JSONLog.AccessLog,
			"error_log":  proxyCfg.JSONLog.ErrorLog,
			"stdout":     proxyCfg.JSONLog.Stdout,
		}
	}

	// Error page plugin (branded HTML error pages)
	if proxyCfg.ErrorPage != nil {
		m := map[string]any{"enabled": proxyCfg.ErrorPage.Enabled}
		if len(proxyCfg.ErrorPage.Statuses) > 0 {
			m["statuses"] = proxyCfg.ErrorPage.Statuses
		}
		configs["error_page"] = m
	}

	// WAF plugin: global GeoLite directory plus per-location policies
	// aggregated in one config, so BeforeProxy can pick the policy by
	// target|location instead of the last-wins pattern other plugins use.
	wafLocations := buildWAFLocations(targets)
	if len(wafLocations) > 0 {
		enabled := true
		if proxyCfg.WAF != nil {
			enabled = proxyCfg.WAF.Enabled
		}
		geodir := ""
		if proxyCfg.WAF != nil {
			geodir = proxyCfg.WAF.GeoLiteDir
		}
		configs["waf"] = map[string]any{
			"enabled":     enabled,
			"geolite_dir": geodir,
			"locations":   wafLocations,
		}
	} else {
		configs["waf"] = map[string]any{"enabled": false}
	}

	// Collect plugin configs from all locations across all targets
	for _, target := range targets {
		klog.V(2).Infof("buildPluginConfigs: target=%s, locations=%d", target.ServerName, len(target.Locations))
		for _, loc := range target.Locations {
			klog.V(2).Infof("buildPluginConfigs: loc=%s, Compression=%v, Optimize=%v, Rewrite=%v, CORS=%v, Headers=%v",
				loc.Path, loc.Compression, loc.Optimize, loc.Rewrite, loc.CORS, loc.Headers)
			// Compression
			if loc.Compression != nil {
				klog.V(2).Infof("buildPluginConfigs: loc.Compression type=%T, enabled=%v, types=%v", loc.Compression, loc.Compression.Enabled, loc.Compression.Types)
				configs["compression"] = structToMap(loc.Compression)
			}
			// Optimizer
			if loc.Optimize != nil {
				configs["optimizer"] = structToMap(loc.Optimize)
			}
			// Rewrite
			if loc.Rewrite != nil {
				configs["rewrite"] = structToMap(loc.Rewrite)
			}
			// CORS
			if loc.CORS != nil {
				configs["cors"] = structToMap(loc.CORS)
			}
			// Headers
			if loc.Headers != nil {
				configs["headers"] = structToMap(loc.Headers)
			}
		}
	}

	return configs
}

func structToMap(v any) map[string]any {
	switch v := v.(type) {
	case *config.CompressionConfig:
		m := make(map[string]any)
		if v.Enabled {
			m["enabled"] = true
			m["level"] = v.Level
			m["min_length"] = v.MinLength
			m["types"] = v.Types
		} else {
			m["enabled"] = false
		}
		return m
	case *config.OptimizeConfig:
		m := make(map[string]any)
		if v.Enabled {
			m["enabled"] = true
			m["minify_css"] = v.MinifyCSS
			m["minify_js"] = v.MinifyJS
			m["uglify_js"] = v.UglifyJS
			if v.Images != nil {
				m["images"] = map[string]any{
					"enabled":        v.Images.Enabled,
					"max_width":      v.Images.MaxWidth,
					"max_height":     v.Images.MaxHeight,
					"quality":        v.Images.Quality,
					"format":         v.Images.Format,
					"strip_metadata": v.Images.StripMetadata,
					"progressive":    v.Images.Progressive,
				}
			}
		} else {
			m["enabled"] = false
		}
		return m
	case *config.RewriteConfig:
		m := make(map[string]any)
		if v.Pattern != "" {
			m["enabled"] = true
			m["rules"] = []map[string]any{
				{
					"pattern":     v.Pattern,
					"replacement": v.Replacement,
					"break":       v.Break,
					"redirect":    v.Redirect,
				},
			}
		}
		return m
	case *config.CORSConfig:
		m := make(map[string]any)
		if v.Enabled {
			m["enabled"] = true
			m["allow_origins"] = v.AllowOrigins
			m["allow_methods"] = v.AllowMethods
			m["allow_headers"] = v.AllowHeaders
			m["expose_headers"] = v.ExposeHeaders
			m["allow_credentials"] = v.AllowCredentials
			m["max_age"] = v.MaxAge
		} else {
			m["enabled"] = false
		}
		return m
	case *config.HeadersConfig:
		m := make(map[string]any)
		if v.RequestAdd != nil {
			m["enabled"] = true
			m["request_add"] = v.RequestAdd
		}
		if len(v.RequestRemove) > 0 {
			m["enabled"] = true
			m["request_remove"] = v.RequestRemove
		}
		if v.ResponseAdd != nil {
			m["enabled"] = true
			m["response_add"] = v.ResponseAdd
		}
		if len(v.ResponseRemove) > 0 {
			m["enabled"] = true
			m["response_remove"] = v.ResponseRemove
		}
		return m
	default:
		return nil
	}
}

// buildWAFLocations collects every location that declares a WAF policy as an
// entry keyed by target + location path. The plugin uses target|location to
// select the policy at request time.
func buildWAFLocations(targets []config.TargetConfig) []any {
	var out []any
	for _, target := range targets {
		for _, loc := range target.Locations {
			if loc.WAF == nil {
				continue
			}
			out = append(out, map[string]any{
				"target":   target.ServerName,
				"location": loc.Path,
				"waf":      wafLocationToMap(loc.WAF),
			})
		}
	}
	return out
}

// wafLocationToMap converts the typed per-location WAF policy into the map
// form the plugin consumes. Rules keep their DNF ([][]WAFCondition) shape.
func wafLocationToMap(v *config.WAFLocationConfig) map[string]any {
	m := map[string]any{"enabled": v.Enabled}

	rules := make([]any, 0, len(v.Rules))
	for _, r := range v.Rules {
		rm := map[string]any{"id": r.ID}
		if r.Name != "" {
			rm["name"] = r.Name
		}
		if r.Enabled != nil {
			rm["enabled"] = *r.Enabled
		}
		if r.Action.Type != "" || r.Action.Code != 0 || r.Action.Message != "" {
			am := map[string]any{}
			if r.Action.Type != "" {
				am["type"] = r.Action.Type
			}
			if r.Action.Code != 0 {
				am["code"] = r.Action.Code
			}
			if r.Action.Message != "" {
				am["message"] = r.Action.Message
			}
			rm["action"] = am
		}
		groups := make([]any, 0, len(r.Conditions))
		for _, grp := range r.Conditions {
			conds := make([]any, 0, len(grp))
			for _, c := range grp {
				cm := map[string]any{"param": c.Param, "operator": c.Operator, "value": c.Value}
				if c.ParamName != "" {
					cm["param_name"] = c.ParamName
				}
				conds = append(conds, cm)
			}
			groups = append(groups, conds)
		}
		rm["conditions"] = groups
		rules = append(rules, rm)
	}
	if len(rules) > 0 {
		m["rules"] = rules
	}
	return m
}
