package cors

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"k8s.io/klog/v2"

	"github.com/cinvat/peretum/plugins/base"
)

type CORSPlugin struct {
	*base.BasePlugin
	config CORSConfig
}

type CORSConfig struct {
	Enabled          bool
	AllowOrigins     []string
	AllowMethods     []string
	AllowHeaders     []string
	ExposeHeaders    []string
	AllowCredentials bool
	MaxAge           int
}

func NewCORSPlugin() *CORSPlugin {
	return &CORSPlugin{
		BasePlugin: base.NewBasePlugin("cors"),
	}
}

func (p *CORSPlugin) Init(config map[string]any) error {
	p.BasePlugin.Init(config)

	p.config.Enabled = config["enabled"].(bool)
	p.config.AllowOrigins = p.getStringSlice(config, "allow_origins")
	p.config.AllowMethods = p.getStringSlice(config, "allow_methods")
	p.config.AllowHeaders = p.getStringSlice(config, "allow_headers")
	p.config.ExposeHeaders = p.getStringSlice(config, "expose_headers")
	p.config.AllowCredentials = config["allow_credentials"].(bool)
	p.config.MaxAge = config["max_age"].(int)

	if len(p.config.AllowMethods) == 0 {
		p.config.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"}
	}

	klog.Infof("CORS plugin initialized: origins=%v, methods=%v, credentials=%v",
		p.config.AllowOrigins, p.config.AllowMethods, p.config.AllowCredentials)

	return nil
}

func (p *CORSPlugin) Start(ctx context.Context) error { return nil }
func (p *CORSPlugin) Stop(ctx context.Context) error  { return nil }

func (p *CORSPlugin) BeforeProxy(w http.ResponseWriter, r *http.Request, target, location string) error {
	if !p.config.Enabled {
		return nil
	}

	origin := r.Header.Get("Origin")
	if origin == "" {
		return nil
	}

	allowed := false
	for _, o := range p.config.AllowOrigins {
		if o == "*" || o == origin {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil
	}

	// Vary: Origin is required for correct caching when CORS is enabled.
	w.Header().Add("Vary", "Origin")

	w.Header().Set("Access-Control-Allow-Origin", origin)

	if p.config.AllowCredentials {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}

	if len(p.config.AllowMethods) > 0 {
		w.Header().Set("Access-Control-Allow-Methods", strings.Join(p.config.AllowMethods, ", "))
	}

	if len(p.config.AllowHeaders) > 0 {
		w.Header().Set("Access-Control-Allow-Headers", strings.Join(p.config.AllowHeaders, ", "))
	} else {
		reqHeaders := r.Header.Get("Access-Control-Request-Headers")
		if reqHeaders != "" {
			w.Header().Set("Access-Control-Allow-Headers", reqHeaders)
		}
	}

	if len(p.config.ExposeHeaders) > 0 {
		w.Header().Set("Access-Control-Expose-Headers", strings.Join(p.config.ExposeHeaders, ", "))
	}

	if p.config.MaxAge > 0 {
		w.Header().Set("Access-Control-Max-Age", fmt.Sprintf("%d", p.config.MaxAge))
	}

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return fmt.Errorf("cors_preflight")
	}

	return nil
}

func (p *CORSPlugin) AfterProxy(w http.ResponseWriter, r *http.Request, target, location string, resp *http.Response) error {
	return nil
}

func (p *CORSPlugin) getStringSlice(config map[string]any, key string) []string {
	if v, ok := config[key].([]any); ok {
		result := make([]string, 0, len(v))
		for _, item := range v {
			if str, ok := item.(string); ok {
				result = append(result, str)
			}
		}
		return result
	}
	if v, ok := config[key].([]string); ok {
		return v
	}
	return nil
}
