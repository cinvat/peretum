package ratelimit

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/cinvat/peretum/plugins/base"
)

type RateLimitPlugin struct {
	*base.BasePlugin
	store   *Store
	rps     float64
	burst   int
	enabled bool
}

func NewRateLimitPlugin() *RateLimitPlugin {
	return &RateLimitPlugin{
		BasePlugin: base.NewBasePlugin("ratelimit"),
		store:      NewStore(),
		rps:        100,
		burst:      200,
	}
}

func (p *RateLimitPlugin) Init(config map[string]any) error {
	p.BasePlugin.Init(config)
	p.enabled = base.GetBool(config, "enabled")
	if v, ok := config["rps"].(float64); ok && v > 0 {
		p.rps = v
	}
	if v, ok := config["requests_per_second"].(float64); ok && v > 0 {
		p.rps = v
	}
	if v, ok := config["burst"].(int); ok && v > 0 {
		p.burst = v
	}
	if v, ok := config["burst"].(float64); ok && v > 0 {
		p.burst = int(v)
	}
	return nil
}

func (p *RateLimitPlugin) Start(ctx context.Context) error { return nil }
func (p *RateLimitPlugin) Stop(ctx context.Context) error  { return nil }

func (p *RateLimitPlugin) key(r *http.Request, target string) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return target + "|" + host
}

func (p *RateLimitPlugin) limiter(key string) bool {
	return p.store.Allow(key, p.rps, p.burst)
}

func (p *RateLimitPlugin) BeforeProxy(w http.ResponseWriter, r *http.Request, target, location string) error {
	if !p.enabled {
		return nil
	}
	l := p.limiter(p.key(r, target))
	if !l {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		return fmt.Errorf("ratelimit_exceeded")
	}
	return nil
}

func (p *RateLimitPlugin) AfterProxy(w http.ResponseWriter, r *http.Request, target, location string, resp *http.Response) error {
	return nil
}
