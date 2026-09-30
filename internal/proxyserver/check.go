package proxyserver

import (
	"fmt"
	"strings"

	"github.com/cinvat/peretum/internal/config"
)

// checkConfig validates the proxy and target configuration files without
// starting the server.
func CheckConfig(cfgPath, targetsDir string) error {
	proxyCfg, err := config.LoadProxy(cfgPath)
	if err != nil {
		return fmt.Errorf("proxy config: %w", err)
	}
	if proxyCfg.MaxCacheSize != "" {
		if _, err := proxyCfg.ParseMaxCacheSize(); err != nil {
			return fmt.Errorf("max_cache_size: %w", err)
		}
	}
	if proxyCfg.MaxCacheAge != "" {
		if _, err := proxyCfg.ParseMaxCacheAge(); err != nil {
			return fmt.Errorf("max_cache_age: %w", err)
		}
	}
	if proxyCfg.MaxResponseBodySize != "" {
		if _, err := proxyCfg.ParseMaxResponseBodySize(); err != nil {
			return fmt.Errorf("max_response_body_size: %w", err)
		}
	}
	if proxyCfg.MaxWriteWorkers < -1 {
		return fmt.Errorf("max_write_workers: must be -1 (unlimited) or greater")
	}
	if _, err := proxyCfg.ParseListeners(); err != nil {
		return fmt.Errorf("listeners: %w", err)
	}
	if rl := proxyCfg.RateLimit; rl != nil {
		if rl.RPS < 0 {
			return fmt.Errorf("rate_limit.rps: must be >= 0")
		}
		if rl.Burst < 0 {
			return fmt.Errorf("rate_limit.burst: must be >= 0")
		}
	}
	if c := proxyCfg.Cluster; c != nil && c.Enabled {
		if c.Lazy {
			if c.DataDir == "" {
				return fmt.Errorf("cluster.data_dir: required when cluster.lazy is true")
			}
			if c.LRUSize < 0 {
				return fmt.Errorf("cluster.lru_size: must be >= 0")
			}
		}
		if c.NATSURI != "" {
			if !strings.HasPrefix(c.NATSURI, "nats://") && !strings.HasPrefix(c.NATSURI, "tls://") {
				return fmt.Errorf("cluster.nats_uri: must start with nats:// or tls://")
			}
		}
		if c.ReplayTimeout < 0 {
			return fmt.Errorf("cluster.replay_timeout: must be >= 0")
		}
	}
	targets, err := config.LoadTargets(targetsDir)
	if err != nil {
		return fmt.Errorf("targets: %w", err)
	}
	for _, t := range targets {
		if len(t.Upstreams) > 0 {
			if _, err := t.ParseUpstreams(); err != nil {
				return fmt.Errorf("target %s: %w", t.ServerName, err)
			}
		}
		for _, loc := range t.Locations {
			if _, err := loc.ParseCacheTTL(); err != nil {
				return fmt.Errorf("target %s location %s cache_ttl: %w", t.ServerName, loc.Path, err)
			}
			if loc.WAF != nil {
				for _, r := range loc.WAF.Rules {
					at := strings.ToLower(strings.ReplaceAll(r.Action.Type, "-", "_"))
					if at == "rate_limit" || at == "ratelimit" {
						if r.Action.RPS < 0 {
							return fmt.Errorf("target %s location %s rule %s: rps must be >= 0", t.ServerName, loc.Path, r.ID)
						}
						if r.Action.Burst < 0 {
							return fmt.Errorf("target %s location %s rule %s: burst must be >= 0", t.ServerName, loc.Path, r.ID)
						}
					}
				}
			}
		}
	}
	fmt.Println("config OK")
	return nil
}
