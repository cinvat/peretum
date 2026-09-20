package cmd

import (
	"fmt"

	"github.com/cinvat/peretum/internal/config"
)

// checkConfig validates the proxy and target configuration files without
// starting the server.
func checkConfig(cfgPath, targetsDir string) error {
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
	targets, err := config.LoadTargets(targetsDir)
	if err != nil {
		return fmt.Errorf("targets: %w", err)
	}
	for _, t := range targets {
		if len(t.Upstreams) > 0 {
			if _, err := t.ParseUpstreams(); err != nil {
				return fmt.Errorf("target %s: %w", t.Name, err)
			}
		}
		for _, loc := range t.Locations {
			if _, err := loc.ParseCacheTTL(); err != nil {
				return fmt.Errorf("target %s location %s cache_ttl: %w", t.Name, loc.Path, err)
			}
		}
	}
	fmt.Println("config OK")
	return nil
}
