package registry

import (
	"context"
	"sync"

	"github.com/cinvat/peretum/plugins/base"
	"github.com/cinvat/peretum/plugins/compression"
	"github.com/cinvat/peretum/plugins/cors"
	"github.com/cinvat/peretum/plugins/errorpage"
	"github.com/cinvat/peretum/plugins/headers"
	"github.com/cinvat/peretum/plugins/jsonlog"
	"github.com/cinvat/peretum/plugins/optimizer"
	"github.com/cinvat/peretum/plugins/prometheus"
	"github.com/cinvat/peretum/plugins/rewrite"
	"github.com/cinvat/peretum/plugins/waf"
)

var (
	once     sync.Once
	registry *Registry
)

func init() {
	once.Do(func() {
		registerDefaultFactories()
	})
}

// registerDefaultFactories registers the built-in plugins. It is called from
// init() and can be re-seeded by tests that reset the registry.
func registerDefaultFactories() {
	Register("cors", func() base.Plugin { return cors.NewCORSPlugin() })
	Register("headers", func() base.Plugin { return headers.NewHeadersPlugin() })
	Register("rewrite", func() base.Plugin { return rewrite.NewRewritePlugin() })
	Register("compression", func() base.Plugin { return compression.NewCompressionPlugin() })
	Register("optimizer", func() base.Plugin { return optimizer.NewOptimizerPlugin() })
	Register("jsonlog", func() base.Plugin { return jsonlog.NewJSONLogPlugin() })
	Register("prometheus_exporter", func() base.Plugin { return prometheus.NewPrometheusPlugin() })
	Register("error_page", func() base.Plugin { return errorpage.NewErrorPagePlugin() })
	Register("waf", func() base.Plugin { return waf.NewWAFPlugin() })
}

func LoadAndInitializePlugins(ctx context.Context, configs map[string]map[string]any) ([]base.Plugin, error) {
	plugins, err := LoadPlugins(ctx, configs)
	if err != nil {
		return nil, err
	}

	if err := InitializeAndStart(ctx, plugins, configs); err != nil {
		return nil, err
	}

	return plugins, nil
}

func StopAllPlugins(ctx context.Context, plugins []base.Plugin) error {
	return StopAll(ctx, plugins)
}
