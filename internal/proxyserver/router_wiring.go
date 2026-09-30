package proxyserver

import (
	"context"
	"fmt"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/handler"
	"github.com/cinvat/peretum/internal/loadbalancer"
	"github.com/cinvat/peretum/internal/router"
	"github.com/cinvat/peretum/plugins/base"
	"github.com/samber/lo"
	"gopkg.in/yaml.v3"
	"k8s.io/klog/v2"
	"net/http"
	"time"
)

func (ps *proxyServer) buildHostRouter() *router.HostRouter {
	// Lazy mode routes hosts through LazyHandler stubs; configs live in the
	// Pebble store and are materialized on first request.
	if ps.targetStore != nil {
		return ps.buildLazyHostRouter()
	}

	hr := router.NewHostRouter()
	targets := make(map[string]http.Handler)
	var defaultHandler http.Handler

	// Build new health checkers for this configuration.
	newHealthCheckers := make(map[string]*loadbalancer.HealthChecker)

	for i := range ps.targets {
		target := &ps.targets[i]
		tch, hc, err := ps.buildTargetConfigHandler(target)
		if err != nil {
			klog.Warningf("failed to build target %s: %v", target.ServerName, err)
			continue
		}
		if hc != nil {
			newHealthCheckers[target.ServerName] = hc
			klog.Infof("enabled active health checks for target %s", target.ServerName)
		}

		hostKey := targetHostKey(target)
		targets[hostKey] = tch

		if defaultHandler == nil && tch.DefaultLoc != nil {
			defaultHandler = tch.DefaultLoc
		}
	}

	// Stop old health checkers that are no longer in the new config.
	for name, oldHC := range ps.healthCheckers {
		if _, ok := newHealthCheckers[name]; !ok {
			oldHC.Stop()
			klog.Infof("stopped health checker for target %s", name)
		}
	}
	// Start new health checkers.
	for name, newHC := range newHealthCheckers {
		if _, ok := ps.healthCheckers[name]; !ok {
			newHC.Start(context.Background())
			klog.Infof("started health checker for target %s", name)
		}
	}
	ps.healthCheckers = newHealthCheckers

	hr.Reload(targets, defaultHandler)
	return hr
}

// buildTargetConfigHandler compiles a target into its per-location handlers.
// It returns the compiled handler plus an (unstarted) health checker if any
// upstream has active health checks configured, leaving the checker lifecycle
// to the caller.
func (ps *proxyServer) buildTargetConfigHandler(target *config.TargetConfig) (*router.TargetConfigHandler, *loadbalancer.HealthChecker, error) {
	upstreams, err := target.ParseUpstreams()
	if err != nil {
		return nil, nil, fmt.Errorf("parse upstreams: %w", err)
	}

	// Build URL -> UpstreamConfig map for O(1) lookup instead of O(n*m) nested loop
	ucByURL := lo.SliceToMap(target.Upstreams, func(uc config.UpstreamConfig) (string, config.UpstreamConfig) {
		return uc.URL, uc
	})

	var lbUpstreams []*loadbalancer.Upstream
	for _, u := range upstreams {
		if uc, ok := ucByURL[u.String()]; ok {
			lbUpstreams = append(lbUpstreams, &loadbalancer.Upstream{
				URL:    u.String(),
				Weight: uc.Weight,
			})
		}
	}
	lb := loadbalancer.New(target.LBAlgorithm, lbUpstreams)

	// Create health checker if any upstream has health_check configured.
	var hc *loadbalancer.HealthChecker
	for _, uc := range target.Upstreams {
		if uc.HealthCheck != nil && uc.HealthCheck.Path != "" {
			hc = loadbalancer.NewHealthChecker(lb, buildHealthCheckConfig(uc.HealthCheck))
			break
		}
	}

	var handlers []*handler.TargetHandler
	var defaultLoc *handler.TargetHandler
	for j := range target.Locations {
		loc := &target.Locations[j]
		h := handler.NewTargetHandler(target, loc, ps.diskCache, ps.writeSem, lb, ps.pluginMgr, ps.maxBodySize)
		handlers = append(handlers, h)

		if loc.Path == "/" && defaultLoc == nil {
			defaultLoc = h
		}
	}

	return &router.TargetConfigHandler{
		Target:     target,
		Handlers:   handlers,
		DefaultLoc: defaultLoc,
	}, hc, nil
}

// buildHealthCheckConfig normalizes a target's health_check block into the
// loadbalancer config with defaults applied.
func buildHealthCheckConfig(uc *config.HealthCheckConfig) *loadbalancer.HealthCheckConfig {
	interval := 10 * time.Second
	if uc.Interval != "" {
		if d, err := time.ParseDuration(uc.Interval); err == nil {
			interval = d
		}
	}
	timeout := 3 * time.Second
	if uc.Timeout != "" {
		if d, err := time.ParseDuration(uc.Timeout); err == nil {
			timeout = d
		}
	}
	expectedStatus := uc.ExpectedStatus
	if expectedStatus == 0 {
		expectedStatus = 200
	}
	return &loadbalancer.HealthCheckConfig{
		Path:           uc.Path,
		Interval:       interval,
		Timeout:        timeout,
		ExpectedStatus: expectedStatus,
		Headers:        uc.Headers,
	}
}

// targetHostKey derives the router host key for a target.
// Uses server_name as the hostname key.
func targetHostKey(target *config.TargetConfig) string {
	return target.ServerName
}

// lazyMaterialize is the load path the lazy router's handlers use. The indirection
// exists so tests can count how often a target is compiled; production always
// goes through materializeTarget.
// applyTargetEvent applies a single target event from the config event store.
func (ps *proxyServer) buildFrontendHandler() http.Handler {
	h := http.Handler(ps.router)
	if ps.pluginMgr == nil {
		return h
	}
	for _, p := range ps.pluginMgr.GetPlugins() {
		if rw, ok := p.(base.RouterWrapper); ok {
			klog.Infof("plugin %s wraps the frontend router", p.Name())
			h = rw.WrapRouter(h)
		}
	}
	return h
}

// frontendHandler returns the wrapped frontend handler, falling back to the
// raw router when the frontend was not built yet (e.g. in tests that start
// QUIC listeners directly).
func (ps *proxyServer) frontendHandler() http.Handler {
	if ps.frontend != nil {
		return ps.frontend
	}
	return ps.buildFrontendHandler()
}

func changedTargetNames(prev, next []config.TargetConfig) []string {
	serialize := func(in []config.TargetConfig) map[string]string {
		out := make(map[string]string, len(in))
		for i := range in {
			if data, err := yaml.Marshal(&in[i]); err == nil {
				out[in[i].ServerName] = string(data)
			}
		}
		return out
	}

	prevByName := serialize(prev)
	nextByName := make(map[string]struct{}, len(next))

	var changed []string
	for i := range next {
		name := next[i].ServerName
		nextByName[name] = struct{}{}
		data, err := yaml.Marshal(&next[i])
		if err != nil {
			continue
		}
		if prevByName[name] != string(data) {
			changed = append(changed, name)
		}
	}
	for name := range prevByName {
		if _, ok := nextByName[name]; !ok {
			changed = append(changed, name+" (deleted)")
		}
	}
	return changed
}

// tcpServers returns every active TCP http.Server, falling back to the
// legacy singular srv field when no listener slice was built.
