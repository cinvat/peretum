package manager

import (
	"context"
	"net/http"
	"sync"

	"github.com/cinvat/peretum/plugins/base"
)

type PluginManager struct {
	plugins      []base.Plugin
	requestHooks []base.RequestHook
	bodyHooks    []base.ResponseBodyHook
	cacheHooks   []base.CacheHook
	metricsHooks []base.MetricsHook
	errorHooks   []base.ErrorHook
	mu           sync.RWMutex
}

func NewPluginManager() *PluginManager {
	return &PluginManager{
		plugins:      make([]base.Plugin, 0),
		requestHooks: make([]base.RequestHook, 0),
		bodyHooks:    make([]base.ResponseBodyHook, 0),
		cacheHooks:   make([]base.CacheHook, 0),
		metricsHooks: make([]base.MetricsHook, 0),
		errorHooks:   make([]base.ErrorHook, 0),
	}
}

func (pm *PluginManager) RegisterPlugin(p base.Plugin) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.plugins = append(pm.plugins, p)

	if h, ok := p.(base.RequestHook); ok {
		pm.requestHooks = append(pm.requestHooks, h)
	}
	if h, ok := p.(base.ResponseBodyHook); ok {
		pm.bodyHooks = append(pm.bodyHooks, h)
	}
	if h, ok := p.(base.CacheHook); ok {
		pm.cacheHooks = append(pm.cacheHooks, h)
	}
	if h, ok := p.(base.MetricsHook); ok {
		pm.metricsHooks = append(pm.metricsHooks, h)
	}
	if h, ok := p.(base.ErrorHook); ok {
		pm.errorHooks = append(pm.errorHooks, h)
	}
}

func (pm *PluginManager) GetPlugins() []base.Plugin {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	result := make([]base.Plugin, len(pm.plugins))
	copy(result, pm.plugins)
	return result
}

func (pm *PluginManager) InitializePlugins(ctx context.Context, configs map[string]map[string]any) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, p := range pm.plugins {
		config := configs[p.Name()]
		if err := p.Init(config); err != nil {
			return err
		}
	}
	return nil
}

func (pm *PluginManager) StartPlugins(ctx context.Context) error {
	pm.mu.RLock()
	plugins := make([]base.Plugin, len(pm.plugins))
	copy(plugins, pm.plugins)
	pm.mu.RUnlock()

	for _, p := range plugins {
		if err := p.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (pm *PluginManager) StopPlugins(ctx context.Context) error {
	pm.mu.RLock()
	plugins := make([]base.Plugin, len(pm.plugins))
	copy(plugins, pm.plugins)
	pm.mu.RUnlock()

	for i := len(plugins) - 1; i >= 0; i-- {
		if err := plugins[i].Stop(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (pm *PluginManager) RunBeforeProxy(w http.ResponseWriter, r *http.Request, target, location string) error {
	pm.mu.RLock()
	hooks := make([]base.RequestHook, len(pm.requestHooks))
	copy(hooks, pm.requestHooks)
	pm.mu.RUnlock()

	for _, h := range hooks {
		if err := h.BeforeProxy(w, r, target, location); err != nil {
			return err
		}
	}
	return nil
}

func (pm *PluginManager) RunAfterProxy(w http.ResponseWriter, r *http.Request, target, location string, resp *http.Response) error {
	pm.mu.RLock()
	hooks := make([]base.RequestHook, len(pm.requestHooks))
	copy(hooks, pm.requestHooks)
	pm.mu.RUnlock()

	for _, h := range hooks {
		if err := h.AfterProxy(w, r, target, location, resp); err != nil {
			return err
		}
	}
	return nil
}

func (pm *PluginManager) RunTransformResponseBody(target, location, contentType string, body []byte) ([]byte, error) {
	pm.mu.RLock()
	hooks := make([]base.ResponseBodyHook, len(pm.bodyHooks))
	copy(hooks, pm.bodyHooks)
	pm.mu.RUnlock()

	result := body
	for _, h := range hooks {
		transformed, err := h.TransformResponseBody(target, location, contentType, result)
		if err != nil {
			return nil, err
		}
		result = transformed
	}
	return result, nil
}

func (pm *PluginManager) RunBeforeCacheStore(key, target, location string, statusCode int, headers http.Header, body []byte) ([]byte, error) {
	pm.mu.RLock()
	hooks := make([]base.CacheHook, len(pm.cacheHooks))
	copy(hooks, pm.cacheHooks)
	pm.mu.RUnlock()

	result := body
	for _, h := range hooks {
		transformed, err := h.BeforeCacheStore(key, target, location, statusCode, headers, result)
		if err != nil {
			return nil, err
		}
		result = transformed
	}
	return result, nil
}

func (pm *PluginManager) RunAfterCacheHit(w http.ResponseWriter, r *http.Request, key, target, location string) error {
	pm.mu.RLock()
	hooks := make([]base.CacheHook, len(pm.cacheHooks))
	copy(hooks, pm.cacheHooks)
	pm.mu.RUnlock()

	for _, h := range hooks {
		if err := h.AfterCacheHit(w, r, key, target, location); err != nil {
			return err
		}
	}
	return nil
}

func (pm *PluginManager) RunShouldCache(target, location string, statusCode int, headers http.Header, body []byte) bool {
	pm.mu.RLock()
	hooks := make([]base.CacheHook, len(pm.cacheHooks))
	copy(hooks, pm.cacheHooks)
	pm.mu.RUnlock()

	for _, h := range hooks {
		if !h.ShouldCache(target, location, statusCode, headers, body) {
			return false
		}
	}
	return true
}

func (pm *PluginManager) RunRecordRequest(target, location, matchType string, cached bool, duration float64) {
	pm.mu.RLock()
	hooks := make([]base.MetricsHook, len(pm.metricsHooks))
	copy(hooks, pm.metricsHooks)
	pm.mu.RUnlock()

	for _, h := range hooks {
		h.RecordRequest(target, location, matchType, cached, duration)
	}
}

func (pm *PluginManager) RunRecordCacheHit(target, location string) {
	pm.mu.RLock()
	hooks := make([]base.MetricsHook, len(pm.metricsHooks))
	copy(hooks, pm.metricsHooks)
	pm.mu.RUnlock()

	for _, h := range hooks {
		h.RecordCacheHit(target, location)
	}
}

func (pm *PluginManager) RunRecordCacheMiss(target, location string) {
	pm.mu.RLock()
	hooks := make([]base.MetricsHook, len(pm.metricsHooks))
	copy(hooks, pm.metricsHooks)
	pm.mu.RUnlock()

	for _, h := range hooks {
		h.RecordCacheMiss(target, location)
	}
}

func (pm *PluginManager) RunRecordCacheEviction() {
	pm.mu.RLock()
	hooks := make([]base.MetricsHook, len(pm.metricsHooks))
	copy(hooks, pm.metricsHooks)
	pm.mu.RUnlock()

	for _, h := range hooks {
		h.RecordCacheEviction()
	}
}

func (pm *PluginManager) RunRecordCacheExpiration() {
	pm.mu.RLock()
	hooks := make([]base.MetricsHook, len(pm.metricsHooks))
	copy(hooks, pm.metricsHooks)
	pm.mu.RUnlock()

	for _, h := range hooks {
		h.RecordCacheExpiration()
	}
}

func (pm *PluginManager) RunLogError(level, target, location, requestID, msg string, fields map[string]any) {
	pm.mu.RLock()
	hooks := make([]base.ErrorHook, len(pm.errorHooks))
	copy(hooks, pm.errorHooks)
	pm.mu.RUnlock()

	for _, h := range hooks {
		h.LogError(level, target, location, requestID, msg, fields)
	}
}
