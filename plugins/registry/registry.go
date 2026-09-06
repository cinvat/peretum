package registry

import (
	"context"
	"fmt"
	"sync"

	"github.com/cinvat/peretum/plugins/base"
)

type Registry struct {
	factories map[string]func() base.Plugin
	mu        sync.RWMutex
}

var defaultRegistry = &Registry{
	factories: make(map[string]func() base.Plugin),
}

func Register(name string, factory func() base.Plugin) {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	defaultRegistry.factories[name] = factory
}

func Get(name string) (base.Plugin, error) {
	defaultRegistry.mu.RLock()
	factory, ok := defaultRegistry.factories[name]
	defaultRegistry.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("plugin not found: %s", name)
	}
	return factory(), nil
}

func List() []string {
	defaultRegistry.mu.RLock()
	defer defaultRegistry.mu.RUnlock()
	names := make([]string, 0, len(defaultRegistry.factories))
	for name := range defaultRegistry.factories {
		names = append(names, name)
	}
	return names
}

func LoadPlugins(ctx context.Context, configs map[string]map[string]any) ([]base.Plugin, error) {
	var plugins []base.Plugin
	for name, config := range configs {
		if !base.GetBool(config, "enabled") {
			continue
		}
		p, err := Get(name)
		if err != nil {
			return nil, fmt.Errorf("failed to create plugin %s: %w", name, err)
		}
		plugins = append(plugins, p)
	}
	return plugins, nil
}

func InitializeAndStart(ctx context.Context, plugins []base.Plugin, configs map[string]map[string]any) error {
	for _, p := range plugins {
		config := configs[p.Name()]
		if err := p.Init(config); err != nil {
			return fmt.Errorf("failed to init plugin %s: %w", p.Name(), err)
		}
	}

	for _, p := range plugins {
		if err := p.Start(ctx); err != nil {
			return fmt.Errorf("failed to start plugin %s: %w", p.Name(), err)
		}
	}
	return nil
}

func StopAll(ctx context.Context, plugins []base.Plugin) error {
	for i := len(plugins) - 1; i >= 0; i-- {
		if err := plugins[i].Stop(ctx); err != nil {
			return fmt.Errorf("failed to stop plugin %s: %w", plugins[i].Name(), err)
		}
	}
	return nil
}
