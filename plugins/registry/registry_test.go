package registry

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/cinvat/peretum/plugins/base"
)

type regPlugin struct {
	name          string
	initErr       error
	startErr      error
	stopErr       error
	initCfg       map[string]any
	startCalled   bool
	stopCalled    bool
	stopOrderSlot *[]string
}

func (p *regPlugin) Name() string { return p.name }

func (p *regPlugin) Init(config map[string]any) error {
	p.initCfg = config
	return p.initErr
}

func (p *regPlugin) Start(ctx context.Context) error {
	p.startCalled = true
	return p.startErr
}

func (p *regPlugin) Stop(ctx context.Context) error {
	p.stopCalled = true
	if p.stopOrderSlot != nil {
		*p.stopOrderSlot = append(*p.stopOrderSlot, p.name)
	}
	return p.stopErr
}

func resetFactories() {
	defaultRegistry.mu.Lock()
	defaultRegistry.factories = make(map[string]func() base.Plugin)
	defaultRegistry.mu.Unlock()
}

func registerStub(name string, p *regPlugin) {
	Register(name, func() base.Plugin { return p })
}

// Must run first (source order): the default registry is populated by init()
// with factories whose function bodies are otherwise never executed. Calling
// Get on every registered name invokes those closures.
func TestInitFactories(t *testing.T) {
	resetFactories()
	registerDefaultFactories()
	for _, name := range []string{
		"cors",
		"headers",
		"rewrite",
		"compression",
		"optimizer",
		"jsonlog",
		"prometheus_exporter",
		"error_page",
		"waf",
	} {
		p, err := Get(name)
		if err != nil {
			t.Fatalf("Get(%q) error: %v", name, err)
		}
		if p.Name() != name {
			t.Fatalf("Get(%q) name = %q", name, p.Name())
		}
	}
	if n := len(List()); n != 9 {
		t.Fatalf("expected 9 init-registered plugins, got %d", n)
	}
}

func TestRegisterAndGet(t *testing.T) {
	resetFactories()
	Register("alpha", func() base.Plugin { return &regPlugin{name: "alpha"} })

	p, err := Get("alpha")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if p.Name() != "alpha" {
		t.Fatalf("Get name = %s", p.Name())
	}

	// Get invokes the factory every time (fresh instances).
	p2, err := Get("alpha")
	if err != nil {
		t.Fatalf("Get second call error: %v", err)
	}
	if p == p2 {
		t.Fatal("expected distinct instances from factory")
	}

	if _, err := Get("missing"); err == nil {
		t.Fatal("expected not-found error")
	} else if !bytes.Contains([]byte(err.Error()), []byte("plugin not found: missing")) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestList(t *testing.T) {
	resetFactories()
	Register("one", func() base.Plugin { return &regPlugin{name: "one"} })
	Register("two", func() base.Plugin { return &regPlugin{name: "two"} })

	names := List()
	sort.Strings(names)
	if len(names) != 2 || names[0] != "one" || names[1] != "two" {
		t.Fatalf("List = %v", names)
	}
}

func TestLoadPlugins(t *testing.T) {
	resetFactories()
	Register("on", func() base.Plugin { return &regPlugin{name: "on"} })
	Register("off", func() base.Plugin { return &regPlugin{name: "off"} })

	configs := map[string]map[string]any{
		"on":  {"enabled": true},
		"off": {"enabled": false},
	}
	plugins, err := LoadPlugins(context.Background(), configs)
	if err != nil {
		t.Fatalf("LoadPlugins error: %v", err)
	}
	if len(plugins) != 1 || plugins[0].Name() != "on" {
		t.Fatalf("LoadPlugins = %v", plugins)
	}

	// Unknown enabled plugin -> wrapped error.
	configs2 := map[string]map[string]any{"ghost": {"enabled": true}}
	if _, err := LoadPlugins(context.Background(), configs2); err == nil {
		t.Fatal("expected error for unknown plugin")
	} else if !bytes.Contains([]byte(err.Error()), []byte("failed to create plugin ghost")) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitializeAndStart(t *testing.T) {
	resetFactories()
	p := &regPlugin{name: "ok"}
	registerStub("ok", p)

	configs := map[string]map[string]any{"ok": {"x": 1}}
	if err := InitializeAndStart(context.Background(), []base.Plugin{p}, configs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.initCfg == nil || p.initCfg["x"] != 1 {
		t.Fatalf("Init config = %v", p.initCfg)
	}
	if !p.startCalled {
		t.Fatal("Start not called")
	}

	// Init error path.
	p2 := &regPlugin{name: "bad", initErr: errors.New("init fail")}
	if err := InitializeAndStart(context.Background(), []base.Plugin{p2}, map[string]map[string]any{"bad": {}}); err == nil {
		t.Fatal("expected init error")
	} else if !bytes.Contains([]byte(err.Error()), []byte("failed to init plugin bad")) {
		t.Fatalf("unexpected error: %v", err)
	}

	// Start error path (Init succeeds).
	p3 := &regPlugin{name: "badstart", startErr: errors.New("start fail")}
	if err := InitializeAndStart(context.Background(), []base.Plugin{p3}, map[string]map[string]any{"badstart": {}}); err == nil {
		t.Fatal("expected start error")
	} else if !bytes.Contains([]byte(err.Error()), []byte("failed to start plugin badstart")) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadAndInitializePlugins(t *testing.T) {
	resetFactories()
	p := &regPlugin{name: "ok"}
	registerStub("ok", p)

	configs := map[string]map[string]any{"ok": {"enabled": true}}
	plugins, err := LoadAndInitializePlugins(context.Background(), configs)
	if err != nil {
		t.Fatalf("LoadAndInitializePlugins error: %v", err)
	}
	if len(plugins) != 1 || !p.startCalled || p.initCfg == nil {
		t.Fatalf("plugIns = %v, startCalled=%v", plugins, p.startCalled)
	}

	// LoadPlugins branch error.
	configs2 := map[string]map[string]any{"ghost": {"enabled": true}}
	if _, err := LoadAndInitializePlugins(context.Background(), configs2); err == nil {
		t.Fatal("expected error from LoadPlugins")
	}

	// InitializeAndStart branch error.
	resetFactories()
	pbad := &regPlugin{name: "bad", initErr: errors.New("boom")}
	registerStub("bad", pbad)
	configs3 := map[string]map[string]any{"bad": {"enabled": true}}
	if _, err := LoadAndInitializePlugins(context.Background(), configs3); err == nil {
		t.Fatal("expected error from InitializeAndStart")
	}
}

func TestStopAll(t *testing.T) {
	resetFactories()
	var order []string
	a := &regPlugin{name: "a", stopOrderSlot: &order}
	b := &regPlugin{name: "b", stopOrderSlot: &order}
	c := &regPlugin{name: "c", stopOrderSlot: &order}
	plugins := []base.Plugin{a, b, c}

	if err := StopAll(context.Background(), plugins); err != nil {
		t.Fatalf("StopAll error: %v", err)
	}
	if len(order) != 3 || order[0] != "c" || order[1] != "b" || order[2] != "a" {
		t.Fatalf("stop order = %v", order)
	}

	// Error path wraps the plugin name.
	fail := &regPlugin{name: "boom", stopErr: errors.New("stop fail")}
	if err := StopAll(context.Background(), []base.Plugin{a, fail}); err == nil {
		t.Fatal("expected stop error")
	} else if !bytes.Contains([]byte(err.Error()), []byte("failed to stop plugin boom")) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStopAllPlugins(t *testing.T) {
	resetFactories()
	var order []string
	a := &regPlugin{name: "a", stopOrderSlot: &order}
	b := &regPlugin{name: "b", stopOrderSlot: &order}
	plugins := []base.Plugin{a, b}

	if err := StopAllPlugins(context.Background(), plugins); err != nil {
		t.Fatalf("StopAllPlugins error: %v", err)
	}
	if !a.stopCalled || !b.stopCalled {
		t.Fatal("Stop not called on both plugins")
	}

	fail := &regPlugin{name: "x", stopErr: errors.New("boom")}
	if err := StopAllPlugins(context.Background(), []base.Plugin{fail}); err == nil {
		t.Fatal("expected stop error")
	}
}
