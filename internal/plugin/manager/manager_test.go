package manager

import (
	"context"
	"net/http"
	"sync"
	"testing"
)

type stubPlugin struct {
	name           string
	initErr        error
	startErr       error
	stopErr        error
	beforeErr      error
	afterErr       error
	transformErr   error
	beforeStoreErr error
	afterHitErr    error
	shouldCache    bool
	initCfg        map[string]any

	mu      sync.Mutex
	records []string
}

func (p *stubPlugin) log(format string, args ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	line := format
	for _, a := range args {
		line += "|" + toString(a)
	}
	p.records = append(p.records, line)
}

func toString(v any) string {
	if v == nil {
		return "nil"
	}
	if s, ok := v.(string); ok {
		return s
	}
	return "?"
}

func (p *stubPlugin) Name() string { return p.name }

func (p *stubPlugin) Init(config map[string]any) error {
	p.initCfg = config
	p.log("init")
	return p.initErr
}

func (p *stubPlugin) Start(ctx context.Context) error {
	p.log("start")
	return p.startErr
}

func (p *stubPlugin) Stop(ctx context.Context) error {
	p.log("stop")
	return p.stopErr
}

func (p *stubPlugin) BeforeProxy(w http.ResponseWriter, r *http.Request, target, location string) error {
	p.log("before:" + target + ":" + location)
	return p.beforeErr
}

func (p *stubPlugin) AfterProxy(w http.ResponseWriter, r *http.Request, target, location string, resp *http.Response) error {
	p.log("after:" + target + ":" + location)
	return p.afterErr
}

func (p *stubPlugin) TransformResponseBody(target, location, contentType string, body []byte) ([]byte, error) {
	p.log("transform:" + target)
	if p.transformErr != nil {
		return nil, p.transformErr
	}
	return append(append([]byte{}, body...), []byte(p.name)...), nil
}

func (p *stubPlugin) BeforeCacheStore(key, target, location string, statusCode int, headers http.Header, body []byte) ([]byte, error) {
	p.log("beforeStore:" + target)
	if p.beforeStoreErr != nil {
		return nil, p.beforeStoreErr
	}
	return append(append([]byte{}, body...), []byte(p.name)...), nil
}

func (p *stubPlugin) AfterCacheHit(w http.ResponseWriter, r *http.Request, key, target, location string) error {
	p.log("afterHit:" + key)
	return p.afterHitErr
}

func (p *stubPlugin) ShouldCache(target, location string, statusCode int, headers http.Header, body []byte) bool {
	p.log("shouldCache")
	return p.shouldCache
}

func (p *stubPlugin) RecordRequest(target, location, matchType string, cached bool, duration float64) {
	p.log("recordRequest")
}

func (p *stubPlugin) RecordCacheHit(target, location string) { p.log("recordCacheHit") }
func (p *stubPlugin) RecordCacheMiss(target, location string) {
	p.log("recordCacheMiss")
}
func (p *stubPlugin) RecordCacheEviction() { p.log("recordCacheEviction") }
func (p *stubPlugin) RecordCacheExpiration() {
	p.log("recordCacheExpiration")
}

func (p *stubPlugin) LogError(level, target, location, requestID, msg string, fields map[string]any) {
	p.log("logError:" + level)
}

// plainPlugin implements only base.Plugin.
type plainPlugin struct{ name string }

func (p *plainPlugin) Name() string { return p.name }
func (p *plainPlugin) Init(config map[string]any) error {
	return nil
}
func (p *plainPlugin) Start(ctx context.Context) error { return nil }
func (p *plainPlugin) Stop(ctx context.Context) error  { return nil }

func (p *stubPlugin) has(prefix string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.records {
		if len(r) >= len(prefix) && r[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

func TestRegisterPlugin(t *testing.T) {
	pm := NewPluginManager()
	sp := &stubPlugin{name: "stub"}
	pp := &plainPlugin{name: "plain"}

	pm.RegisterPlugin(pp)
	pm.RegisterPlugin(sp)

	if len(pm.plugins) != 2 {
		t.Fatalf("plugins len = %d", len(pm.plugins))
	}
	if len(pm.requestHooks) != 1 {
		t.Fatalf("requestHooks len = %d (plain plugin must not register)", len(pm.requestHooks))
	}
	if len(pm.bodyHooks) != 1 {
		t.Fatalf("bodyHooks len = %d", len(pm.bodyHooks))
	}
	if len(pm.cacheHooks) != 1 {
		t.Fatalf("cacheHooks len = %d", len(pm.cacheHooks))
	}
	if len(pm.metricsHooks) != 1 {
		t.Fatalf("metricsHooks len = %d", len(pm.metricsHooks))
	}
	if len(pm.errorHooks) != 1 {
		t.Fatalf("errorHooks len = %d", len(pm.errorHooks))
	}
}

func TestGetPlugins(t *testing.T) {
	pm := NewPluginManager()
	pm.RegisterPlugin(&stubPlugin{name: "a"})
	pm.RegisterPlugin(&stubPlugin{name: "b"})

	got := pm.GetPlugins()
	if len(got) != 2 || got[0].Name() != "a" || got[1].Name() != "b" {
		t.Fatalf("GetPlugins = %v", got)
	}
	// Returned slice is a copy.
	got[0] = &plainPlugin{name: "mutated"}
	if pm.plugins[0].Name() != "a" {
		t.Fatal("GetPlugins returned an alias of internal slice")
	}
}

func TestInitializePlugins(t *testing.T) {
	pm := NewPluginManager()
	sp := &stubPlugin{name: "a"}
	pm.RegisterPlugin(sp)

	cfg := map[string]map[string]any{"a": {"k": "v"}}
	if err := pm.InitializePlugins(context.Background(), cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sp.initCfg == nil || sp.initCfg["k"] != "v" {
		t.Fatalf("Init called with %v", sp.initCfg)
	}

	// Init error short-circuits.
	fail := &stubPlugin{name: "a", initErr: context.DeadlineExceeded}
	pm2 := NewPluginManager()
	pm2.RegisterPlugin(fail)
	if err := pm2.InitializePlugins(context.Background(), nil); err == nil {
		t.Fatal("expected init error")
	}
}

func TestStartPlugins(t *testing.T) {
	pm := NewPluginManager()
	sp := &stubPlugin{name: "a"}
	pm.RegisterPlugin(sp)
	if err := pm.StartPlugins(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !sp.has("start") {
		t.Fatal("Start not called")
	}

	pm2 := NewPluginManager()
	pm2.RegisterPlugin(&stubPlugin{name: "a", startErr: context.Canceled})
	if err := pm2.StartPlugins(context.Background()); err == nil {
		t.Fatal("expected start error")
	}
}

func TestStopPlugins(t *testing.T) {
	pm := NewPluginManager()
	a := &stubPlugin{name: "a"}
	b := &stubPlugin{name: "b"}
	pm.RegisterPlugin(a)
	pm.RegisterPlugin(b)
	if err := pm.StopPlugins(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Stops run in reverse registration order.
	order := []string{}
	a.mu.Lock()
	b.mu.Lock()
	for _, r := range b.records {
		order = append(order, "b:"+r)
	}
	for _, r := range a.records {
		order = append(order, "a:"+r)
	}
	a.mu.Unlock()
	b.mu.Unlock()
	if order[0] != "b:stop" || order[1] != "a:stop" {
		t.Fatalf("stop order = %v", order)
	}

	pm2 := NewPluginManager()
	pm2.RegisterPlugin(&stubPlugin{name: "a", stopErr: context.DeadlineExceeded})
	if err := pm2.StopPlugins(context.Background()); err == nil {
		t.Fatal("expected stop error")
	}
}

func TestRunBeforeProxy(t *testing.T) {
	pm := NewPluginManager()
	// No request hooks -> nil.
	if err := pm.RunBeforeProxy(nil, nil, "t", "l"); err != nil {
		t.Fatalf("unexpected error with no hooks: %v", err)
	}

	sp := &stubPlugin{name: "a"}
	pm.RegisterPlugin(sp)
	if err := pm.RunBeforeProxy(nil, nil, "t", "l"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !sp.has("before") {
		t.Fatal("BeforeProxy not called")
	}

	pm2 := NewPluginManager()
	pm2.RegisterPlugin(&stubPlugin{name: "a", beforeErr: context.Canceled})
	if err := pm2.RunBeforeProxy(nil, nil, "t", "l"); err == nil {
		t.Fatal("expected before error")
	}
}

func TestRunAfterProxy(t *testing.T) {
	pm := NewPluginManager()
	sp := &stubPlugin{name: "a"}
	pm.RegisterPlugin(sp)
	if err := pm.RunAfterProxy(nil, nil, "t", "l", nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !sp.has("after") {
		t.Fatal("AfterProxy not called")
	}

	pm2 := NewPluginManager()
	pm2.RegisterPlugin(&stubPlugin{name: "a", afterErr: context.Canceled})
	if err := pm2.RunAfterProxy(nil, nil, "t", "l", nil); err == nil {
		t.Fatal("expected after error")
	}
}

func TestRunTransformResponseBody(t *testing.T) {
	pm := NewPluginManager()
	if got, err := pm.RunTransformResponseBody("t", "l", "text/plain", []byte("base")); err != nil {
		t.Fatalf("no hooks: err %v", err)
	} else if string(got) != "base" {
		t.Fatalf("no hooks: got %q", got)
	}

	a := &stubPlugin{name: "a"}
	b := &stubPlugin{name: "b"}
	pm.RegisterPlugin(a)
	pm.RegisterPlugin(b)
	got, err := pm.RunTransformResponseBody("t", "l", "text/plain", []byte("x"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != "xab" {
		t.Fatalf("transform chain = %q", got)
	}

	pm2 := NewPluginManager()
	pm2.RegisterPlugin(&stubPlugin{name: "a", transformErr: context.Canceled})
	if _, err := pm2.RunTransformResponseBody("t", "l", "text/plain", []byte("x")); err == nil {
		t.Fatal("expected transform error")
	}
}

func TestRunBeforeCacheStore(t *testing.T) {
	pm := NewPluginManager()
	if got, err := pm.RunBeforeCacheStore("k", "t", "l", 200, nil, []byte("x")); err != nil {
		t.Fatalf("no hooks: err %v", err)
	} else if string(got) != "x" {
		t.Fatalf("no hooks: got %q", got)
	}

	a := &stubPlugin{name: "a"}
	b := &stubPlugin{name: "b"}
	pm.RegisterPlugin(a)
	pm.RegisterPlugin(b)
	got, err := pm.RunBeforeCacheStore("k", "t", "l", 200, nil, []byte("y"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != "yab" {
		t.Fatalf("beforeStore chain = %q", got)
	}

	pm2 := NewPluginManager()
	pm2.RegisterPlugin(&stubPlugin{name: "a", beforeStoreErr: context.Canceled})
	if _, err := pm2.RunBeforeCacheStore("k", "t", "l", 200, nil, []byte("y")); err == nil {
		t.Fatal("expected beforeStore error")
	}
}

func TestRunAfterCacheHit(t *testing.T) {
	pm := NewPluginManager()
	if err := pm.RunAfterCacheHit(nil, nil, "k", "t", "l"); err != nil {
		t.Fatalf("no hooks err: %v", err)
	}

	sp := &stubPlugin{name: "a"}
	pm.RegisterPlugin(sp)
	if err := pm.RunAfterCacheHit(nil, nil, "k", "t", "l"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !sp.has("afterHit") {
		t.Fatal("AfterCacheHit not called")
	}

	pm2 := NewPluginManager()
	pm2.RegisterPlugin(&stubPlugin{name: "a", afterHitErr: context.Canceled})
	if err := pm2.RunAfterCacheHit(nil, nil, "k", "t", "l"); err == nil {
		t.Fatal("expected afterHit error")
	}
}

func TestRunShouldCache(t *testing.T) {
	pm := NewPluginManager()
	if !pm.RunShouldCache("t", "l", 200, nil, nil) {
		t.Fatal("no hooks should return true")
	}

	yes := &stubPlugin{name: "yes", shouldCache: true}
	pm.RegisterPlugin(yes)
	if !pm.RunShouldCache("t", "l", 200, nil, nil) {
		t.Fatal("all-true should return true")
	}

	pm2 := NewPluginManager()
	pm2.RegisterPlugin(&stubPlugin{name: "no", shouldCache: false})
	if pm2.RunShouldCache("t", "l", 200, nil, nil) {
		t.Fatal("false hook should return false")
	}
	pm2.RegisterPlugin(&stubPlugin{name: "also", shouldCache: true})
	if pm2.RunShouldCache("t", "l", 200, nil, nil) {
		t.Fatal("any false hook should veto")
	}
}

func TestRunMetricsHooks(t *testing.T) {
	pm := NewPluginManager()
	pm.RunRecordRequest("t", "l", "exact", true, 1.5)
	pm.RunRecordCacheHit("t", "l")
	pm.RunRecordCacheMiss("t", "l")
	pm.RunRecordCacheEviction()
	pm.RunRecordCacheExpiration()

	sp := &stubPlugin{name: "m"}
	pm.RegisterPlugin(sp)
	pm.RunRecordRequest("t", "l", "exact", true, 1.5)
	pm.RunRecordCacheHit("t", "l")
	pm.RunRecordCacheMiss("t", "l")
	pm.RunRecordCacheEviction()
	pm.RunRecordCacheExpiration()

	for _, want := range []string{"recordRequest", "recordCacheHit", "recordCacheMiss", "recordCacheEviction", "recordCacheExpiration"} {
		if !sp.has(want) {
			t.Fatalf("metrics hook %q not called", want)
		}
	}
}

func TestRunLogError(t *testing.T) {
	pm := NewPluginManager()
	pm.RunLogError("ERROR", "t", "l", "reqid", "boom", map[string]any{"k": 1})

	sp := &stubPlugin{name: "e"}
	pm.RegisterPlugin(sp)
	pm.RunLogError("ERROR", "t", "l", "reqid", "boom", map[string]any{"k": 1})
	if !sp.has("logError") {
		t.Fatal("LogError not called")
	}
}
