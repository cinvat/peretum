package prometheus

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

var (
	initOnce   sync.Once
	oncePlugin *PrometheusPlugin
)

func getOncePlugin(t *testing.T) *PrometheusPlugin {
	t.Helper()
	initOnce.Do(func() {
		p := NewPrometheusPlugin()
		if err := p.Init(map[string]any{"listen": "127.0.0.1:0", "enabled": true}); err != nil {
			panic(err)
		}
		oncePlugin = p
	})
	return oncePlugin
}

func TestNewPrometheusPlugin(t *testing.T) {
	p := NewPrometheusPlugin()
	if p.Name() != "prometheus_exporter" {
		t.Errorf("name = %q", p.Name())
	}
	if p.server != nil {
		t.Error("server should be nil before Start")
	}
}

func TestInit(t *testing.T) {
	p := getOncePlugin(t)
	if p.listenAddr != "127.0.0.1:0" {
		t.Errorf("listenAddr = %q", p.listenAddr)
	}
	if p.targetRequestsTotal == nil || p.cacheExpirationsTotal == nil {
		t.Error("metrics not initialized")
	}
}

func TestStopWithoutInit(t *testing.T) {
	p := NewPrometheusPlugin()
	if err := p.Stop(context.Background()); err != nil {
		t.Errorf("Stop on nil server: %v", err)
	}
}

func TestRecordMetrics(t *testing.T) {
	p := getOncePlugin(t)
	p.RecordRequest("target-a", "/api", "prefix", false, 0.5)
	p.RecordRequest("target-a", "/api", "prefix", true, 0.25)
	p.RecordCacheHit("target-a", "/api")
	p.RecordCacheMiss("target-a", "/api")
	p.RecordCacheEviction()
	p.RecordCacheExpiration()
	p.UpdateCacheMetrics(1, 2, 3, 4, 5678, 12)
}

func TestStartStopFreePort(t *testing.T) {
	p := NewPrometheusPlugin()
	p.listenAddr = "127.0.0.1:0"
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestStartBindError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	p := NewPrometheusPlugin()
	p.listenAddr = ln.Addr().String()
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
