package loadbalancer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDefaultHealthCheckConfig(t *testing.T) {
	c := DefaultHealthCheckConfig()
	if c.Path != "/healthz" {
		t.Fatalf("Path = %q, want /healthz", c.Path)
	}
	if c.ExpectedStatus != 200 {
		t.Fatalf("ExpectedStatus = %d, want 200", c.ExpectedStatus)
	}
	if c.Interval <= 0 || c.Timeout <= 0 {
		t.Fatalf("Interval/Timeout must be positive, got %v/%v", c.Interval, c.Timeout)
	}
}

func TestNewHealthCheckerNilConfigUsesDefaults(t *testing.T) {
	lb := NewRoundRobin([]*Upstream{{URL: "http://example.com"}})
	hc := NewHealthChecker(lb, nil)
	if hc.config.Path != "/healthz" {
		t.Fatalf("config = %+v, want defaults", hc.config)
	}
}

func TestHealthCheckerProbe(t *testing.T) {
	var gotPath, gotHeader string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotHeader = r.Header.Get("X-Probe")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	t.Run("non-2xx is unhealthy", func(t *testing.T) {
		hc := NewHealthChecker(nil, &HealthCheckConfig{Path: "/healthz", ExpectedStatus: 200})
		if hc.probe(&Upstream{URL: ts.URL}) {
			t.Fatal("204 should not satisfy ExpectedStatus 200")
		}
	})

	t.Run("expected status and custom headers", func(t *testing.T) {
		hc := NewHealthChecker(nil, &HealthCheckConfig{
			Path:           "/healthz",
			ExpectedStatus: http.StatusNoContent,
			Headers:        map[string]string{"X-Probe": "yes"},
		})
		if !hc.probe(&Upstream{URL: ts.URL}) {
			t.Fatal("204 should satisfy ExpectedStatus 204")
		}
		if gotPath != "/healthz" {
			t.Fatalf("probe path = %q, want /healthz", gotPath)
		}
		if gotHeader != "yes" {
			t.Fatalf("probe header = %q, want yes", gotHeader)
		}
	})

	t.Run("zero expected status defaults to 200", func(t *testing.T) {
		hc := NewHealthChecker(nil, &HealthCheckConfig{Path: "/healthz"})
		if hc.probe(&Upstream{URL: ts.URL}) {
			t.Fatal("204 should not satisfy the implicit default of 200")
		}
	})
}

func TestHealthCheckerProbeUnparseableAndUnreachable(t *testing.T) {
	hc := NewHealthChecker(nil, &HealthCheckConfig{Path: "/healthz", Timeout: 100 * time.Millisecond})

	// No scheme separator at all.
	if hc.probe(&Upstream{URL: "not-a-url"}) {
		t.Fatal("URL without scheme must be unhealthy")
	}

	// A valid scheme pointing at a closed port.
	if hc.probe(&Upstream{URL: "http://127.0.0.1:1"}) {
		t.Fatal("unreachable upstream must be unhealthy")
	}
}

func TestHealthCheckerProbeUnhealthyAndRecovers(t *testing.T) {
	var healthy atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if healthy.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	up := &Upstream{URL: ts.URL}
	lb := NewRoundRobin([]*Upstream{up})
	up.Healthy.Store(false)

	hc := NewHealthChecker(lb, &HealthCheckConfig{Path: "/healthz", ExpectedStatus: 200})

	// Still failing: the probe runs and the upstream stays down.
	hc.probeUnhealthy()
	if up.Healthy.Load() {
		t.Fatal("upstream should remain unhealthy while the probe fails")
	}

	// Upstream comes back: the probe marks it healthy again.
	healthy.Store(true)
	hc.probeUnhealthy()
	if !up.Healthy.Load() {
		t.Fatal("upstream should be marked healthy after a successful probe")
	}
}

func TestHealthCheckerProbeUnhealthySkipsHealthy(t *testing.T) {
	// A healthy upstream must not be probed at all; an unreachable URL would
	// fail the probe, so staying healthy proves it was skipped.
	up := &Upstream{URL: "http://127.0.0.1:1"}
	up.Healthy.Store(true)

	hc := NewHealthChecker(NewRoundRobin([]*Upstream{up}), &HealthCheckConfig{Path: "/healthz"})
	hc.probeUnhealthy()

	if !up.Healthy.Load() {
		t.Fatal("healthy upstream should not have been re-probed")
	}
}

func TestHealthCheckerProbeUnhealthyNoUpstreams(t *testing.T) {
	hc := NewHealthChecker(NewRoundRobin(nil), &HealthCheckConfig{Path: "/healthz"})
	hc.probeUnhealthy() // must not panic
}

func TestHealthCheckerStartStopIsIdempotent(t *testing.T) {
	up := &Upstream{URL: "http://127.0.0.1:1"}
	up.Healthy.Store(true)
	lb := NewRoundRobin([]*Upstream{up})

	hc := NewHealthChecker(lb, &HealthCheckConfig{
		Path:     "/healthz",
		Interval: 5 * time.Millisecond,
		Timeout:  50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Two Starts must only spawn one goroutine, otherwise Stop would block on
	// a WaitGroup count that never returns.
	hc.Start(ctx)
	hc.Start(ctx)

	time.Sleep(20 * time.Millisecond)
	hc.Stop()
}

func TestHealthCheckerRunExitsOnContextCancel(t *testing.T) {
	up := &Upstream{URL: "http://127.0.0.1:1"}
	up.Healthy.Store(true)
	lb := NewRoundRobin([]*Upstream{up})

	hc := NewHealthChecker(lb, &HealthCheckConfig{
		Path:     "/healthz",
		Interval: time.Hour, // never fires during the test
		Timeout:  50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	hc.Start(ctx)
	cancel()

	// Stop waits for run() to return; if ctx cancellation were not honored this
	// would hang until the test times out.
	hc.Stop()
}
