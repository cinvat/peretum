package loadbalancer

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// HealthCheckConfig defines how to probe an upstream.
type HealthCheckConfig struct {
	// Path is the HTTP path to probe (e.g., "/healthz"). Required.
	Path string
	// Interval is how often to probe unhealthy upstreams.
	Interval time.Duration
	// Timeout is the maximum time to wait for a probe response.
	Timeout time.Duration
	// ExpectedStatus is the expected HTTP status code (default 200).
	ExpectedStatus int
	// Headers are additional headers to send with the probe.
	Headers map[string]string
}

// DefaultHealthCheckConfig returns a sensible default.
func DefaultHealthCheckConfig() *HealthCheckConfig {
	return &HealthCheckConfig{
		Path:           "/healthz",
		Interval:       10 * time.Second,
		Timeout:        3 * time.Second,
		ExpectedStatus: 200,
	}
}

// HealthChecker runs active health checks against upstreams.
type HealthChecker struct {
	mu        sync.RWMutex
	lb        LoadBalancer
	config    *HealthCheckConfig
	client    *http.Client
	stopCh    chan struct{}
	wg        sync.WaitGroup
	startOnce sync.Once
}

// NewHealthChecker creates a health checker for the given load balancer.
func NewHealthChecker(lb LoadBalancer, config *HealthCheckConfig) *HealthChecker {
	if config == nil {
		config = DefaultHealthCheckConfig()
	}
	return &HealthChecker{
		lb:     lb,
		config: config,
		client: &http.Client{
			Timeout: config.Timeout,
			Transport: &http.Transport{
				DisableKeepAlives: true, // Don't hold connections between probes
			},
		},
		stopCh: make(chan struct{}),
	}
}

// Start begins periodic health checking in the background.
func (hc *HealthChecker) Start(ctx context.Context) {
	hc.startOnce.Do(func() {
		hc.wg.Add(1)
		go hc.run(ctx)
	})
}

// Stop stops the health checker.
func (hc *HealthChecker) Stop() {
	close(hc.stopCh)
	hc.wg.Wait()
}

func (hc *HealthChecker) run(ctx context.Context) {
	defer hc.wg.Done()

	ticker := time.NewTicker(hc.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-hc.stopCh:
			return
		case <-ticker.C:
			hc.probeUnhealthy()
		}
	}
}

func (hc *HealthChecker) probeUnhealthy() {
	upstreams := hc.lb.GetUpstreams()
	if len(upstreams) == 0 {
		return
	}

	for _, u := range upstreams {
		if u.Healthy.Load() {
			continue
		}
		healthy := hc.probe(u)
		hc.lb.MarkHealthy(u.URL, healthy)
		if healthy {
			// Log recovery
			fmt.Printf("upstream %s recovered\n", u.URL)
		}
	}
}

func (hc *HealthChecker) probe(u *Upstream) bool {
	// Parse the upstream URL
	scheme, host, ok := strings.Cut(u.URL, "://")
	if !ok {
		return false
	}

	// Build probe URL
	probeURL := fmt.Sprintf("%s://%s%s", scheme, host, hc.config.Path)

	req, err := http.NewRequest("GET", probeURL, nil)
	if err != nil {
		return false
	}
	// Add custom headers
	for k, v := range hc.config.Headers {
		req.Header.Set(k, v)
	}

	resp, err := hc.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	expected := hc.config.ExpectedStatus
	if expected == 0 {
		expected = 200
	}
	return resp.StatusCode == expected
}
