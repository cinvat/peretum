package prometheus

import (
	"context"
	"net/http"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"

	"github.com/cinvat/peretum/plugins/base"
)

type PrometheusPlugin struct {
	*base.BasePlugin

	targetRequestsTotal         *prometheus.CounterVec
	targetCachedRequestsTotal   *prometheus.CounterVec
	locationRequestsTotal       *prometheus.CounterVec
	locationCachedRequestsTotal *prometheus.CounterVec
	proxyRequestsTotal          prometheus.Counter
	proxyCachedRequestsTotal    prometheus.Counter
	proxyActiveRequests         prometheus.Gauge
	proxyRequestDuration        *prometheus.HistogramVec
	cacheSizeBytes              prometheus.Gauge
	cacheEntries                prometheus.Gauge
	cacheHitsTotal              prometheus.Counter
	cacheMissesTotal            prometheus.Counter
	cacheEvictionsTotal         prometheus.Counter
	cacheExpirationsTotal       prometheus.Counter

	server     *http.Server
	listenAddr string
	mu         sync.RWMutex
}

func NewPrometheusPlugin() *PrometheusPlugin {
	return &PrometheusPlugin{
		BasePlugin: base.NewBasePlugin("prometheus_exporter"),
	}
}

func (p *PrometheusPlugin) Init(config map[string]any) error {
	p.BasePlugin.Init(config)

	p.listenAddr = ":9090"
	if addr, ok := config["listen"].(string); ok && addr != "" {
		p.listenAddr = config["listen"].(string)
	}

	p.targetRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "peretum_target_requests_total",
			Help: "Total number of requests processed by target",
		},
		[]string{"target"},
	)

	p.targetCachedRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "peretum_target_cached_requests_total",
			Help: "Total number of cached requests served by target",
		},
		[]string{"target"},
	)

	p.locationRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "peretum_location_requests_total",
			Help: "Total number of requests processed by location",
		},
		[]string{"target", "location", "match_type"},
	)

	p.locationCachedRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "peretum_location_cached_requests_total",
			Help: "Total number of cached requests served by location",
		},
		[]string{"target", "location", "match_type"},
	)

	p.proxyRequestsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peretum_proxy_requests_total",
			Help: "Total number of requests processed by proxy",
		},
	)

	p.proxyCachedRequestsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peretum_proxy_cached_requests_total",
			Help: "Total number of cached requests served by proxy",
		},
	)

	p.proxyActiveRequests = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "peretum_proxy_active_requests",
			Help: "Number of currently active requests",
		},
	)

	p.proxyRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "peretum_proxy_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"target", "location", "cached"},
	)

	p.cacheSizeBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "peretum_cache_size_bytes",
			Help: "Current cache size in bytes",
		},
	)

	p.cacheEntries = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "peretum_cache_entries",
			Help: "Current number of cache entries",
		},
	)

	p.cacheHitsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peretum_cache_hits_total",
			Help: "Total number of cache hits",
		},
	)

	p.cacheMissesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peretum_cache_misses_total",
			Help: "Total number of cache misses",
		},
	)

	p.cacheEvictionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peretum_cache_evictions_total",
			Help: "Total number of cache evictions",
		},
	)

	p.cacheExpirationsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peretum_cache_expirations_total",
			Help: "Total number of cache expirations",
		},
	)

	return nil
}

func (p *PrometheusPlugin) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	p.server = &http.Server{
		Addr:    p.listenAddr,
		Handler: mux,
	}

	go func() {
		klog.Infof("Prometheus exporter listening on %s", p.listenAddr)
		if err := p.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Errorf("Prometheus server error: %v", err)
		}
	}()

	return nil
}

func (p *PrometheusPlugin) Stop(ctx context.Context) error {
	if p.server != nil {
		return p.server.Shutdown(ctx)
	}
	return nil
}

func (p *PrometheusPlugin) RecordRequest(target, location, matchType string, cached bool, duration float64) {
	p.targetRequestsTotal.WithLabelValues(target).Inc()
	p.locationRequestsTotal.WithLabelValues(target, location, "").Inc()
	p.proxyRequestsTotal.Inc()

	if cached {
		p.targetCachedRequestsTotal.WithLabelValues(target).Inc()
		p.locationCachedRequestsTotal.WithLabelValues(target, location, "").Inc()
		p.proxyCachedRequestsTotal.Inc()
	}

	p.proxyRequestDuration.WithLabelValues(target, location, strconv.FormatBool(cached)).Observe(duration)
}

func (p *PrometheusPlugin) RecordCacheHit(target, location string) {
	p.cacheHitsTotal.Inc()
}

func (p *PrometheusPlugin) RecordCacheMiss(target, location string) {
	p.cacheMissesTotal.Inc()
}

func (p *PrometheusPlugin) RecordCacheEviction() {
	p.cacheEvictionsTotal.Inc()
}

func (p *PrometheusPlugin) RecordCacheExpiration() {
	p.cacheExpirationsTotal.Inc()
}

func (p *PrometheusPlugin) UpdateCacheMetrics(hits, misses, evictions, expirations, size, entries int64) {
	p.cacheHitsTotal.Add(float64(hits))
	p.cacheMissesTotal.Add(float64(misses))
	p.cacheEvictionsTotal.Add(float64(evictions))
	p.cacheExpirationsTotal.Add(float64(expirations))
	p.cacheSizeBytes.Set(float64(size))
	p.cacheEntries.Set(float64(entries))
}
