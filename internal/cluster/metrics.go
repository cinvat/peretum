package cluster

import (
	"sync/atomic"
	"time"
)

// MetricsCollector collects and exposes metrics for CDN scale operations.
type MetricsCollector struct {
	// Reload metrics
	reloadTotal     atomic.Uint64
	reloadDuration  atomic.Uint64 // nanoseconds
	reloadErrors    atomic.Uint64
	deltaReloads    atomic.Uint64
	fullReloads     atomic.Uint64
	
	// Config metrics
	targetsLoaded    atomic.Uint64
	targetsChanged   atomic.Uint64
	targetsDeleted   atomic.Uint64
	hotTierHits      atomic.Uint64
	hotTierMisses    atomic.Uint64
	warmTierHits     atomic.Uint64
	warmTierMisses   atomic.Uint64
	evictions        atomic.Uint64
	
	// Config streaming
	streamConnects   atomic.Uint64
	streamDisconnects atomic.Uint64
	streamUpdates    atomic.Uint64
	streamErrors     atomic.Uint64
	
	// Request metrics
	requestsTotal    atomic.Uint64
	requestsCached   atomic.Uint64
	requestsProxied  atomic.Uint64
	requestErrors    atomic.Uint64
	
	// Latency buckets (in microseconds)
	latencyBuckets [10]atomic.Uint64 // <1ms, <5ms, <10ms, <50ms, <100ms, <500ms, <1s, <5s, <10s, >10s
}

// RecordReload records a reload event with duration.
func (m *MetricsCollector) RecordReload(isDelta bool, duration time.Duration, err error) {
	if m == nil {
		return
	}
	m.reloadTotal.Add(1)
	m.reloadDuration.Add(uint64(duration.Nanoseconds()))
	if err != nil {
		m.reloadErrors.Add(1)
	}
	if isDelta {
		m.deltaReloads.Add(1)
	} else {
		m.fullReloads.Add(1)
	}
}

// RecordTargetChange records a target change event.
func (m *MetricsCollector) RecordTargetChange(changed, deleted bool) {
	if m == nil {
		return
	}
	m.targetsLoaded.Add(1)
	if changed {
		m.targetsChanged.Add(1)
	}
	if deleted {
		m.targetsDeleted.Add(1)
	}
}

// RecordCacheHit records a cache tier hit.
func (m *MetricsCollector) RecordCacheHit(tier string) {
	if m == nil {
		return
	}
	switch tier {
	case "hot":
		m.hotTierHits.Add(1)
	case "warm":
		m.warmTierHits.Add(1)
	}
}

// RecordCacheMiss records a cache tier miss.
func (m *MetricsCollector) RecordCacheMiss(tier string) {
	if m == nil {
		return
	}
	switch tier {
	case "hot":
		m.hotTierMisses.Add(1)
	case "warm":
		m.warmTierMisses.Add(1)
	}
}

// RecordEviction records a tenant eviction.
func (m *MetricsCollector) RecordEviction() {
	if m == nil {
		return
	}
	m.evictions.Add(1)
}

// RecordStreamEvent records a config streaming event.
func (m *MetricsCollector) RecordStreamEvent(eventType string) {
	if m == nil {
		return
	}
	switch eventType {
	case "connect":
		m.streamConnects.Add(1)
	case "disconnect":
		m.streamDisconnects.Add(1)
	case "update":
		m.streamUpdates.Add(1)
	case "error":
		m.streamErrors.Add(1)
	}
}

// RecordRequest records a request with latency.
func (m *MetricsCollector) RecordRequest(cached bool, latency time.Duration) {
	if m == nil {
		return
	}
	m.requestsTotal.Add(1)
	if cached {
		m.requestsCached.Add(1)
	} else {
		m.requestsProxied.Add(1)
	}
	
	us := latency.Microseconds()
	if us < 1000 {
		m.latencyBuckets[0].Add(1)
	} else if us < 5000 {
		m.latencyBuckets[1].Add(1)
	} else if us < 10000 {
		m.latencyBuckets[2].Add(1)
	} else if us < 50000 {
		m.latencyBuckets[3].Add(1)
	} else if us < 100000 {
		m.latencyBuckets[4].Add(1)
	} else if us < 500000 {
		m.latencyBuckets[5].Add(1)
	} else if us < 1000000 {
		m.latencyBuckets[6].Add(1)
	} else if us < 5000000 {
		m.latencyBuckets[7].Add(1)
	} else if us < 10000000 {
		m.latencyBuckets[8].Add(1)
	} else {
		m.latencyBuckets[9].Add(1)
	}
}

// RecordRequestError records a request error.
func (m *MetricsCollector) RecordRequestError() {
	if m == nil {
		return
	}
	m.requestErrors.Add(1)
}

// GetStats returns all metrics as a map.
func (m *MetricsCollector) GetStats() map[string]interface{} {
	if m == nil {
		return map[string]interface{}{}
	}
	return map[string]interface{}{
		"reload": map[string]interface{}{
			"total":       m.reloadTotal.Load(),
			"duration_ns": m.reloadDuration.Load(),
			"errors":      m.reloadErrors.Load(),
			"delta":       m.deltaReloads.Load(),
			"full":        m.fullReloads.Load(),
			"avg_duration_ns": func() uint64 {
				total := m.reloadTotal.Load()
				if total == 0 {
					return 0
				}
				return m.reloadDuration.Load() / total
			}(),
		},
		"targets": map[string]interface{}{
			"loaded":   m.targetsLoaded.Load(),
			"changed":  m.targetsChanged.Load(),
			"deleted":  m.targetsDeleted.Load(),
		},
		"cache": map[string]interface{}{
			"hot_hits":   m.hotTierHits.Load(),
			"hot_misses": m.hotTierMisses.Load(),
			"warm_hits":  m.warmTierHits.Load(),
			"warm_misses": m.warmTierMisses.Load(),
			"evictions":  m.evictions.Load(),
		},
		"streaming": map[string]interface{}{
			"connects":    m.streamConnects.Load(),
			"disconnects": m.streamDisconnects.Load(),
			"updates":     m.streamUpdates.Load(),
			"errors":      m.streamErrors.Load(),
		},
		"requests": map[string]interface{}{
			"total":     m.requestsTotal.Load(),
			"cached":    m.requestsCached.Load(),
			"proxied":   m.requestsProxied.Load(),
			"errors":    m.requestErrors.Load(),
			"latency_buckets": map[string]uint64{
				"<1ms":      m.latencyBuckets[0].Load(),
				"<5ms":      m.latencyBuckets[1].Load(),
				"<10ms":     m.latencyBuckets[2].Load(),
				"<50ms":     m.latencyBuckets[3].Load(),
				"<100ms":    m.latencyBuckets[4].Load(),
				"<500ms":    m.latencyBuckets[5].Load(),
				"<1s":       m.latencyBuckets[6].Load(),
				"<5s":       m.latencyBuckets[7].Load(),
				"<10s":      m.latencyBuckets[8].Load(),
				">10s":      m.latencyBuckets[9].Load(),
			},
		},
	}
}

// Reset resets all metrics.
func (m *MetricsCollector) Reset() {
	m.reloadTotal.Store(0)
	m.reloadDuration.Store(0)
	m.reloadErrors.Store(0)
	m.deltaReloads.Store(0)
	m.fullReloads.Store(0)
	m.targetsLoaded.Store(0)
	m.targetsChanged.Store(0)
	m.targetsDeleted.Store(0)
	m.hotTierHits.Store(0)
	m.hotTierMisses.Store(0)
	m.warmTierHits.Store(0)
	m.warmTierMisses.Store(0)
	m.evictions.Store(0)
	m.streamConnects.Store(0)
	m.streamDisconnects.Store(0)
	m.streamUpdates.Store(0)
	m.streamErrors.Store(0)
	m.requestsTotal.Store(0)
	m.requestsCached.Store(0)
	m.requestsProxied.Store(0)
	m.requestErrors.Store(0)
	for i := range m.latencyBuckets {
		m.latencyBuckets[i].Store(0)
	}
}