package cdnscale

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cinvat/peretum/internal/config"
)

// TargetConfigVersion holds a versioned target configuration.
type TargetConfigVersion struct {
	Target     *config.TargetConfig
	Version    string    // SHA256 hash of the config
	LoadedAt   time.Time
	AccessedAt time.Time
	AccessCount uint64
}

// ConfigVersionStore manages versioned target configurations with delta reload support.
type ConfigVersionStore struct {
	mu           sync.RWMutex
	targets      map[string]*TargetConfigVersion // target name -> versioned config
	globalConfig *config.ProxyConfig
	globalHash   string
	
	// Metrics
	reloadCount     uint64
	deltaReloads    uint64
	fullReloads     uint64
	tenantReloads   uint64
	lastReloadTime  time.Time
	lastReloadError string
	
	// Tiered config
	hotTier   *LRUCache[string, *TargetConfigVersion] // hot tenants in memory
	warmTier  map[string][]byte                       // serialized configs (compressed)
	hotTierSize int
	maxHotTierSize int
}

func NewConfigVersionStore(hotTierSize int) *ConfigVersionStore {
	if hotTierSize <= 0 {
		hotTierSize = 1000
	}
	return &ConfigVersionStore{
		targets:       make(map[string]*TargetConfigVersion),
		warmTier:      make(map[string][]byte),
		hotTier:       NewLRUCache[string, *TargetConfigVersion](hotTierSize),
		maxHotTierSize: hotTierSize,
	}
}

// computeHash computes SHA256 hash of a target config.
func computeHash(target *config.TargetConfig) string {
	// Create a deterministic string representation
	s := fmt.Sprintf("%s|%s|%d|%d",
		target.Name,
		target.Listen,
		len(target.Upstreams),
		len(target.Locations),
	)
	for _, u := range target.Upstreams {
		hc := ""
		if u.HealthCheck != nil {
			hc = u.HealthCheck.Path
		}
		s += fmt.Sprintf("|%s|%d|%s", u.URL, u.Weight, hc)
	}
	for _, loc := range target.Locations {
		s += fmt.Sprintf("|%s|%s|%v|%s", loc.Path, loc.MatchType, loc.Cache, loc.CacheTTL)
	}
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])
}

// computeGlobalHash computes hash of global proxy config.
func computeGlobalHash(cfg *config.ProxyConfig) string {
	s := fmt.Sprintf("%v|%v|%v|%v",
		cfg.Listeners,
		cfg.CacheDir,
		cfg.MaxCacheSize,
		cfg.MaxCacheAge,
	)
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])
}

// LoadTargets loads targets and computes their versions.
// Returns the new targets map and a list of target names that changed.
func (cvs *ConfigVersionStore) LoadTargets(targetsDir string) (map[string]*TargetConfigVersion, []string, error) {
	targets, err := config.LoadTargets(targetsDir)
	if err != nil {
		return nil, nil, err
	}

	changed := make([]string, 0)
	newTargets := make(map[string]*TargetConfigVersion)

	cvs.mu.Lock()
	defer cvs.mu.Unlock()

	for _, target := range targets {
		t := target // copy to get pointer
		name := t.Name
		if name == "" {
			continue // skip targets without name
		}
		hash := computeHash(&t)
		existing := cvs.targets[name]
		
		if existing == nil || existing.Version != hash {
			// Config changed or new target
			changed = append(changed, name)
			newTargets[name] = &TargetConfigVersion{
				Target:      &t,
				Version:     hash,
				LoadedAt:    time.Now(),
				AccessedAt:  time.Now(),
				AccessCount: 0,
			}
		} else {
			// Unchanged - keep existing version, update access time
			existing.AccessedAt = time.Now()
			existing.AccessCount++
			newTargets[name] = existing
		}
	}

	// Remove deleted targets
	for name := range cvs.targets {
		if _, ok := newTargets[name]; !ok {
			changed = append(changed, name+" (deleted)")
			// Remove from warm tier too
			delete(cvs.warmTier, name)
		}
	}

	cvs.targets = newTargets
	return cvs.targets, changed, nil
}

// LoadGlobalConfig loads global config and returns whether it changed.
func (cvs *ConfigVersionStore) LoadGlobalConfig(cfgPath string) (bool, error) {
	cfg, err := config.LoadProxy(cfgPath)
	if err != nil {
		return false, err
	}

	hash := computeGlobalHash(cfg)
	changed := cvs.globalHash != hash
	
	cvs.mu.Lock()
	defer cvs.mu.Unlock()
	
	cvs.globalConfig = cfg
	cvs.globalHash = hash
	return changed, nil
}

// GetTarget returns a target config by name, updating access stats.
func (cvs *ConfigVersionStore) GetTarget(name string) (*TargetConfigVersion, bool) {
	cvs.mu.RLock()
	target, ok := cvs.targets[name]
	cvs.mu.RUnlock()
	
	if !ok {
		return nil, false
	}
	
	// Update access stats (async to avoid lock contention)
	go func() {
		cvs.mu.Lock()
		if t, ok := cvs.targets[name]; ok {
			t.AccessedAt = time.Now()
			t.AccessCount++
		}
		cvs.mu.Unlock()
	}()
	
	// Also add to hot tier if frequently accessed
	if target.AccessCount > 10 {
		cvs.hotTier.Put(name, target)
	}
	
	return target, true
}

// GetAllTargets returns all current targets.
func (cvs *ConfigVersionStore) GetAllTargets() map[string]*TargetConfigVersion {
	cvs.mu.RLock()
	defer cvs.mu.RUnlock()
	
	result := make(map[string]*TargetConfigVersion, len(cvs.targets))
	for k, v := range cvs.targets {
		result[k] = v
	}
	return result
}

// GetGlobalConfig returns the current global config.
func (cvs *ConfigVersionStore) GetGlobalConfig() *config.ProxyConfig {
	cvs.mu.RLock()
	defer cvs.mu.RUnlock()
	return cvs.globalConfig
}

// RecordReload records a reload event.
func (cvs *ConfigVersionStore) RecordReload(isDelta bool, err error) {
	cvs.mu.Lock()
	defer cvs.mu.Unlock()
	
	cvs.reloadCount++
	cvs.lastReloadTime = time.Now()
	if err != nil {
		cvs.lastReloadError = err.Error()
	} else {
		cvs.lastReloadError = ""
	}
	if isDelta {
		cvs.deltaReloads++
	} else {
		cvs.fullReloads++
	}
}

// RecordTenantReload increments tenant reload counter.
func (cvs *ConfigVersionStore) RecordTenantReload() {
	cvs.mu.Lock()
	defer cvs.mu.Unlock()
	cvs.tenantReloads++
}

// GetStats returns reload statistics.
func (cvs *ConfigVersionStore) GetStats() map[string]interface{} {
	cvs.mu.RLock()
	defer cvs.mu.RUnlock()
	
	return map[string]interface{}{
		"total_reloads":      cvs.reloadCount,
		"delta_reloads":      cvs.deltaReloads,
		"full_reloads":       cvs.fullReloads,
		"tenant_reloads":     cvs.tenantReloads,
		"last_reload_time":   cvs.lastReloadTime,
		"last_reload_error":  cvs.lastReloadError,
		"targets_total":      len(cvs.targets),
		"hot_tier_size":      cvs.hotTier.Len(),
		"warm_tier_size":     len(cvs.warmTier),
	}
}

// PromoteToHot moves a tenant to the hot tier (frequently accessed).
func (cvs *ConfigVersionStore) PromoteToHot(name string) {
	cvs.mu.RLock()
	target, ok := cvs.targets[name]
	cvs.mu.RUnlock()
	
	if ok {
		cvs.hotTier.Put(name, target)
	}
}

// DemoteFromHot removes a tenant from the hot tier.
func (cvs *ConfigVersionStore) DemoteFromHot(name string) {
	cvs.hotTier.Delete(name)
}

// EvictColdTenants evicts least recently accessed tenants from memory
// when memory pressure is high. Returns number of evicted tenants.
func (cvs *ConfigVersionStore) EvictColdTenants(maxMemoryMB int) int {
	// This is a simplified implementation
	// In production, you'd check actual memory usage
	cvs.mu.Lock()
	defer cvs.mu.Unlock()
	
	if len(cvs.targets) <= cvs.maxHotTierSize {
		return 0
	}
	
	// Sort by access time (oldest first)
	type targetInfo struct {
		name       string
		accessedAt time.Time
		accessCount uint64
	}
	
	infos := make([]targetInfo, 0, len(cvs.targets))
	for name, target := range cvs.targets {
		infos = append(infos, targetInfo{
			name:        name,
			accessedAt:  target.AccessedAt,
			accessCount: target.AccessCount,
		})
	}
	
	sort.Slice(infos, func(i, j int) bool {
		// Prioritize by access count (lower = colder), then by access time (older = colder)
		if infos[i].accessCount != infos[j].accessCount {
			return infos[i].accessCount < infos[j].accessCount
		}
		return infos[i].accessedAt.Before(infos[j].accessedAt)
	})
	
	evicted := 0
	targetCount := len(cvs.targets)
	for _, info := range infos {
		if targetCount <= cvs.maxHotTierSize {
			break
		}
		// Don't evict if recently accessed (within 5 minutes)
		if time.Since(info.accessedAt) < 5*time.Minute {
			continue
		}
		// Move to warm tier (serialize)
		if _, ok := cvs.targets[info.name]; ok {
			// In production, serialize and compress
			cvs.warmTier[info.name] = []byte("serialized")
			cvs.hotTier.Delete(info.name)
			targetCount--
			evicted++
		}
	}
	return evicted
}