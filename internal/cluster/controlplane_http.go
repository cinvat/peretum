package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/cinvat/peretum/internal/config"
	"k8s.io/klog/v2"
)

type ControlPlaneHTTP struct {
	mu          sync.RWMutex
	isLeader    bool
	nodeID      string
	configDir   string
	configStore *ConfigVersionStore
	configHash  string

	peerURLs []string
}

func NewControlPlaneHTTP(nodeID, configDir string, configStore *ConfigVersionStore, peerURLs []string) *ControlPlaneHTTP {
	return &ControlPlaneHTTP{
		nodeID:      nodeID,
		configDir:   configDir,
		configStore: configStore,
		peerURLs:    peerURLs,
	}
}

func (cph *ControlPlaneHTTP) SetLeader(isLeader bool) {
	cph.mu.Lock()
	defer cph.mu.Unlock()
	cph.isLeader = isLeader
}

func (cph *ControlPlaneHTTP) IsLeader() bool {
	cph.mu.RLock()
	defer cph.mu.RUnlock()
	return cph.isLeader
}

func (cph *ControlPlaneHTTP) NodeID() string {
	cph.mu.RLock()
	defer cph.mu.RUnlock()
	return cph.nodeID
}

func (cph *ControlPlaneHTTP) SetPeerURLs(urls []string) {
	cph.mu.Lock()
	defer cph.mu.Unlock()
	cph.peerURLs = urls
}

func (cph *ControlPlaneHTTP) PeerURLs() []string {
	cph.mu.RLock()
	defer cph.mu.RUnlock()
	return cph.peerURLs
}

// GET /health - basic health check
func (cph *ControlPlaneHTTP) Health(w http.ResponseWriter, r *http.Request) {
	cph.mu.RLock()
	defer cph.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "ok",
		"node_id":   cph.nodeID,
		"is_leader": cph.isLeader,
		"time":      time.Now().Unix(),
	})
}

// GET /health/leader - returns 200 if this node is leader
func (cph *ControlPlaneHTTP) HealthLeader(w http.ResponseWriter, r *http.Request) {
	cph.mu.RLock()
	defer cph.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if cph.isLeader {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"leader": cph.nodeID,
			"status": "ok",
		})
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "not leader",
		})
	}
}

// POST /leadership/claim - claim leadership
func (cph *ControlPlaneHTTP) ClaimLeadership(w http.ResponseWriter, r *http.Request) {
	candidateID := r.Header.Get("X-Candidate-ID")
	if candidateID == "" {
		http.Error(w, "missing candidate ID", http.StatusBadRequest)
		return
	}

	cph.mu.Lock()
	defer cph.mu.Unlock()

	if cph.isLeader {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{
			"error":         "already have leader",
			"currentLeader": cph.nodeID,
		})
		return
	}

	cph.isLeader = true
	cph.nodeID = candidateID

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "leadership granted",
		"leader": candidateID,
	})
}

// POST /leadership/resign - resign leadership
func (cph *ControlPlaneHTTP) ResignLeadership(w http.ResponseWriter, r *http.Request) {
	cph.mu.Lock()
	defer cph.mu.Unlock()

	if !cph.isLeader {
		http.Error(w, "not leader", http.StatusConflict)
		return
	}

	cph.isLeader = false
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "leadership resigned",
	})
}

// GET /sync - full config sync for standby
func (cph *ControlPlaneHTTP) SyncConfig(w http.ResponseWriter, r *http.Request) {
	cph.mu.RLock()
	isLeader := cph.isLeader
	cph.mu.RUnlock()

	if !isLeader {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}

	targets := cph.configStore.GetAllTargets()
	w.Header().Set("Content-Type", "application/json")

	resp := map[string]interface{}{
		"version": time.Now().Unix(),
		"targets": make(map[string]interface{}),
	}

	targetsMap := resp["targets"].(map[string]interface{})
	for name, target := range targets {
		targetsMap[name] = map[string]interface{}{
			"target":    target.Target,
			"version":   target.Version,
			"loaded_at": target.LoadedAt.Unix(),
		}
	}

	json.NewEncoder(w).Encode(resp)
}

// POST /sync/target - sync a single target
func (cph *ControlPlaneHTTP) SyncTarget(w http.ResponseWriter, r *http.Request) {
	cph.mu.RLock()
	isLeader := cph.isLeader
	cph.mu.RUnlock()

	if !isLeader {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	if req.Name == "" {
		http.Error(w, "missing target name", http.StatusBadRequest)
		return
	}

	target, ok := cph.configStore.GetTarget(req.Name)
	if !ok {
		http.Error(w, "target not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"target":    target.Target,
		"version":   target.Version,
		"loaded_at": target.LoadedAt.Unix(),
	})
}

// POST /config/reload - trigger config reload
func (cph *ControlPlaneHTTP) ReloadConfig(w http.ResponseWriter, r *http.Request) {
	cph.mu.RLock()
	isLeader := cph.isLeader
	cph.mu.RUnlock()

	if !isLeader {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}

	// This would trigger a reload in the main server
	// For now, just return success
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "reload triggered",
	})
}

// GET /stats - control plane stats
func (cph *ControlPlaneHTTP) Stats(w http.ResponseWriter, r *http.Request) {
	cph.mu.RLock()
	defer cph.mu.RUnlock()

	stats := map[string]interface{}{
		"node_id":   cph.nodeID,
		"is_leader": cph.isLeader,
		"targets":   len(cph.configStore.GetAllTargets()),
		"peer_urls": cph.peerURLs,
	}

	if statsMap := cph.configStore.GetStats(); statsMap != nil {
		for k, v := range statsMap {
			stats[k] = v
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// POST /peer/notify - notify peer of config change
func (cph *ControlPlaneHTTP) NotifyPeer(w http.ResponseWriter, r *http.Request) {
	// Used by leader to notify standby of config changes
	var req struct {
		TargetName string `json:"target_name"`
		Action     string `json:"action"` // "update" or "delete"
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	cph.mu.RLock()
	isLeader := cph.isLeader
	cph.mu.RUnlock()

	if !isLeader {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}

	// In a real implementation, this would push to connected peers
	// For now, just acknowledge
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "notified",
	})
}

// SyncFromLeader syncs config from leader (used by standby)
func (cph *ControlPlaneHTTP) SyncFromLeader(ctx context.Context, leaderURL string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", leaderURL+"/sync", nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sync failed: %s", resp.Status)
	}

	var syncResp struct {
		Version int64 `json:"version"`
		Targets map[string]struct {
			Target   *config.TargetConfig `json:"target"`
			Version  string               `json:"version"`
			LoadedAt int64                `json:"loaded_at"`
		} `json:"targets"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&syncResp); err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	// Update local config store
	for name, target := range syncResp.Targets {
		if target.Target != nil {
			cph.configStore.SetTarget(name, target.Target, target.Version, time.Unix(target.LoadedAt, 0))
		}
	}

	return nil
}

// RunStandbySync runs the sync loop for standby nodes
func (cph *ControlPlaneHTTP) RunStandbySync(ctx context.Context, leaderURL string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Initial sync
	if err := cph.SyncFromLeader(ctx, leaderURL); err != nil {
		klog.Warningf("Initial sync from leader failed: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := cph.SyncFromLeader(ctx, leaderURL); err != nil {
				klog.Warningf("Sync from leader failed: %v", err)
			}
		}
	}
}
