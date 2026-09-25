package router

import (
	"net/http"
	"strings"
	"sync"

	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/handler"
	"k8s.io/klog/v2"
)

// DefaultHostname is the reserved router key used as the fallback target when
// an incoming Host matches no configured target.
const DefaultHostname = "_default"

type HostRouter struct {
	targets        map[string]http.Handler
	defaultHandler http.Handler
	mu             sync.RWMutex
}

type TargetConfigHandler struct {
	Target     *config.TargetConfig
	Handlers   []*handler.TargetHandler
	DefaultLoc *handler.TargetHandler
}

func NewHostRouter() *HostRouter {
	return &HostRouter{
		targets: make(map[string]http.Handler),
	}
}

// NormalizeHost strips the port from an HTTP Host header and lowercases the
// result so that it can be used as a target-store key. It handles bracketed
// IPv6 literals such as "[::1]:8443", which a naive split on ":" would mangle.
func NormalizeHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return ""
	}

	// Bracketed IPv6 literal: "[::1]" or "[::1]:8080".
	if strings.HasPrefix(host, "[") {
		if end := strings.Index(host, "]"); end != -1 {
			return host[:end+1]
		}
		return host
	}

	// Bare IPv6 literal has multiple colons and no port, so only strip after
	// the last colon when exactly one is present.
	if strings.Count(host, ":") == 1 {
		if idx := strings.Index(host, ":"); idx != -1 {
			host = host[:idx]
		}
	}

	// A trailing dot is a valid absolute form of the same name.
	return strings.TrimSuffix(host, ".")
}

func (hr *HostRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	host := NormalizeHost(req.Host)

	hr.mu.RLock()
	tch, ok := hr.targets[host]
	if !ok {
		tch, ok = hr.targets[DefaultHostname]
	}
	def := hr.defaultHandler
	hr.mu.RUnlock()

	if !ok && def != nil {
		klog.V(4).Infof("Using default handler for host: %s", host)
		def.ServeHTTP(w, req)
		return
	}

	if tch != nil {
		klog.V(4).Infof("Found target handler for host: %s", host)
		tch.ServeHTTP(w, req)
		return
	}

	klog.V(4).Infof("No matching target for host: %s", host)
	http.Error(w, "No matching target for host", http.StatusNotFound)
}

func (tch *TargetConfigHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path

	var best *handler.TargetHandler
	var bestPriority int

	for _, h := range tch.Handlers {
		if !h.Location().Matches(path) {
			continue
		}

		var priority int
		switch h.Location().MatchType {
		case config.MatchExact:
			priority = 1000
		case config.MatchRegex:
			priority = 500
		case config.MatchPrefix:
			priority = len(strings.TrimSuffix(h.Location().Path, "/"))
		}

		if priority > bestPriority {
			best = h
			bestPriority = priority
		}
	}

	if best != nil {
		best.ServeHTTP(w, req)
		return
	}

	if tch.DefaultLoc != nil {
		tch.DefaultLoc.ServeHTTP(w, req)
		return
	}

	http.Error(w, "No matching location", http.StatusNotFound)
}

// Reload atomically replaces the entire routing table.
func (hr *HostRouter) Reload(targets map[string]http.Handler, defaultHandler http.Handler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.targets = targets
	hr.defaultHandler = defaultHandler
}

// Upsert installs (or replaces) a single host handler without rebuilding the
// whole routing table. Used for incremental lazy-mode updates.
func (hr *HostRouter) Upsert(host string, h http.Handler) {
	if host == "" || h == nil {
		return
	}
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.targets[host] = h
}

// RemoveHost deletes a host from the routing table. In-flight requests hold
// the old handler pointer, so they finish safely.
func (hr *HostRouter) RemoveHost(host string) {
	if host == "" {
		return
	}
	hr.mu.Lock()
	defer hr.mu.Unlock()
	delete(hr.targets, host)
}

// GetTargets returns a copy of the routing table to prevent external mutation.
func (hr *HostRouter) GetTargets() map[string]http.Handler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	result := make(map[string]http.Handler, len(hr.targets))
	for k, v := range hr.targets {
		result[k] = v
	}
	return result
}

func (hr *HostRouter) GetDefaultHandler() http.Handler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.defaultHandler
}
