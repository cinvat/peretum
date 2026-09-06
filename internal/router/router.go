package router

import (
	"net/http"
	"strings"
	"sync"

	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/handler"
	"k8s.io/klog/v2"
)

type HostRouter struct {
	targets        map[string]*TargetConfigHandler
	defaultHandler *handler.TargetHandler
	mu             sync.RWMutex
}

type TargetConfigHandler struct {
	Target     *config.TargetConfig
	Handlers   []*handler.TargetHandler
	DefaultLoc *handler.TargetHandler
}

func NewHostRouter() *HostRouter {
	return &HostRouter{
		targets: make(map[string]*TargetConfigHandler),
	}
}

func (hr *HostRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	klog.Infof("HostRouter ServeHTTP: host=%s, path=%s", req.Host, req.URL.Path)
	host := req.Host
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}

	hr.mu.RLock()
	tch, ok := hr.targets[host]
	hr.mu.RUnlock()

	if !ok {
		hr.mu.RLock()
		tch, ok = hr.targets["_default"]
		hr.mu.RUnlock()
	}

	if !ok && hr.defaultHandler != nil {
		klog.Infof("Using default handler for host: %s", host)
		hr.defaultHandler.ServeHTTP(w, req)
		return
	}

	if tch != nil {
		klog.Infof("Found target handler for host: %s", host)
		tch.ServeHTTP(w, req)
		return
	}

	klog.Infof("No matching target for host: %s", host)
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

func (hr *HostRouter) Reload(targets map[string]*TargetConfigHandler, defaultHandler *handler.TargetHandler) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.targets = targets
	hr.defaultHandler = defaultHandler
}

func (hr *HostRouter) GetTargets() map[string]*TargetConfigHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.targets
}

func (hr *HostRouter) GetDefaultHandler() *handler.TargetHandler {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.defaultHandler
}
