package proxyserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cinvat/peretum/internal/cluster"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/router"
	"gopkg.in/yaml.v3"
	"k8s.io/klog/v2"
	"net/http"
)

// lazyMaterialize is the load path the lazy router's handlers use. The indirection
// exists so tests can count how often a target is compiled; production always
// goes through materializeTarget.
func (ps *proxyServer) lazyMaterialize(ctx context.Context, hostname string) (*router.TargetConfigHandler, error) {
	if ps.lazyLoad != nil {
		return ps.lazyLoad(ctx, hostname)
	}
	return ps.materializeTarget(ctx, hostname)
}

// buildLazyHostRouter builds a router that resolves hosts from the target store
// instead of holding them in memory.
//
// The routing table is left empty on purpose. A CDN edge may hold 10M+ targets,
// and one map entry per hostname would put every cold target back in RAM, which
// is exactly what lazy loading exists to avoid. Instead the store is the routing
// table: a request costs one point lookup on h/<hostname> to decide whether the
// host exists, and the config itself is only read and compiled if it does.
//
// That lookup runs on every request, but it is a Pebble point read against a
// key the block cache already has, and it replaces an unbounded map. Memory is
// therefore O(1) plus the bounded handler LRU, independent of target count.
//
// One consequence worth noting: a target becomes routable the moment its event
// is applied to the store, so incremental updates no longer need to touch the
// router at all. applyTargetUpdate only has to invalidate the LRU entry.
func (ps *proxyServer) buildLazyHostRouter() *router.HostRouter {
	hr := router.NewHostRouter()
	hr.Reload(nil, nil) // no in-memory table: the store is the routing table

	// Shared by every host, so the per-request handler the resolver builds below
	// still coalesces concurrent first requests for the same cold target.
	flights := router.NewFlightTable(ps.lazyLRU)

	hr.SetResolver(func(ctx context.Context, host string) (http.Handler, bool) {
		exists, err := ps.targetStore.HasTargetByHost(ctx, host)
		if err != nil {
			klog.Errorf("lazy router: lookup %s: %v", host, err)
			return nil, false
		}
		if !exists {
			return nil, false
		}
		return router.NewLazyHandler(host, ps.lazyLRU, func(ctx context.Context) (*router.TargetConfigHandler, error) {
			return ps.lazyMaterialize(ctx, host)
		}, flights), true
	})

	ps.lazyRouter = hr
	return hr
}

// materializeTarget loads a target config from the Pebble store by hostname,
// compiles its handlers, and returns the compiled TargetConfigHandler.
func (ps *proxyServer) materializeTarget(ctx context.Context, hostname string) (*router.TargetConfigHandler, error) {
	data, ok, err := ps.targetStore.GetTargetByHost(ctx, hostname)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("target for hostname %q not in store", hostname)
	}

	var t config.TargetConfig
	if err := yaml.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("target for hostname %s: %w", hostname, err)
	}

	tch, hc, err := ps.buildTargetConfigHandler(&t)
	if err != nil {
		return nil, err
	}
	if hc != nil {
		klog.Warningf("active health checks are disabled for lazily loaded target %s", hostname)
	}
	return tch, nil
}

// ensureLazyStore provisions the Pebble store before the first router build.
// A non-empty store is used as-is (fast restart).
//
// When a NATS URI is configured the store is owned by the config event store:
// the consumer started in newProxyServer replays the current state of every
// target into the store, so there is nothing to seed here and no local
// config.d is read. Without NATS the store is seeded from local config.d.
func (ps *proxyServer) ensureLazyStore() error {
	if ps.targetStore == nil {
		return nil
	}

	ctx := context.Background()
	provisioned, err := ps.targetStore.HasAnyTarget(ctx)
	if err != nil {
		return fmt.Errorf("target store: %w", err)
	}
	if provisioned {
		// Deliberately not counting the rows: at CDN scale that is a full
		// keyspace scan just to produce a log line.
		klog.Infof("target store already provisioned; using as-is")
		return nil
	}

	// With a NATS URI the store is filled by replaying the config event store
	// in the consumer started above; local config.d is deliberately ignored so
	// that the control plane stays the single source of truth.
	if ps.proxyCfg != nil && ps.proxyCfg.Cluster != nil && ps.proxyCfg.Cluster.NATSURI != "" {
		klog.Infof("config sourced from NATS event store at %s; skipping local config.d", ps.proxyCfg.Cluster.NATSURI)
		return nil
	}

	// No control plane: seed from local config.d targets.
	// Store each target under its hostname(s) from the listen field.
	for i := range ps.targets {
		t := &ps.targets[i]
		data, err := yaml.Marshal(t)
		if err != nil {
			klog.Errorf("seed target %s: %v", t.ServerName, err)
			continue
		}
		// Determine hostnames from server_name field (comma-separated)
		for _, hostname := range config.ParseServerNames(t.ServerName) {
			if err := ps.targetStore.PutTargetByHost(ctx, hostname, data); err != nil {
				klog.Errorf("seed target %s for hostname %s: %v", t.ServerName, hostname, err)
			}
		}
	}
	return nil
}

// parseHostnames extracts hostnames from the listen field.
// If listen is empty or port-only (starts with ":"), falls back to target name.
// Multiple hostnames can be comma-separated.
// parseServerNames extracts hostnames from the server_name field.
// server_name can be comma-separated for multiple hostnames.
func (ps *proxyServer) applyTargetEvent(ctx context.Context, event *cluster.TargetEvent) error {
	if event == nil {
		return errors.New("nil event")
	}
	if event.Deleted {
		ps.applyTargetDelete(event.ServerName)
		return nil
	}
	var targetConfig config.TargetConfig
	if err := json.Unmarshal(event.Config, &targetConfig); err != nil {
		return fmt.Errorf("unmarshal config for %s: %w", event.ServerName, err)
	}
	return ps.applyTargetUpdate(event.ServerName, &targetConfig)
}

// applyTargetUpdate persists a target config update from the control plane
// and refreshes the lazy router so the next request serves the new config.
// The config is stored under every hostname it claims via server_name, since
// incoming Host lookups are hostname-keyed.
func (ps *proxyServer) applyTargetUpdate(serverName string, targetConfig *config.TargetConfig) error {
	if targetConfig == nil {
		return errors.New("nil target config")
	}
	klog.Infof("Received target config update from control plane: %s", serverName)

	if ps.targetStore == nil {
		return errors.New("target store not initialized")
	}

	// Fall back to the event's subject name when the config omits server_name.
	names := config.ParseServerNames(targetConfig.ServerName)
	if len(names) == 0 {
		names = config.ParseServerNames(serverName)
	}
	if len(names) == 0 {
		return fmt.Errorf("target %q has no server_name", serverName)
	}

	data, err := yaml.Marshal(targetConfig)
	if err != nil {
		return fmt.Errorf("marshal target %s: %w", serverName, err)
	}

	for _, hostname := range names {
		if err := ps.targetStore.PutTargetByHost(context.Background(), hostname, data); err != nil {
			return fmt.Errorf("store target %s for hostname %s: %w", serverName, hostname, err)
		}

		// The store is the routing table, so an update only has to drop the
		// compiled handler: the next request re-reads the config we just wrote.
		// There is no per-host router entry to add or replace, which is what
		// keeps updates O(1) in memory.
		ps.lazyLRU.Delete(hostname)
	}
	return nil
}

// applyTargetDelete removes a target from the store and the lazy router.
func (ps *proxyServer) applyTargetDelete(name string) {
	klog.Infof("Received target delete from control plane: %s", name)
	if ps.targetStore == nil {
		return
	}
	// In lazy mode, targets are stored by hostname. We don't know the hostnames
	// from just the name, so we delete by name as fallback.
	// Ideally the control plane would send the hostnames in the delete message.
	if err := ps.targetStore.DeleteTargetByHost(context.Background(), name); err != nil {
		klog.Errorf("delete stored target %s: %v", name, err)
	}
	// Drop the compiled handler; the router resolves through the store, so once
	// the row is gone the hostname stops resolving on its own.
	ps.lazyLRU.Delete(name)
}

// buildFrontendHandler wraps the router with every plugin implementing
// base.RouterWrapper (e.g. error_page). The wrapper sits in front of the
// whole router so it can intercept router 404s and any 4xx/5xx response.
