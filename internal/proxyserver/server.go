package proxyserver

import (
	"context"
	disk "github.com/cinvat/peretum/internal/cache/disk"
	"github.com/cinvat/peretum/internal/cluster"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/loadbalancer"
	"github.com/cinvat/peretum/internal/plugin/manager"
	"github.com/cinvat/peretum/internal/router"
	"github.com/quic-go/quic-go/http3"
	"k8s.io/klog/v2"
	"net"
	"net/http"
	"path/filepath"
	"sync"
)

const (
	maxWriteWorkers = 8
)

type proxyServer struct {
	router      *router.HostRouter
	diskCache   *disk.DiskCache
	writeSem    chan struct{}
	proxyCfg    *config.ProxyConfig
	maxBodySize int64
	targets     []config.TargetConfig
	pluginMgr   *manager.PluginManager

	// Health checkers for each target (keyed by target name).
	healthCheckers map[string]*loadbalancer.HealthChecker

	// Lazy mode (cluster.lazy): the Pebble target store holds cold configs
	// off-RAM; lazyLRU caches compiled handlers for hot hosts; lazyRouter is
	// the live router updated incrementally from NATS updates.
	targetStore *cluster.TargetStore
	lazyLRU     *cluster.LRUCache[string, *router.TargetConfigHandler]
	lazyRouter  *router.HostRouter

	// lazyLoad materializes a target by hostname. It exists so tests can observe
	// how many times a cold target is compiled; production always uses
	// materializeTarget. buildLazyHostRouter resolves through it.
	lazyLoad func(ctx context.Context, hostname string) (*router.TargetConfigHandler, error)

	// NATS config sync for control plane updates
	natsSync *cluster.NATSConfigSync

	// configReady is closed once the retained config events have been applied
	// to the target store and the live consumer is attached. Startup waits for
	// it, because a store-backed router resolves hostnames by looking them up:
	// a target that has not been replayed yet is a missing route, so serving
	// mid-replay turns into 404s that look like a broken origin. Nil when
	// there is no event stream to replay.
	configReady <-chan struct{}

	// configErr is why the event stream could not be started, reported by
	// start() so the process fails loudly instead of serving an empty store.
	configErr error

	// cfgPath/targetsDir are the config file and target directory the proxy
	// was started with, so SIGHUP reloads re-read the same sources no matter
	// which working directory the process runs from. Empty keeps the legacy
	// "config.yaml"/"config.d" defaults.
	cfgPath    string
	targetsDir string

	// frontend is the top-level request handler served on every listener.
	// It wraps the router with any plugins implementing base.RouterWrapper
	// (e.g. error_page) and is rebuilt on reload so config changes apply.
	frontend http.Handler

	// srvs/lns/h3Srvs/h3Conns hold every active frontend listener so the
	// proxy supports multiple nginx-style `listeners:` blocks at once. The
	// singular srv/ln/h3Srv/h3Conn fields keep pointing at the first TCP and
	// first QUIC listener for backward compatibility.
	srvs        []*http.Server
	lns         []net.Listener
	h3Srvs      []*http3.Server
	h3Conns     []net.PacketConn
	listenSpecs []config.ListenersSpec
	srv         *http.Server
	ln          net.Listener
	h3Srv       *http3.Server
	h3Conn      net.PacketConn
	mu          sync.Mutex
}

func newProxyServer(proxyCfg *config.ProxyConfig, targets []config.TargetConfig, diskCache *disk.DiskCache, writeSem chan struct{}, pluginMgr *manager.PluginManager, maxBodySize int64) *proxyServer {
	ps := &proxyServer{
		router:         router.NewHostRouter(),
		diskCache:      diskCache,
		writeSem:       writeSem,
		proxyCfg:       proxyCfg,
		targets:        targets,
		pluginMgr:      pluginMgr,
		maxBodySize:    maxBodySize,
		healthCheckers: make(map[string]*loadbalancer.HealthChecker),
	}

	// Initialize the lazy on-disk target store when clustering is enabled.
	if proxyCfg != nil && proxyCfg.Cluster != nil {
		// Lazy mode: open the on-disk target store (Pebble) so configs can
		// stay off-RAM. The store is filled by the config event store, or
		// seeded from config.d when no NATS URI is set (see ensureLazyStore).
		if proxyCfg.Cluster.Lazy {
			storeDir := proxyCfg.Cluster.DataDir
			if storeDir == "" {
				if proxyCfg.CacheDir != "" {
					storeDir = filepath.Join(proxyCfg.CacheDir, "targetstore")
				} else {
					storeDir = filepath.Join(".peretum", "targetstore")
				}
			}
			ts, err := cluster.OpenTargetStore(storeDir)
			if err != nil {
				klog.Errorf("failed to open target store at %s: %v", storeDir, err)
			} else {
				ps.targetStore = ts
				lruSize := proxyCfg.Cluster.LRUSize
				if lruSize <= 0 {
					lruSize = 1000
				}
				ps.lazyLRU = cluster.NewLRUCache[string, *router.TargetConfigHandler](lruSize)
				klog.Infof("lazy target loading enabled: store=%s lru_capacity=%d", storeDir, ps.lazyLRU.Capacity())
			}
		}
	}

	// Initialize NATS config sync if a NATS cluster is configured.
	if proxyCfg != nil && proxyCfg.Cluster != nil && proxyCfg.Cluster.NATSURI != "" {
		natsSync, err := cluster.NewNATSConfigSync(context.Background(), proxyCfg.Cluster.NATSURI, cluster.DefaultStreamConfig())
		if err != nil {
			klog.Errorf("failed to create NATS sync: %v", err)
		} else {
			ps.natsSync = natsSync

			// Consume the config event store. Start replays the retained state
			// of every target into the store and then follows live updates,
			// signalling configReady when the edge is caught up. start() holds
			// off binding listeners until then, because until the replay lands
			// a target has no route to serve.
			ps.configReady, ps.configErr = natsSync.Start(context.Background(), ps.applyTargetEvent)
			if ps.configErr != nil {
				klog.Errorf("failed to start NATS config consumer: %v", ps.configErr)
			}

			// Subscribe to cache purge broadcasts. The handler purges the
			// live disk cache in memory, so every edge drops the entries
			// without scanning files.
			if _, err := natsSync.SubscribeCachePurge(ps.handlePurgeEvent); err != nil {
				klog.Errorf("failed to subscribe to cache purge: %v", err)
			}
		}
	}

	return ps
}
