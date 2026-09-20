package cmd

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	disk "github.com/cinvat/peretum/internal/cache/disk"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/handler"
	"github.com/cinvat/peretum/internal/loadbalancer"
	"github.com/cinvat/peretum/internal/plugin/manager"
	"github.com/cinvat/peretum/internal/router"
	"github.com/cinvat/peretum/plugins/base"
	"github.com/cinvat/peretum/plugins/registry"
	"github.com/quic-go/quic-go/http3"
	"k8s.io/klog/v2"
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
	return &proxyServer{
		router:         router.NewHostRouter(),
		diskCache:      diskCache,
		writeSem:       writeSem,
		proxyCfg:       proxyCfg,
		targets:        targets,
		pluginMgr:      pluginMgr,
		maxBodySize:    maxBodySize,
		healthCheckers: make(map[string]*loadbalancer.HealthChecker),
	}
}

func (ps *proxyServer) buildHostRouter() *router.HostRouter {
	hr := router.NewHostRouter()
	targets := make(map[string]*router.TargetConfigHandler)
	var defaultHandler *handler.TargetHandler

	// Build new health checkers for this configuration.
	newHealthCheckers := make(map[string]*loadbalancer.HealthChecker)

	for i := range ps.targets {
		target := &ps.targets[i]
		upstreams, err := target.ParseUpstreams()
		if err != nil {
			klog.Warningf("failed to parse upstreams for %s: %v", target.Name, err)
			continue
		}
		var lbUpstreams []*loadbalancer.Upstream
		for _, u := range upstreams {
			for _, uc := range target.Upstreams {
				if u.String() == uc.URL {
					lbUpstreams = append(lbUpstreams, &loadbalancer.Upstream{
						URL:    u.String(),
						Weight: uc.Weight,
					})
					break
				}
			}
		}
		lb := loadbalancer.New(target.LBAlgorithm, lbUpstreams)

		// Create health checker if any upstream has health_check configured.
		var hcConfig *loadbalancer.HealthCheckConfig
		for _, uc := range target.Upstreams {
			if uc.HealthCheck != nil && uc.HealthCheck.Path != "" {
				interval := 10 * time.Second
				if uc.HealthCheck.Interval != "" {
					if d, err := time.ParseDuration(uc.HealthCheck.Interval); err == nil {
						interval = d
					}
				}
				timeout := 3 * time.Second
				if uc.HealthCheck.Timeout != "" {
					if d, err := time.ParseDuration(uc.HealthCheck.Timeout); err == nil {
						timeout = d
					}
				}
				expectedStatus := uc.HealthCheck.ExpectedStatus
				if expectedStatus == 0 {
					expectedStatus = 200
				}
				hcConfig = &loadbalancer.HealthCheckConfig{
					Path:           uc.HealthCheck.Path,
					Interval:       interval,
					Timeout:        timeout,
					ExpectedStatus: expectedStatus,
					Headers:        uc.HealthCheck.Headers,
				}
				break
			}
		}
		if hcConfig != nil {
			hc := loadbalancer.NewHealthChecker(lb, hcConfig)
			newHealthCheckers[target.Name] = hc
			klog.Infof("enabled active health checks for target %s (path=%s, interval=%v, timeout=%v)", target.Name, hcConfig.Path, hcConfig.Interval, hcConfig.Timeout)
		}

		var handlers []*handler.TargetHandler
		var defaultLoc *handler.TargetHandler

		for j := range target.Locations {
			loc := &target.Locations[j]
			h := handler.NewTargetHandler(target, loc, ps.diskCache, ps.writeSem, lb, ps.pluginMgr, ps.maxBodySize)
			handlers = append(handlers, h)

			if loc.Path == "/" && defaultLoc == nil {
				defaultLoc = h
			}
		}

		tch := &router.TargetConfigHandler{
			Target:     target,
			Handlers:   handlers,
			DefaultLoc: defaultLoc,
		}

		hostKey := target.Name
		if target.Listen != "" && !strings.HasPrefix(target.Listen, ":") {
			if idx := strings.Index(target.Listen, ":"); idx != -1 {
				hostKey = target.Listen[:idx]
			} else {
				hostKey = target.Listen
			}
		}
		targets[hostKey] = tch

		if defaultHandler == nil {
			defaultHandler = defaultLoc
		}
	}

	// Stop old health checkers that are no longer in the new config.
	for name, oldHC := range ps.healthCheckers {
		if _, ok := newHealthCheckers[name]; !ok {
			oldHC.Stop()
			klog.Infof("stopped health checker for target %s", name)
		}
	}
	// Start new health checkers.
	for name, newHC := range newHealthCheckers {
		if _, ok := ps.healthCheckers[name]; !ok {
			newHC.Start(context.Background())
			klog.Infof("started health checker for target %s", name)
		}
	}
	ps.healthCheckers = newHealthCheckers

	hr.Reload(targets, defaultHandler)
	return hr
}

// buildFrontendHandler wraps the router with every plugin implementing
// base.RouterWrapper (e.g. error_page). The wrapper sits in front of the
// whole router so it can intercept router 404s and any 4xx/5xx response.
func (ps *proxyServer) buildFrontendHandler() http.Handler {
	h := http.Handler(ps.router)
	if ps.pluginMgr == nil {
		return h
	}
	for _, p := range ps.pluginMgr.GetPlugins() {
		if rw, ok := p.(base.RouterWrapper); ok {
			klog.Infof("plugin %s wraps the frontend router", p.Name())
			h = rw.WrapRouter(h)
		}
	}
	return h
}

// frontendHandler returns the wrapped frontend handler, falling back to the
// raw router when the frontend was not built yet (e.g. in tests that start
// QUIC listeners directly).
func (ps *proxyServer) frontendHandler() http.Handler {
	if ps.frontend != nil {
		return ps.frontend
	}
	return ps.buildFrontendHandler()
}

func (ps *proxyServer) buildTLSConfig() *tls.Config {
	var certs []tls.Certificate

	for _, target := range ps.targets {
		if target.TLS != nil && target.TLS.CertFile != "" && target.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(target.TLS.CertFile, target.TLS.KeyFile)
			if err != nil {
				klog.Warningf("failed to load TLS cert for target %s: %v", target.Name, err)
				continue
			}
			certs = append(certs, cert)
			klog.Infof("loaded TLS cert for target: %s", target.Name)
		}
	}

	if ps.proxyCfg.TLSCertFile != "" && ps.proxyCfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(ps.proxyCfg.TLSCertFile, ps.proxyCfg.TLSKeyFile)
		if err != nil {
			klog.Warningf("failed to load global TLS cert: %v", err)
		} else {
			certs = append(certs, cert)
			klog.Info("loaded global TLS cert")
		}
	}

	if len(certs) == 0 {
		if ps.tlsRequested() {
			cert, err := generateSelfSignedCert()
			if err != nil {
				klog.Warningf("failed to generate self-signed cert: %v", err)
				return nil
			}
			certs = append(certs, cert)
			klog.Info("no TLS certificates configured; generated self-signed certificate")
		} else {
			return nil
		}
	}

	// Advertise HTTP/2 (and HTTP/1.1 fallback) via ALPN explicitly. The
	// per-handshake config returned by GetConfigForClient replaces the base
	// config, so net/http's automatic "h2" ALPN entry (added to the base
	// config) would otherwise be discarded and every client would negotiate
	// plain HTTP/1.1.
	nextProtos := []string{"h2", "http/1.1"}
	return &tls.Config{
		Certificates: certs,
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			return &tls.Config{Certificates: certs, NextProtos: nextProtos}, nil
		},
		NextProtos: nextProtos,
		MinVersion: tls.VersionTLS12,
	}
}

// tlsRequested reports whether the proxy is expected to serve TLS, either
// because an explicit listener asks for ssl or quic, global TLS cert paths
// are set, or any target declares per-target TLS cert paths. When a
// self-signed certificate is generated in this case, all HTTP versions
// (HTTP/1.1, HTTP/2, and HTTP/3) work out of the box.
func (ps *proxyServer) tlsRequested() bool {
	if ps.proxyCfg == nil {
		return false
	}
	// Check listeners for ssl/quic flags
	specs, _ := ps.proxyCfg.ParseListeners()
	for _, sp := range specs {
		if sp.SSL || sp.QUIC {
			return true
		}
	}
	if ps.proxyCfg.TLSCertFile != "" || ps.proxyCfg.TLSKeyFile != "" {
		return true
	}
	for i := range ps.targets {
		if t := ps.targets[i].TLS; t != nil && (t.CertFile != "" || t.KeyFile != "") {
			return true
		}
	}
	return false
}

// generateSelfSignedCert creates a throwaway ECDSA certificate so the proxy
// can serve TLS, HTTP/2, and HTTP/3 when no real certificate is configured.
// Injectable crypto seams so tests can exercise the failure branches of
// self-signed certificate generation without touching the real RNG.
var (
	ecdsaGenerateKey       = ecdsa.GenerateKey
	createX509Certificate  = x509.CreateCertificate
	marshalECPrivateKey    = x509.MarshalECPrivateKey
	generateSelfSignedCert = newSelfSignedCert
)

// newSelfSignedCert creates a throwaway ECDSA certificate so the proxy
// can serve TLS, HTTP/2, and HTTP/3 when no real certificate is configured.
// Clients must explicitly trust it (e.g. curl -k).
func newSelfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsaGenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "peretum"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	der, err := createX509Certificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}
	keyDER, err := marshalECPrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal key: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return tls.X509KeyPair(certPEM, keyPEM)
}

func (ps *proxyServer) reload() error {
	cfgPath := ps.cfgPath
	if cfgPath == "" {
		cfgPath = "config.yaml"
	}
	targetsDir := ps.targetsDir
	if targetsDir == "" {
		targetsDir = "config.d"
	}
	return ps.reloadFrom(cfgPath, targetsDir)
}

func (ps *proxyServer) reloadFrom(cfgPath, targetsDir string) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	klog.Info("reloading configuration...")

	proxyCfg, err := config.LoadProxy(cfgPath)
	if err != nil {
		return fmt.Errorf("proxy config: %w", err)
	}
	targets, err := config.LoadTargets(targetsDir)
	if err != nil {
		return fmt.Errorf("targets: %w", err)
	}

	ps.targets = targets
	ps.proxyCfg = proxyCfg

	newRouter := ps.buildHostRouter()

	ps.router.Reload(newRouter.GetTargets(), newRouter.GetDefaultHandler())

	// Allow plugins to reopen/rotate their outputs (e.g. jsonlog) so a
	// logrotate-style rename between SIGHUP reloads is picked up.
	if ps.pluginMgr != nil {
		for _, p := range ps.pluginMgr.GetPlugins() {
			if rp, ok := p.(interface{ Reopen() error }); ok {
				if err := rp.Reopen(); err != nil {
					klog.Warningf("plugin %s reopen failed: %v", p.Name(), err)
				}
			}
		}
	}

	tlsConfig := ps.buildTLSConfig()
	if tlsConfig != nil {
		updated := false
		for _, s := range ps.tcpServers() {
			s.TLSConfig = tlsConfig
			updated = true
		}
		for _, h := range ps.quicServers() {
			h.TLSConfig = tlsConfig.Clone()
			updated = true
		}
		if updated {
			klog.Info("TLS certificates reloaded")
		}
	}

	klog.Infof("reloaded: %d targets", len(targets))
	for _, t := range targets {
		klog.Infof("  target: %s (lb=%s)", t.Name, t.LBAlgorithm)
		for _, u := range t.Upstreams {
			klog.Infof("    upstream: %s (weight=%d)", u.URL, u.Weight)
		}
		for _, loc := range t.Locations {
			klog.Infof("    location: %s %s (cache=%v)", loc.MatchType, loc.Path, loc.Cache)
		}
		if t.TLS != nil && t.TLS.CertFile != "" {
			klog.Infof("    TLS: %s", t.TLS.CertFile)
		}
		if t.Listen != "" {
			klog.Infof("    host: %s", t.Listen)
		}
	}

	return nil
}

// tcpServers returns every active TCP http.Server, falling back to the
// legacy singular srv field when no listener slice was built.
func (ps *proxyServer) tcpServers() []*http.Server {
	if len(ps.srvs) > 0 {
		return ps.srvs
	}
	if ps.srv != nil {
		return []*http.Server{ps.srv}
	}
	return nil
}

// quicServers returns every active HTTP/3 server, falling back to the
// legacy singular h3Srv field when no listener slice was built.
func (ps *proxyServer) quicServers() []*http3.Server {
	if len(ps.h3Srvs) > 0 {
		return ps.h3Srvs
	}
	if ps.h3Srv != nil {
		return []*http3.Server{ps.h3Srv}
	}
	return nil
}

// packetConns returns every active QUIC packet connection, falling back to
// the legacy singular h3Conn field when no listener slice was built.
func (ps *proxyServer) packetConns() []net.PacketConn {
	if len(ps.h3Conns) > 0 {
		return ps.h3Conns
	}
	if ps.h3Conn != nil {
		return []net.PacketConn{ps.h3Conn}
	}
	return nil
}

// closeListeners releases any sockets bound before a startup failure so a
// partially started proxy cannot hold onto ports.
func (ps *proxyServer) closeListeners() {
	for _, ln := range ps.lns {
		ln.Close()
	}
	for _, c := range ps.h3Conns {
		c.Close()
	}
}

// listenerSpecs resolves the frontend listeners for start().
func (ps *proxyServer) listenerSpecs() ([]config.ListenersSpec, error) {
	if ps.proxyCfg == nil {
		return []config.ListenersSpec{{Addr: ":8081"}}, nil
	}
	specs, err := ps.proxyCfg.ParseListeners()
	if err != nil {
		return nil, err
	}
	if len(specs) > 0 {
		return specs, nil
	}
	// Default if no listeners configured.
	return []config.ListenersSpec{{Addr: ":8081"}}, nil
}

func (ps *proxyServer) start() error {
	klog.Infof("start() called")
	ps.router = ps.buildHostRouter()
	ps.frontend = ps.buildFrontendHandler()

	specs, err := ps.listenerSpecs()
	if err != nil {
		return fmt.Errorf("listeners: %w", err)
	}
	ps.listenSpecs = specs

	tlsConfig := ps.buildTLSConfig()
	for _, sp := range specs {
		if sp.SSL && tlsConfig == nil {
			return fmt.Errorf("listener %s requires TLS but no certificate is available", sp.Addr)
		}
	}

	protoSet := new(http.Protocols)
	protoSet.SetHTTP1(true)
	protoSet.SetHTTP2(true)
	protoSet.SetUnencryptedHTTP2(true)

	// Bind every TCP listener first so a bind failure cannot leave a
	// partially started proxy behind. A quic-only directive opens no TCP
	// socket; its matching `ssl` line (if any) provides the TCP one.
	for _, sp := range specs {
		if !sp.SSL && sp.QUIC {
			continue
		}
		var tlscfg *tls.Config
		if sp.SSL {
			tlscfg = tlsConfig
		}
		srv := &http.Server{
			Addr:         sp.Addr,
			Handler:      ps.frontendHandler(),
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 60 * time.Second,
			IdleTimeout:  120 * time.Second,
			TLSConfig:    tlscfg,
			// Serve unencrypted HTTP/2 (h2c) directly via net/http so gRPC
			// (HTTP/2 without TLS) works on plaintext frontends.
			Protocols: protoSet,
		}
		ln, err := net.Listen("tcp", sp.Addr)
		if err != nil {
			ps.closeListeners()
			return fmt.Errorf("listen on %s: %w", sp.Addr, err)
		}
		ps.srvs = append(ps.srvs, srv)
		ps.lns = append(ps.lns, ln)
	}
	if len(ps.srvs) > 0 {
		ps.srv = ps.srvs[0]
		ps.ln = ps.lns[0]
	}

	// Bind the QUIC (UDP) listeners.
	for _, sp := range specs {
		if !sp.QUIC {
			continue
		}
		if err := ps.startQuicListener(sp.Addr, tlsConfig); err != nil {
			ps.closeListeners()
			return err
		}
	}
	if len(ps.h3Srvs) > 0 {
		ps.h3Srv = ps.h3Srvs[0]
		ps.h3Conn = ps.h3Conns[0]
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP)

	go func() {
		for range sigChan {
			if err := ps.reload(); err != nil {
				klog.Errorf("reload failed: %v", err)
			}
		}
	}()

	for _, sp := range specs {
		if sp.SSL {
			klog.Infof("listening on %s (TLS)", sp.Addr)
		} else if !sp.QUIC {
			klog.Infof("listening on %s", sp.Addr)
		}
	}

	// Start every serve loop. ServeTLS negotiates HTTP/2 via ALPN; a
	// plaintext listener gets h2c natively via http.Server.Protocols.
	serveErr := make(chan error, len(ps.srvs)+len(ps.h3Conns))
	for i := range ps.srvs {
		go func(i int) {
			srv, ln := ps.srvs[i], ps.lns[i]
			if srv.TLSConfig != nil {
				serveErr <- srv.ServeTLS(ln, "", "")
			} else {
				serveErr <- srv.Serve(ln)
			}
		}(i)
	}
	for i := range ps.h3Srvs {
		go func(i int) {
			serveErr <- h3Serve(ps.h3Srvs[i], ps.h3Conns[i])
		}(i)
	}

	// Block until every serve loop exits. A graceful shutdown makes each
	// return http.ErrServerClosed; a real error stops the rest immediately.
	total := len(ps.srvs) + len(ps.h3Conns)
	var firstErr error
	received := 0
	for received < total {
		err := <-serveErr
		received++
		if err != nil && !errors.Is(err, http.ErrServerClosed) && firstErr == nil {
			firstErr = err
			break
		}
	}
	if firstErr != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if serr := ps.shutdown(shutdownCtx); serr != nil {
			klog.Warningf("cleanup after serve error: %v", serr)
		}
		// Drain the remaining serve loops now that the listeners are down.
		for ; received < total; received++ {
			<-serveErr
		}
		return firstErr
	}
	return nil
}

// startQuicListener binds a UDP socket and registers an HTTP/3 (QUIC)
// server for the given address. It is a no-op for an empty address. The
// serve loop itself is started by the caller (start or startHTTP3).
func (ps *proxyServer) startQuicListener(addr string, tlsConfig *tls.Config) error {
	if addr == "" {
		return nil
	}
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return fmt.Errorf("listen http3 on %s: %w", addr, err)
	}
	ps.h3Conns = append(ps.h3Conns, conn)
	ps.h3Srvs = append(ps.h3Srvs, &http3.Server{
		Addr:      addr,
		Handler:   ps.frontendHandler(),
		TLSConfig: tlsConfig.Clone(),
	})
	klog.Infof("HTTP/3 (QUIC) server listening on %s", addr)
	return nil
}

// startHTTP3 is no longer used — HTTP/3 is configured via listeners array with 'quic' flag.
func (ps *proxyServer) startHTTP3(tlsConfig *tls.Config) error {
	return nil
}

var h3Serve = func(srv *http3.Server, conn net.PacketConn) error { return srv.Serve(conn) }

var serverShutdown = func(ctx context.Context, srv *http.Server) error { return srv.Shutdown(ctx) }

func (ps *proxyServer) shutdown(ctx context.Context) error {
	signal.Reset(syscall.SIGHUP)

	// Stop all health checkers.
	for name, hc := range ps.healthCheckers {
		hc.Stop()
		klog.Infof("stopped health checker for target %s", name)
	}

	var closeErrs []error
	for _, h := range ps.quicServers() {
		if err := h3Shutdown(ctx, h); err != nil {
			closeErrs = append(closeErrs, err)
		}
	}
	for _, c := range ps.packetConns() {
		if err := c.Close(); err != nil {
			closeErrs = append(closeErrs, err)
		}
	}
	if len(ps.tcpServers()) > 0 {
		for _, s := range ps.tcpServers() {
			if err := serverShutdown(ctx, s); err != nil {
				closeErrs = append(closeErrs, err)
			}
		}
		if len(closeErrs) > 0 {
			return errors.Join(closeErrs...)
		}
		return nil
	}
	if ps.ln != nil {
		return ps.ln.Close()
	}
	if len(closeErrs) > 0 {
		return errors.Join(closeErrs...)
	}
	return nil
}

var h3Shutdown = func(ctx context.Context, srv *http3.Server) error { return srv.Shutdown(ctx) }

// buildWriteSem returns the write-semaphore channel that gates concurrent
// cache writes. A worker count below zero means unlimited (nil channel,
// handled by the handler), zero uses the built-in default, and any positive
// value sizes the buffered channel.
func buildWriteSem(workers int) chan struct{} {
	if workers < 0 {
		return nil
	}
	if workers == 0 {
		workers = maxWriteWorkers
	}
	return make(chan struct{}, workers)
}

// run wires up the proxy from cfgPath (proxy settings) and targetsDir
// (per-target .yaml files) and blocks until the server is shut down.
// It returns nil on graceful shutdown or an error during startup
// (config load, cache init, plugin init, listen, or server start).
func run(ctx context.Context, cfgPath, targetsDir string) error {
	proxyCfg, err := config.LoadProxy(cfgPath)
	if err != nil {
		return fmt.Errorf("failed to load proxy config: %w", err)
	}

	targets, err := config.LoadTargets(targetsDir)
	if err != nil {
		return fmt.Errorf("failed to load targets: %w", err)
	}

	maxCacheSize, _ := proxyCfg.ParseMaxCacheSize()
	maxCacheAge, _ := time.ParseDuration(proxyCfg.MaxCacheAge)

	diskCache, err := disk.New(proxyCfg.CacheDir, maxCacheSize, maxCacheAge)
	if err != nil {
		return fmt.Errorf("failed to init cache: %w", err)
	}
	defer diskCache.Close()

	// Load and initialize plugins
	pluginConfigs := buildPluginConfigs(proxyCfg, targets)
	plugins, err := registry.LoadAndInitializePlugins(context.Background(), pluginConfigs)
	if err != nil {
		return fmt.Errorf("failed to initialize plugins: %w", err)
	}
	defer func() {
		_ = registry.StopAllPlugins(context.Background(), plugins)
	}()

	pluginMgr := manager.NewPluginManager()
	for _, p := range plugins {
		pluginMgr.RegisterPlugin(p)
		klog.Infof("loaded plugin: %s", p.Name())
	}

	maxBodySize := int64(0)
	if proxyCfg.MaxResponseBodySize != "" {
		if v, err := proxyCfg.ParseMaxResponseBodySize(); err == nil {
			maxBodySize = v
		}
	}

	writeSem := buildWriteSem(proxyCfg.MaxWriteWorkers)

	ps := newProxyServer(proxyCfg, targets, diskCache, writeSem, pluginMgr, maxBodySize)
	ps.cfgPath = cfgPath
	ps.targetsDir = targetsDir

	shutdownDone := make(chan struct{})
	runEnd := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-ctx.Done():
			klog.Info("shutting down...")
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer shutdownCancel()
			if err := ps.shutdown(shutdownCtx); err != nil {
				klog.Errorf("shutdown error: %v", err)
			}
		case <-runEnd:
			// run() is returning on its own (startup error); nothing to shut down.
		}
	}()

	if err := ps.start(); err != nil && err != http.ErrServerClosed {
		close(runEnd)
		<-shutdownDone
		return fmt.Errorf("server error: %w", err)
	}

	// Wait for the shutdown goroutine to finish before returning so callers
	// (and tests) observe a clean lifecycle.
	close(runEnd)
	<-shutdownDone
	return nil
}

func buildPluginConfigs(proxyCfg *config.ProxyConfig, targets []config.TargetConfig) map[string]map[string]any {
	configs := make(map[string]map[string]any)

	// Prometheus exporter
	if proxyCfg.MetricsAddr != "" {
		configs["prometheus_exporter"] = map[string]any{
			"enabled": true,
			"listen":  proxyCfg.MetricsAddr,
		}
	} else {
		// Default disabled
		configs["prometheus_exporter"] = map[string]any{
			"enabled": false,
		}
	}

	// JSON log plugin (nginx-style access + error logs)
	if proxyCfg.JSONLog != nil {
		configs["jsonlog"] = map[string]any{
			"enabled":    proxyCfg.JSONLog.Enabled,
			"access_log": proxyCfg.JSONLog.AccessLog,
			"error_log":  proxyCfg.JSONLog.ErrorLog,
			"stdout":     proxyCfg.JSONLog.Stdout,
		}
	}

	// Error page plugin (branded HTML error pages)
	if proxyCfg.ErrorPage != nil {
		m := map[string]any{"enabled": proxyCfg.ErrorPage.Enabled}
		if len(proxyCfg.ErrorPage.Statuses) > 0 {
			m["statuses"] = proxyCfg.ErrorPage.Statuses
		}
		configs["error_page"] = m
	}

	// WAF plugin: global GeoLite directory plus per-location policies
	// aggregated in one config, so BeforeProxy can pick the policy by
	// target|location instead of the last-wins pattern other plugins use.
	wafLocations := buildWAFLocations(targets)
	if len(wafLocations) > 0 {
		enabled := true
		if proxyCfg.WAF != nil {
			enabled = proxyCfg.WAF.Enabled
		}
		geodir := ""
		if proxyCfg.WAF != nil {
			geodir = proxyCfg.WAF.GeoLiteDir
		}
		configs["waf"] = map[string]any{
			"enabled":     enabled,
			"geolite_dir": geodir,
			"locations":   wafLocations,
		}
	} else {
		configs["waf"] = map[string]any{"enabled": false}
	}

	// Collect plugin configs from all locations across all targets
	for _, target := range targets {
		klog.V(2).Infof("buildPluginConfigs: target=%s, locations=%d", target.Name, len(target.Locations))
		for _, loc := range target.Locations {
			klog.V(2).Infof("buildPluginConfigs: loc=%s, Compression=%v, Optimize=%v, Rewrite=%v, CORS=%v, Headers=%v",
				loc.Path, loc.Compression, loc.Optimize, loc.Rewrite, loc.CORS, loc.Headers)
			// Compression
			if loc.Compression != nil {
				klog.V(2).Infof("buildPluginConfigs: loc.Compression type=%T, enabled=%v, types=%v", loc.Compression, loc.Compression.Enabled, loc.Compression.Types)
				configs["compression"] = structToMap(loc.Compression)
			}
			// Optimizer
			if loc.Optimize != nil {
				configs["optimizer"] = structToMap(loc.Optimize)
			}
			// Rewrite
			if loc.Rewrite != nil {
				configs["rewrite"] = structToMap(loc.Rewrite)
			}
			// CORS
			if loc.CORS != nil {
				configs["cors"] = structToMap(loc.CORS)
			}
			// Headers
			if loc.Headers != nil {
				configs["headers"] = structToMap(loc.Headers)
			}
		}
	}

	return configs
}

func structToMap(v any) map[string]any {
	switch v := v.(type) {
	case *config.CompressionConfig:
		m := make(map[string]any)
		if v.Enabled {
			m["enabled"] = true
			m["level"] = v.Level
			m["min_length"] = v.MinLength
			m["types"] = v.Types
		} else {
			m["enabled"] = false
		}
		return m
	case *config.OptimizeConfig:
		m := make(map[string]any)
		if v.Enabled {
			m["enabled"] = true
			m["minify_css"] = v.MinifyCSS
			m["minify_js"] = v.MinifyJS
			m["uglify_js"] = v.UglifyJS
			if v.Images != nil {
				m["images"] = map[string]any{
					"enabled":        v.Images.Enabled,
					"max_width":      v.Images.MaxWidth,
					"max_height":     v.Images.MaxHeight,
					"quality":        v.Images.Quality,
					"format":         v.Images.Format,
					"strip_metadata": v.Images.StripMetadata,
					"progressive":    v.Images.Progressive,
				}
			}
		} else {
			m["enabled"] = false
		}
		return m
	case *config.RewriteConfig:
		m := make(map[string]any)
		if v.Pattern != "" {
			m["enabled"] = true
			m["rules"] = []map[string]any{
				{
					"pattern":     v.Pattern,
					"replacement": v.Replacement,
					"break":       v.Break,
					"redirect":    v.Redirect,
				},
			}
		}
		return m
	case *config.CORSConfig:
		m := make(map[string]any)
		if v.Enabled {
			m["enabled"] = true
			m["allow_origins"] = v.AllowOrigins
			m["allow_methods"] = v.AllowMethods
			m["allow_headers"] = v.AllowHeaders
			m["expose_headers"] = v.ExposeHeaders
			m["allow_credentials"] = v.AllowCredentials
			m["max_age"] = v.MaxAge
		} else {
			m["enabled"] = false
		}
		return m
	case *config.HeadersConfig:
		m := make(map[string]any)
		if v.RequestAdd != nil {
			m["enabled"] = true
			m["request_add"] = v.RequestAdd
		}
		if len(v.RequestRemove) > 0 {
			m["enabled"] = true
			m["request_remove"] = v.RequestRemove
		}
		if v.ResponseAdd != nil {
			m["enabled"] = true
			m["response_add"] = v.ResponseAdd
		}
		if len(v.ResponseRemove) > 0 {
			m["enabled"] = true
			m["response_remove"] = v.ResponseRemove
		}
		return m
	default:
		return nil
	}
}

// buildWAFLocations collects every location that declares a WAF policy as an
// entry keyed by target + location path. The plugin uses target|location to
// select the policy at request time.
func buildWAFLocations(targets []config.TargetConfig) []any {
	var out []any
	for _, target := range targets {
		for _, loc := range target.Locations {
			if loc.WAF == nil {
				continue
			}
			out = append(out, map[string]any{
				"target":   target.Name,
				"location": loc.Path,
				"waf":      wafLocationToMap(loc.WAF),
			})
		}
	}
	return out
}

// wafLocationToMap converts the typed per-location WAF policy into the map
// form the plugin consumes. Rules keep their DNF ([][]WAFCondition) shape.
func wafLocationToMap(v *config.WAFLocationConfig) map[string]any {
	m := map[string]any{"enabled": v.Enabled}

	rules := make([]any, 0, len(v.Rules))
	for _, r := range v.Rules {
		rm := map[string]any{"id": r.ID}
		if r.Name != "" {
			rm["name"] = r.Name
		}
		if r.Enabled != nil {
			rm["enabled"] = *r.Enabled
		}
		if r.Action.Type != "" || r.Action.Code != 0 || r.Action.Message != "" {
			am := map[string]any{}
			if r.Action.Type != "" {
				am["type"] = r.Action.Type
			}
			if r.Action.Code != 0 {
				am["code"] = r.Action.Code
			}
			if r.Action.Message != "" {
				am["message"] = r.Action.Message
			}
			rm["action"] = am
		}
		groups := make([]any, 0, len(r.Conditions))
		for _, grp := range r.Conditions {
			conds := make([]any, 0, len(grp))
			for _, c := range grp {
				cm := map[string]any{"param": c.Param, "operator": c.Operator, "value": c.Value}
				if c.ParamName != "" {
					cm["param_name"] = c.ParamName
				}
				conds = append(conds, cm)
			}
			groups = append(groups, conds)
		}
		rm["conditions"] = groups
		rules = append(rules, rm)
	}
	if len(rules) > 0 {
		m["rules"] = rules
	}
	return m
}
