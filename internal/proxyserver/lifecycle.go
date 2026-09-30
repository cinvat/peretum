package proxyserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	disk "github.com/cinvat/peretum/internal/cache/disk"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/plugin/manager"
	"github.com/cinvat/peretum/plugins/registry"
	"github.com/quic-go/quic-go/http3"
	"k8s.io/klog/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

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

	startTime := time.Now()
	klog.Info("reloading configuration...")

	// Load global config
	proxyCfg, err := config.LoadProxy(cfgPath)
	if err != nil {
		return fmt.Errorf("proxy config: %w", err)
	}

	targets, err := config.LoadTargets(targetsDir)
	if err != nil {
		return fmt.Errorf("targets: %w", err)
	}

	// Diff against the previously loaded targets. This only makes the reload
	// log informative; the router is always rebuilt in full.
	changedTargets := changedTargetNames(ps.targets, targets)
	isDelta := len(changedTargets) > 0 && len(changedTargets) < len(targets)

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

	duration := time.Since(startTime)

	if isDelta {
		klog.Infof("delta reload completed in %v: %d targets changed", duration, len(changedTargets))
		for _, ct := range changedTargets {
			klog.Infof("  changed: %s", ct)
		}
	} else {
		klog.Infof("full reload completed in %v: %d targets", duration, len(targets))
		for _, t := range targets {
			klog.Infof("  target: %s (lb=%s)", t.ServerName, t.LBAlgorithm)
			for _, u := range t.Upstreams {
				klog.Infof("    upstream: %s (weight=%d)", u.URL, u.Weight)
			}
			for _, loc := range t.Locations {
				klog.Infof("    location: %s %s (cache=%v)", loc.MatchType, loc.Path, loc.Cache)
			}
			if t.TLS != nil && t.TLS.CertFile != "" {
				klog.Infof("    TLS: %s", t.TLS.CertFile)
			}
			if t.ServerName != "" {
				klog.Infof("    server_name: %s", t.ServerName)
			}
		}
	}

	return nil
}

// changedTargetNames returns the server names whose serialized config differs
// between prev and next, marking removed targets with a " (deleted)" suffix.
// It exists purely to make reload logs and metrics informative; the router is
// always rebuilt from scratch.
func (ps *proxyServer) waitForConfigReplay() error {
	if ps.configErr != nil {
		return fmt.Errorf("config event store unavailable: %w", ps.configErr)
	}
	if ps.configReady == nil {
		return nil
	}

	timeout := config.DefaultReplayTimeout
	if ps.proxyCfg != nil && ps.proxyCfg.Cluster != nil && ps.proxyCfg.Cluster.ReplayTimeout > 0 {
		timeout = ps.proxyCfg.Cluster.ReplayTimeout
	}

	begin := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ps.configReady:
		klog.Infof("config replay complete in %s; target store is current", time.Since(begin).Round(time.Millisecond))
		return nil
	case <-timer.C:
		return fmt.Errorf("config replay did not complete within %s; refusing to serve an incomplete target store (raise cluster.replay_timeout for a large stream)", timeout)
	}
}

func (ps *proxyServer) start() error {
	klog.Infof("start() called")

	// Provision the lazy target store (pull/seed on first launch) before the
	// router is built from it.
	if err := ps.ensureLazyStore(); err != nil {
		klog.Errorf("lazy target store init failed: %v", err)
	}

	// Do not build or serve routes until the retained config has been applied.
	if err := ps.waitForConfigReplay(); err != nil {
		return err
	}

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
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
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
var h3Serve = func(srv *http3.Server, conn net.PacketConn) error { return srv.Serve(conn) }

var serverShutdown = func(ctx context.Context, srv *http.Server) error { return srv.Shutdown(ctx) }

func (ps *proxyServer) shutdown(ctx context.Context) error {
	// Stop NATS config sync
	if ps.natsSync != nil {
		if err := ps.natsSync.Close(); err != nil {
			klog.Errorf("NATS sync close error: %v", err)
		}
		ps.natsSync = nil
	}

	// Close the lazy target store (flushes pending config writes to Pebble).
	if ps.targetStore != nil {
		if err := ps.targetStore.Close(); err != nil {
			klog.Errorf("target store close error: %v", err)
		}
		ps.targetStore = nil
	}

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
func Run(ctx context.Context, cfgPath, targetsDir string) error {
	proxyCfg, err := config.LoadProxy(cfgPath)
	if err != nil {
		return fmt.Errorf("failed to load proxy config: %w", err)
	}

	// A lazy edge configured with a NATS URI takes its state from the
	// JetStream replay instead of the local config.d directory.
	var targets []config.TargetConfig
	skipLocalTargets := proxyCfg != nil && proxyCfg.Cluster != nil && proxyCfg.Cluster.Lazy && proxyCfg.Cluster.NATSURI != ""
	if !skipLocalTargets {
		targets, err = config.LoadTargets(targetsDir)
		if err != nil {
			return fmt.Errorf("failed to load targets: %w", err)
		}
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
			// Run() is returning on its own (startup error); nothing to shut down.
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
