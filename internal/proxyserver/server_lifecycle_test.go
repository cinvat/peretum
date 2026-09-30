package proxyserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cinvat/peretum/internal/plugin/manager"
	"github.com/cinvat/peretum/internal/router"
	"github.com/quic-go/quic-go/http3"
)

func TestReloadFrom(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, cfgPath, runConfigYAML(":9999", filepath.Join(dir, "cache"), false))
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		"server_name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n    cache: true\n")

	t.Run("ProxyConfigError", func(t *testing.T) {
		ps := &proxyServer{}
		if err := ps.reloadFrom(filepath.Join(dir, "missing.yaml"), targetsDir); err == nil ||
			!strings.Contains(err.Error(), "proxy config") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("TargetsError", func(t *testing.T) {
		ps := &proxyServer{}
		if err := ps.reloadFrom(cfgPath, filepath.Join(dir, "notargets")); err == nil ||
			!strings.Contains(err.Error(), "targets") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("Success", func(t *testing.T) {
		ps := &proxyServer{router: router.NewHostRouter()}
		if err := ps.reloadFrom(cfgPath, targetsDir); err != nil {
			t.Fatalf("reloadFrom: %v", err)
		}
		if len(ps.proxyCfg.Listeners) != 1 || ps.proxyCfg.Listeners[0] != ":9999" {
			t.Fatalf("proxyCfg.Listeners = %v", ps.proxyCfg.Listeners)
		}
		if len(ps.targets) != 1 || ps.targets[0].ServerName != "tg" {
			t.Fatalf("targets = %+v", ps.targets)
		}
		if got := ps.router.GetDefaultHandler(); got == nil {
			t.Fatal("expected default handler after reload")
		}
	})

	t.Run("PluginReopen", func(t *testing.T) {
		pm := manager.NewPluginManager()
		pm.RegisterPlugin(plainFakePlugin{})
		pm.RegisterPlugin(reopenOKFakePlugin{})
		pm.RegisterPlugin(reopenFailFakePlugin{})
		ps := &proxyServer{router: router.NewHostRouter(), pluginMgr: pm}
		if err := ps.reloadFrom(cfgPath, targetsDir); err != nil {
			t.Fatalf("reloadFrom: %v", err)
		}
	})

	t.Run("TLSReload", func(t *testing.T) {
		cert, key := genCertFiles(t, dir)
		tdir := filepath.Join(dir, "tls-targets")
		if err := os.Mkdir(tdir, 0o700); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(tdir, "tls.yaml"),
			fmt.Sprintf("server_name: tls\n\nupstreams:\n  - url: http://y:1\nlocations:\n  - path: /\ntls:\n  cert_file: %s\n  key_file: %s\n", cert, key))
		ps := &proxyServer{router: router.NewHostRouter(), srv: &http.Server{}, h3Srv: &http3.Server{}}
		if err := ps.reloadFrom(cfgPath, tdir); err != nil {
			t.Fatalf("reloadFrom: %v", err)
		}
		if ps.srv.TLSConfig == nil {
			t.Fatal("expected srv.TLSConfig set after TLS reload")
		}
		if ps.h3Srv.TLSConfig == nil {
			t.Fatal("expected h3Srv.TLSConfig set after TLS reload")
		}
	})
}

func TestReloadUsesStoredPaths(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "config.d")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, cfgPath, runConfigYAML(":9999", filepath.Join(dir, "cache"), false))
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		"server_name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n")

	ps := &proxyServer{router: router.NewHostRouter()}
	// Run from a working directory that has no config.yaml/config.d, so a
	// reload jumping back to relative defaults would fail to find the config.
	t.Chdir(t.TempDir())
	ps.cfgPath = cfgPath
	ps.targetsDir = targetsDir
	if err := ps.reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(ps.proxyCfg.Listeners) != 1 || ps.proxyCfg.Listeners[0] != ":9999" {
		t.Fatalf("proxyCfg.Listeners = %v", ps.proxyCfg.Listeners)
	}
	if len(ps.targets) != 1 || ps.targets[0].ServerName != "tg" {
		t.Fatalf("targets = %+v", ps.targets)
	}
}

func TestReloadDefaultsToRelativePaths(t *testing.T) {
	t.Chdir(t.TempDir()) // no config.yaml/config.d anywhere
	ps := &proxyServer{router: router.NewHostRouter()}
	if err := ps.reload(); err == nil || !strings.Contains(err.Error(), "proxy config") {
		t.Fatalf("reload() from empty dir should fail on the proxy config, err = %v", err)
	}
}

func TestShutdownNilServer(t *testing.T) {
	ps := &proxyServer{}
	if err := ps.shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestShutdownListenerOnly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ps := &proxyServer{ln: ln}
	if err := ps.shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if _, err := ln.Accept(); err == nil {
		t.Fatal("expected listener to be closed")
	}
}

func TestBuildWriteSem(t *testing.T) {
	if sem := buildWriteSem(-1); sem != nil {
		t.Fatalf("buildWriteSem(-1) = %v, want nil (unlimited)", sem)
	}
	if sem := buildWriteSem(0); sem == nil || cap(sem) != maxWriteWorkers {
		t.Fatalf("buildWriteSem(0) = %v, want default capacity", sem)
	}
	if sem := buildWriteSem(3); sem == nil || cap(sem) != 3 {
		t.Fatalf("buildWriteSem(3) = %v, want capacity 3", sem)
	}
}

func TestShutdownH3Errors(t *testing.T) {
	origServer := serverShutdown
	origH3 := h3Shutdown
	serverShutdown = func(context.Context, *http.Server) error { return fmt.Errorf("srv-fail") }
	h3Shutdown = func(context.Context, *http3.Server) error { return fmt.Errorf("h3-fail") }
	defer func() { serverShutdown = origServer; h3Shutdown = origH3 }()

	ps := &proxyServer{
		srv:    &http.Server{},
		h3Srv:  &http3.Server{},
		h3Conn: &closedPacketConn{},
	}
	err := ps.shutdown(context.Background())
	if err == nil || !strings.Contains(err.Error(), "h3-fail") || !strings.Contains(err.Error(), "srv-fail") {
		t.Fatalf("err = %v", err)
	}
}

// Start with TLS + http3_addr configured, then serve HTTP/1.1 over TLS and
// HTTP/3 over QUIC, and shut down cleanly.
func TestShutdownQuicOnlyErrors(t *testing.T) {
	orig := h3Shutdown
	h3Shutdown = func(context.Context, *http3.Server) error { return fmt.Errorf("h3x") }
	defer func() { h3Shutdown = orig }()

	ps := &proxyServer{h3Srv: &http3.Server{}, h3Conn: &closedPacketConn{}}
	err := ps.shutdown(context.Background())
	if err == nil || !strings.Contains(err.Error(), "h3x") {
		t.Fatalf("err = %v", err)
	}
}
