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
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/plugin/manager"
	"github.com/cinvat/peretum/internal/router"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http2"
)

func genCertFiles(t *testing.T, dir string) (string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	writeFile(t, certFile, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
	writeFile(t, keyFile, string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})))
	return certFile, keyFile
}

func mustLoadCert(t *testing.T, certFile, keyFile string) []tls.Certificate {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("load cert: %v", err)
	}
	return []tls.Certificate{cert}
}

type closedPacketConn struct{}

func (closedPacketConn) Close() error { return fmt.Errorf("conn-fail") }
func (closedPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	return 0, nil, fmt.Errorf("closed")
}
func (closedPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	return 0, fmt.Errorf("closed")
}
func (closedPacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
}
func (closedPacketConn) SetDeadline(t time.Time) error      { return nil }
func (closedPacketConn) SetReadDeadline(t time.Time) error  { return nil }
func (closedPacketConn) SetWriteDeadline(t time.Time) error { return nil }

func TestNewProxyServer(t *testing.T) {
	cfg := &config.ProxyConfig{Listen: ":8081"}
	tgt := []config.TargetConfig{{Name: "tg"}}
	dc := newDiskCache(t, t.TempDir())
	sem := make(chan struct{}, 8)
	pm := manager.NewPluginManager()
	ps := newProxyServer(cfg, tgt, dc, sem, pm, 0)
	if ps.router == nil || ps.diskCache != dc || ps.writeSem != sem || ps.proxyCfg != cfg ||
		len(ps.targets) != 1 || ps.pluginMgr != pm || ps.maxBodySize != 0 {
		t.Fatalf("newProxyServer did not wire fields: %+v", ps)
	}
}

func TestBuildHostRouter(t *testing.T) {
	targets := []config.TargetConfig{
		{
			Name:      "a",
			Upstreams: []config.UpstreamConfig{{URL: "http://a:1", Weight: 1}},
			Locations: []config.LocationConfig{
				{Path: "/", Cache: true},
				{Path: "/", Cache: true},
			},
		},
		{
			Name:      "b",
			Listen:    "b.example:8080",
			Upstreams: []config.UpstreamConfig{{URL: "http://b:2"}},
			Locations: []config.LocationConfig{{Path: "/api"}},
		},
		{
			Name:      "c",
			Listen:    "plainhost",
			Upstreams: []config.UpstreamConfig{{URL: "http://c:3"}},
			Locations: []config.LocationConfig{{Path: "/", Cache: true}},
		},
		{
			Name:      "d",
			Listen:    ":9090",
			Upstreams: []config.UpstreamConfig{{URL: "http://d:4"}},
			Locations: []config.LocationConfig{{Path: "/x"}},
		},
		{
			Name:      "bad",
			Upstreams: []config.UpstreamConfig{{URL: "not-a-url"}},
			Locations: []config.LocationConfig{{Path: "/"}},
		},
	}
	ps := &proxyServer{targets: targets}
	hr := ps.buildHostRouter()
	got := hr.GetTargets()
	for _, key := range []string{"a", "b.example", "plainhost", "d"} {
		if got[key] == nil {
			t.Fatalf("missing host key %q: %v", key, got)
		}
	}
	if got["bad"] != nil {
		t.Fatalf("expected invalid-upstream target skipped, got %v", got["bad"])
	}
	if hr.GetDefaultHandler() == nil {
		t.Fatal("expected default handler")
	}
}

func TestBuildTLSConfig(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCertFiles(t, dir)
	garbageCert := filepath.Join(dir, "garbage.pem")
	writeFile(t, garbageCert, "not a pem")

	bad := filepath.Join(dir, "badpair.pem")
	_ = garbageCert
	ps := &proxyServer{
		proxyCfg: &config.ProxyConfig{},
		targets: []config.TargetConfig{
			{Name: "notls"},
			{Name: "empty", TLS: &config.TargetTLSConfig{CertFile: "", KeyFile: ""}},
			{Name: "nokey", TLS: &config.TargetTLSConfig{CertFile: cert}},
			{Name: "badpair", TLS: &config.TargetTLSConfig{CertFile: bad, KeyFile: bad}},
			{Name: "good", TLS: &config.TargetTLSConfig{CertFile: cert, KeyFile: key}},
		},
	}
	tc := ps.buildTLSConfig()
	if tc == nil {
		t.Fatal("expected tls.Config with one valid cert")
	}
	if len(tc.Certificates) != 1 {
		t.Fatalf("certificates = %d, want 1", len(tc.Certificates))
	}
	if tc.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %d", tc.MinVersion)
	}
	got, err := tc.GetConfigForClient(nil)
	if err != nil || got == nil {
		t.Fatalf("GetConfigForClient = %v, %v", got, err)
	}
}

func TestBuildTLSConfigGlobal(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCertFiles(t, dir)
	bad := filepath.Join(dir, "bad.pem")
	writeFile(t, bad, "nope")

	t.Run("GlobalOnlyCertNoKey", func(t *testing.T) {
		ps := &proxyServer{proxyCfg: &config.ProxyConfig{TLSCertFile: cert}}
		tc := ps.buildTLSConfig()
		// TLS was requested (tls_cert_file set) but no readable key pair was
		// found, so a self-signed certificate covers the TLS frontend.
		if tc == nil {
			t.Fatal("expected self-signed tls.Config fallback")
		}
		if len(tc.Certificates) != 1 {
			t.Fatalf("certificates = %d, want 1 self-signed", len(tc.Certificates))
		}
	})

	t.Run("GlobalBadPair", func(t *testing.T) {
		ps := &proxyServer{proxyCfg: &config.ProxyConfig{TLSCertFile: bad, TLSKeyFile: bad}}
		tc := ps.buildTLSConfig()
		if tc == nil {
			t.Fatal("expected self-signed tls.Config fallback")
		}
		if len(tc.Certificates) != 1 {
			t.Fatalf("certificates = %d, want 1 self-signed", len(tc.Certificates))
		}
	})

	t.Run("SelfSignedGenerationFails", func(t *testing.T) {
		orig := ecdsaGenerateKey
		ecdsaGenerateKey = func(c elliptic.Curve, rand io.Reader) (*ecdsa.PrivateKey, error) {
			return nil, errors.New("gen boom")
		}
		defer func() { ecdsaGenerateKey = orig }()
		ps := &proxyServer{proxyCfg: &config.ProxyConfig{HTTP3Addr: ":8443"}}
		if tc := ps.buildTLSConfig(); tc != nil {
			t.Fatalf("expected nil tls.Config when self-signed generation fails, got %v", tc)
		}
	})

	t.Run("GlobalAndTargetCerts", func(t *testing.T) {
		ps := &proxyServer{
			proxyCfg: &config.ProxyConfig{TLSCertFile: cert, TLSKeyFile: key},
			targets: []config.TargetConfig{
				{Name: "good", TLS: &config.TargetTLSConfig{CertFile: cert, KeyFile: key}},
			},
		}
		tc := ps.buildTLSConfig()
		if tc == nil || len(tc.Certificates) != 2 {
			t.Fatalf("expected 2 certificates, got %v", tc)
		}
	})

	t.Run("PlaintextNoTLS", func(t *testing.T) {
		ps := &proxyServer{
			proxyCfg: &config.ProxyConfig{},
			targets: []config.TargetConfig{
				{Name: "a", TLS: nil},
			},
		}
		if tc := ps.buildTLSConfig(); tc != nil {
			t.Fatalf("expected nil tls.Config for plaintext, got %v", tc)
		}
	})
}

func TestTLSRequested(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCertFiles(t, dir)
	_ = cert
	_ = key

	cases := []struct {
		name    string
		proxy   *config.ProxyConfig
		targets []config.TargetConfig
		want    bool
	}{
		{"nilProxy", nil, nil, false},
		{"empty", &config.ProxyConfig{}, nil, false},
		{"http3Addr", &config.ProxyConfig{HTTP3Addr: ":8443"}, nil, true},
		{"globalCert", &config.ProxyConfig{TLSCertFile: "c.pem"}, nil, true},
		{"globalKey", &config.ProxyConfig{TLSKeyFile: "k.pem"}, nil, true},
		{"targetTLS", &config.ProxyConfig{}, []config.TargetConfig{{Name: "t", TLS: &config.TargetTLSConfig{CertFile: "c.pem"}}}, true},
		{"targetNoTLS", &config.ProxyConfig{}, []config.TargetConfig{{Name: "t"}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ps := &proxyServer{proxyCfg: c.proxy, targets: c.targets}
			if got := ps.tlsRequested(); got != c.want {
				t.Fatalf("tlsRequested() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestGenerateSelfSignedCert(t *testing.T) {
	cert, err := generateSelfSignedCert()
	if err != nil {
		t.Fatalf("generateSelfSignedCert: %v", err)
	}
	if len(cert.Certificate) != 1 {
		t.Fatalf("certificate blob count = %d, want 1", len(cert.Certificate))
	}

	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "self-signed-ok")
	}))
	srv.TLS = tlsCfg
	srv.StartTLS()
	defer srv.Close()

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET over self-signed TLS: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "self-signed-ok" {
		t.Fatalf("body = %q", body)
	}
}

func TestGenerateSelfSignedCertErrorPaths(t *testing.T) {
	t.Run("KeyGenFails", func(t *testing.T) {
		orig := ecdsaGenerateKey
		ecdsaGenerateKey = func(c elliptic.Curve, rand io.Reader) (*ecdsa.PrivateKey, error) {
			return nil, errors.New("keygen boom")
		}
		defer func() { ecdsaGenerateKey = orig }()
		if _, err := generateSelfSignedCert(); err == nil {
			t.Fatal("expected key generation error")
		}
	})

	t.Run("CreateCertFails", func(t *testing.T) {
		orig := createX509Certificate
		createX509Certificate = func(r io.Reader, template, parent *x509.Certificate, pub, priv any) ([]byte, error) {
			return nil, errors.New("create boom")
		}
		defer func() { createX509Certificate = orig }()
		if _, err := generateSelfSignedCert(); err == nil {
			t.Fatal("expected cert creation error")
		}
	})

	t.Run("MarshalKeyFails", func(t *testing.T) {
		orig := marshalECPrivateKey
		marshalECPrivateKey = func(key *ecdsa.PrivateKey) ([]byte, error) {
			return nil, errors.New("marshal boom")
		}
		defer func() { marshalECPrivateKey = orig }()
		if _, err := generateSelfSignedCert(); err == nil {
			t.Fatal("expected marshal error")
		}
	})
}

type plainFakePlugin struct{}

func (plainFakePlugin) Name() string                { return "plain" }
func (plainFakePlugin) Init(map[string]any) error   { return nil }
func (plainFakePlugin) Start(context.Context) error { return nil }
func (plainFakePlugin) Stop(context.Context) error  { return nil }

type reopenOKFakePlugin struct{ plainFakePlugin }

func (reopenOKFakePlugin) Name() string  { return "ok" }
func (reopenOKFakePlugin) Reopen() error { return nil }

type reopenFailFakePlugin struct{ plainFakePlugin }

func (reopenFailFakePlugin) Name() string  { return "fail" }
func (reopenFailFakePlugin) Reopen() error { return fmt.Errorf("boom") }

type routerWrapFakePlugin struct{ plainFakePlugin }

func (routerWrapFakePlugin) WrapRouter(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Router-Wrapped", "1")
		h.ServeHTTP(w, r)
	})
}

func TestFrontendHandler(t *testing.T) {
	// No plugin manager: the frontend is the bare router.
	ps := &proxyServer{router: router.NewHostRouter()}
	fh := ps.frontendHandler()
	if fh == nil {
		t.Fatal("frontendHandler returned nil")
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	fh.ServeHTTP(rec, req)
	if rec.Result().Header.Get("X-Router-Wrapped") != "" {
		t.Fatalf("no wrapper expected without plugins, got %v", rec.Result().Header)
	}

	// With a RouterWrapper plugin the returned handler intercepts requests.
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(plainFakePlugin{})
	pm.RegisterPlugin(routerWrapFakePlugin{})
	ps2 := &proxyServer{router: router.NewHostRouter(), pluginMgr: pm}
	fh2 := ps2.frontendHandler()
	rec2 := httptest.NewRecorder()
	fh2.ServeHTTP(rec2, req)
	if rec2.Result().Header.Get("X-Router-Wrapped") != "1" {
		t.Fatalf("expected wrapper to run, got %v", rec2.Result().Header)
	}
}

func TestFrontendHandlerSet(t *testing.T) {
	// When ps.frontend is already built, frontendHandler returns it as-is.
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	ps := &proxyServer{frontend: inner}
	fh := ps.frontendHandler()
	rec := httptest.NewRecorder()
	fh.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected pre-built frontend to be served, got status %d", rec.Code)
	}
}

func TestReloadFrom(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, cfgPath, runConfigYAML(":9999", filepath.Join(dir, "cache"), false))
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		"name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n    cache: true\n")

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
		if ps.proxyCfg.Listen != ":9999" {
			t.Fatalf("proxyCfg.Listen = %q", ps.proxyCfg.Listen)
		}
		if len(ps.targets) != 1 || ps.targets[0].Name != "tg" {
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
			fmt.Sprintf("name: tls\nlisten: :8443\nupstreams:\n  - url: http://y:1\nlocations:\n  - path: /\ntls:\n  cert_file: %s\n  key_file: %s\n", cert, key))
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
		"name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n")

	ps := &proxyServer{router: router.NewHostRouter()}
	// Run from a working directory that has no config.yaml/config.d, so a
	// reload jumping back to relative defaults would fail to find the config.
	t.Chdir(t.TempDir())
	ps.cfgPath = cfgPath
	ps.targetsDir = targetsDir
	if err := ps.reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if ps.proxyCfg.Listen != ":9999" {
		t.Fatalf("proxyCfg.Listen = %q", ps.proxyCfg.Listen)
	}
	if len(ps.targets) != 1 || ps.targets[0].Name != "tg" {
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

func TestStartDefaultListenError(t *testing.T) {
	ln, err := net.Listen("tcp", ":8081")
	if err != nil {
		t.Skipf("port 8081 in use, skipping default-listen test: %v", err)
	}
	defer ln.Close()
	ps := &proxyServer{proxyCfg: &config.ProxyConfig{}, targets: nil}
	if err := ps.start(); err == nil || !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("err = %v", err)
	}
}

func TestStartServeAndSIGHUPReloadError(t *testing.T) {
	t.Chdir(t.TempDir()) // no config.yaml/config.d: reload() must fail

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
	}))
	defer up.Close()

	port := freePort(t)
	dir := t.TempDir()
	dc := newDiskCache(t, dir)
	proxyCfg := &config.ProxyConfig{Listen: fmt.Sprintf("127.0.0.1:%d, :", port), CacheDir: dir}
	targets := []config.TargetConfig{
		{
			Name:      "tg",
			Listen:    "tls-echo:8443",
			Upstreams: []config.UpstreamConfig{{URL: up.URL}},
			TLS:       &config.TargetTLSConfig{CertFile: "dummy.pem"},
			Locations: []config.LocationConfig{{Path: "/", Cache: true}},
		},
	}
	ps := newProxyServer(proxyCfg, targets, dc, make(chan struct{}, 8), nil, 0)

	done := make(chan error, 1)
	go func() { done <- ps.start() }()

	var resp *http.Response
	var err error
	for i := 0; i < 100; i++ {
		resp, err = http.Get(fmt.Sprintf("http://127.0.0.1:%d/x", port))
		if err == nil && resp.StatusCode == 200 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil || resp == nil {
		ps.shutdown(context.Background())
		t.Fatalf("server never came up: %v", err)
	}
	resp.Body.Close()

	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatalf("kill SIGHUP: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ps.shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	select {
	case err := <-done:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("start returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("start() never returned")
	}
}

func TestRunProxyConfigError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := run(ctx, "/definitely/missing/config.yaml", "")
	if err == nil || !strings.Contains(err.Error(), "proxy config") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunTargetsError(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	writeFile(t, cfgPath, runConfigYAML(":0", filepath.Join(dir, "cache"), false))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := run(ctx, cfgPath, filepath.Join(dir, "missing-targets"))
	if err == nil || !strings.Contains(err.Error(), "targets") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunCacheError(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		"name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n")
	writeFile(t, cfgPath, runConfigYAML(":0", "/dev/null/cache", false))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := run(ctx, cfgPath, targetsDir)
	if err == nil || !strings.Contains(err.Error(), "cache") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		"name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n")
	writeFile(t, cfgPath, runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), false))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = run(ctx, cfgPath, targetsDir)
	if err == nil || !strings.Contains(err.Error(), "server error") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunPluginInitError(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		"name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n")
	var b strings.Builder
	b.WriteString(runConfigYAML(":0", filepath.Join(dir, "cache"), false))
	fmt.Fprintf(&b, "json_log:\n  enabled: true\n  access_log: \"/dev/null/a.jsonl\"\n  error_log: \"/dev/null/e.jsonl\"\n  stdout: false\n")
	writeFile(t, cfgPath, b.String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := run(ctx, cfgPath, targetsDir)
	if err == nil || !strings.Contains(err.Error(), "initialize plugins") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunShutdownError(t *testing.T) {
	orig := serverShutdown
	serverShutdown = func(ctx context.Context, srv *http.Server) error {
		_ = srv.Shutdown(ctx)
		return fmt.Errorf("oops")
	}
	defer func() { serverShutdown = orig }()

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hi")
	}))
	defer up.Close()

	port := freePort(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		fmt.Sprintf("name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n", up.URL))
	writeFile(t, cfgPath, runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), false))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfgPath, targetsDir) }()

	resp := waitForServer(t, fmt.Sprintf("http://127.0.0.1:%d/x", port))
	resp.Body.Close()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() never returned after cancel")
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

func TestRunConfiguredWorkersAndBodyLimit(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/small":
			fmt.Fprint(w, "hello world")
		case "/big":
			fmt.Fprint(w, strings.Repeat("x", 1024))
		default:
			fmt.Fprint(w, "nope")
		}
	}))
	defer up.Close()

	port := freePort(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		fmt.Sprintf("name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n    cache: true\n", up.URL))
	cfg := runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), false)
	cfg += "max_write_workers: -1\nmax_response_body_size: \"50B\"\n"
	writeFile(t, cfgPath, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfgPath, targetsDir) }()

	// A response larger than max_response_body_size must be rejected.
	big := waitForServer(t, fmt.Sprintf("http://127.0.0.1:%d/big", port))
	big.Body.Close()
	if big.StatusCode != http.StatusBadGateway {
		t.Fatalf("big status = %d, want 502", big.StatusCode)
	}

	// Unlimited workers (-1 => nil writeSem) still allow caching: a miss
	// writes through and the second identical request is served from cache.
	small := waitForServer(t, fmt.Sprintf("http://127.0.0.1:%d/small", port))
	small.Body.Close()
	if small.StatusCode != http.StatusOK {
		t.Fatalf("small status = %d", small.StatusCode)
	}
	small2 := waitForServer(t, fmt.Sprintf("http://127.0.0.1:%d/small", port))
	defer small2.Body.Close()
	if small2.StatusCode != http.StatusOK || small2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("small2 status=%d x-cache=%q, want HIT", small2.StatusCode, small2.Header.Get("X-Cache"))
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() never returned after cancel")
	}
}

func TestRunUnlimitedBodyAndWorkers(t *testing.T) {
	body := strings.Repeat("y", 4096)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
	defer up.Close()

	port := freePort(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		fmt.Sprintf("name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n    cache: true\n", up.URL))
	cfg := runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), false)
	cfg += "max_write_workers: -1\nmax_response_body_size: \"-1\"\n"
	writeFile(t, cfgPath, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfgPath, targetsDir) }()

	resp := waitForServer(t, fmt.Sprintf("http://127.0.0.1:%d/stream", port))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(got) != body {
		t.Fatalf("body length = %d, want %d", len(got), len(body))
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() never returned after cancel")
	}
}

func TestRunServeAndGracefulShutdown(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello world")
	}))
	defer up.Close()

	port := freePort(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		fmt.Sprintf("name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n    cache: true\n", up.URL))
	writeFile(t, cfgPath, runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), true))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfgPath, targetsDir) }()

	resp := waitForServer(t, fmt.Sprintf("http://127.0.0.1:%d/cacheme", port))
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() never returned after cancel")
	}
}

func TestRunServeTLS(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "secure hello")
	}))
	defer up.Close()

	port := freePort(t)
	dir := t.TempDir()
	cert, key := genCertFiles(t, dir)
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		fmt.Sprintf("name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n", up.URL))
	cfg := runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), false)
	cfg += fmt.Sprintf("tls_cert_file: %s\ntls_key_file: %s\n", cert, key)
	writeFile(t, cfgPath, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfgPath, targetsDir) }()

	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	var resp *http.Response
	var err error
	for i := 0; i < 100; i++ {
		resp, err = client.Get(fmt.Sprintf("https://127.0.0.1:%d/x", port))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		cancel()
		t.Fatalf("https get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		resp.Body.Close()
		cancel()
		t.Fatalf("status = %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() never returned after cancel")
	}
}

func TestRunServeH2C(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "h2c hello")
	}))
	defer up.Close()

	port := freePort(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	targetsDir := filepath.Join(dir, "targets")
	if err := os.Mkdir(targetsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(targetsDir, "t.yaml"),
		fmt.Sprintf("name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n", up.URL))
	writeFile(t, cfgPath, runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), false))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfgPath, targetsDir) }()

	// HTTP/2 prior-knowledge client over plaintext (h2c), gRPC-style.
	client := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		},
	}
	var resp *http.Response
	var err error
	for i := 0; i < 100; i++ {
		resp, err = client.Get(fmt.Sprintf("http://127.0.0.1:%d/x", port))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		cancel()
		t.Fatalf("h2c get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "h2c hello" {
		cancel()
		t.Fatalf("status=%d body=%q", resp.StatusCode, string(body))
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() never returned after cancel")
	}
}

func TestBuildPluginConfigs(t *testing.T) {
	proxyCfg := &config.ProxyConfig{
		MetricsAddr: ":9090",
		JSONLog:     &config.JSONLogConfig{Enabled: true, AccessLog: "/a", ErrorLog: "/e", Stdout: true},
		ErrorPage:   &config.ErrorPageConfig{Enabled: true, Statuses: []int{404, 502}},
	}
	targets := []config.TargetConfig{
		{
			Name: "lean",
			Locations: []config.LocationConfig{
				{
					Path:        "/l",
					Compression: &config.CompressionConfig{Enabled: false},
					Optimize:    &config.OptimizeConfig{Enabled: true},
					Rewrite:     &config.RewriteConfig{},
					CORS:        &config.CORSConfig{Enabled: false},
					Headers:     &config.HeadersConfig{},
				},
				{Path: "/d", Optimize: &config.OptimizeConfig{Enabled: false}},
			},
		},
		{
			Name: "full",
			Locations: []config.LocationConfig{
				{
					Path:        "/f",
					Compression: &config.CompressionConfig{Enabled: true, Level: 5, MinLength: 100, Types: []string{"text/html"}},
					Optimize: &config.OptimizeConfig{
						Enabled: true, MinifyCSS: true, MinifyJS: true, UglifyJS: true,
						Images: &config.ImageOptimizeConfig{Enabled: true, MaxWidth: 800},
					},
					Rewrite: &config.RewriteConfig{Pattern: "^/x", Replacement: "/y", Break: true, Redirect: "permanent"},
					CORS:    &config.CORSConfig{Enabled: true, AllowOrigins: []string{"*"}},
					Headers: &config.HeadersConfig{
						RequestAdd:     map[string]string{"A": "b"},
						RequestRemove:  []string{"RA"},
						ResponseAdd:    map[string]string{"C": "d"},
						ResponseRemove: []string{"RC"},
					},
				},
			},
		},
	}

	configs := buildPluginConfigs(proxyCfg, targets)

	if v, ok := configs["prometheus_exporter"]; !ok || v["enabled"] != true || v["listen"] != ":9090" {
		t.Fatalf("prometheus config = %v", configs["prometheus_exporter"])
	}
	if v, ok := configs["jsonlog"]; !ok || v["enabled"] != true || v["access_log"] != "/a" || v["error_log"] != "/e" || v["stdout"] != true {
		t.Fatalf("jsonlog config = %v", configs["jsonlog"])
	}

	if v, ok := configs["compression"]; !ok || v["enabled"] != true || v["level"] != 5 || v["min_length"] != 100 {
		t.Fatalf("compression config = %v", configs["compression"])
	}
	if v, ok := configs["optimizer"]; !ok || v["enabled"] != true || v["minify_css"] != true || v["uglify_js"] != true {
		t.Fatalf("optimizer config = %v", configs["optimizer"])
	}
	im, ok := configs["optimizer"]["images"].(map[string]any)
	if !ok || im["max_width"] != 800 {
		t.Fatalf("optimizer images = %v", configs["optimizer"]["images"])
	}
	if v, ok := configs["rewrite"]; !ok || v["enabled"] != true {
		t.Fatalf("rewrite config = %v", configs["rewrite"])
	} else {
		rules, ok := v["rules"].([]map[string]any)
		if !ok || len(rules) != 1 || rules[0]["break"] != true || rules[0]["redirect"] != "permanent" ||
			rules[0]["pattern"] != "^/x" || rules[0]["replacement"] != "/y" {
			t.Fatalf("rewrite rules = %v", v["rules"])
		}
	}
	if v, ok := configs["cors"]; !ok || v["enabled"] != true || len(v["allow_origins"].([]string)) != 1 {
		t.Fatalf("cors config = %v", configs["cors"])
	}
	if v, ok := configs["headers"]; !ok || v["enabled"] != true ||
		v["request_add"] == nil || len(v["request_remove"].([]string)) != 1 ||
		v["response_add"] == nil || len(v["response_remove"].([]string)) != 1 {
		t.Fatalf("headers config = %v", configs["headers"])
	}

	if v, ok := configs["error_page"]; !ok || v["enabled"] != true {
		t.Fatalf("error_page config = %v", configs["error_page"])
	} else {
		st, ok := v["statuses"].([]int)
		if !ok || len(st) != 2 || st[0] != 404 || st[1] != 502 {
			t.Fatalf("error_page statuses = %v", v["statuses"])
		}
	}
}

func TestBuildPluginConfigsDisabledPrometheus(t *testing.T) {
	proxyCfg := &config.ProxyConfig{}
	targets := []config.TargetConfig{{Name: "x", Locations: []config.LocationConfig{{Path: "/"}}}}
	configs := buildPluginConfigs(proxyCfg, targets)
	if v, ok := configs["prometheus_exporter"]; !ok || v["enabled"] != false {
		t.Fatalf("prometheus disabled config = %v", configs["prometheus_exporter"])
	}
	if _, ok := configs["jsonlog"]; ok {
		t.Fatalf("jsonlog should be absent, got %v", configs["jsonlog"])
	}
	if _, ok := configs["error_page"]; ok {
		t.Fatalf("error_page should be absent, got %v", configs["error_page"])
	}
}

func TestBuildPluginConfigsErrorPageDefaults(t *testing.T) {
	proxyCfg := &config.ProxyConfig{ErrorPage: &config.ErrorPageConfig{Enabled: false}}
	targets := []config.TargetConfig{}
	configs := buildPluginConfigs(proxyCfg, targets)
	v, ok := configs["error_page"]
	if !ok {
		t.Fatalf("error_page config missing: %v", configs)
	}
	if v["enabled"] != false {
		t.Fatalf("error_page enabled = %v, want false", v["enabled"])
	}
	if _, hasStatuses := v["statuses"]; hasStatuses {
		t.Fatalf("error_page with no statuses must not set key, got %v", v)
	}
}

func TestBuildPluginConfigsWAF(t *testing.T) {
	enabled := true
	proxyCfg := &config.ProxyConfig{
		WAF: &config.WAFConfig{Enabled: true, GeoLiteDir: "/var/lib/geolite"},
	}
	targets := []config.TargetConfig{
		{
			Name: "full",
			Locations: []config.LocationConfig{
				{
					Path: "/f",
					WAF: &config.WAFLocationConfig{
						Enabled: true,
						Rules: []config.WAFRule{
							{
								ID: "r1", Name: "name1", Enabled: &enabled,
								Action: config.WAFRuleAction{Type: "deny", Code: 451, Message: "msg1"},
								Conditions: [][]config.WAFCondition{
									{{Param: "header", ParamName: "Content-Length", Operator: "gt", Value: "100"}},
									{{Param: "user_agent", Operator: "contains", Value: "safari"}},
								},
							},
							{ID: "r2"},
						},
					},
				},
				{Path: "/plain"}, // no WAF: must not appear in locations
			},
		},
		{
			Name: "lean",
			Locations: []config.LocationConfig{
				{Path: "/l", WAF: &config.WAFLocationConfig{Enabled: false}},
			},
		},
	}

	configs := buildPluginConfigs(proxyCfg, targets)
	v, ok := configs["waf"]
	if !ok {
		t.Fatalf("waf config missing: %v", configs)
	}
	if v["enabled"] != true || v["geolite_dir"] != "/var/lib/geolite" {
		t.Fatalf("waf global = %v", v)
	}

	locs, ok := v["locations"].([]any)
	if !ok || len(locs) != 2 {
		t.Fatalf("waf locations = %v", v["locations"])
	}

	first := locs[0].(map[string]any)
	if first["target"] != "full" || first["location"] != "/f" {
		t.Fatalf("first location = %v", first)
	}
	wm := first["waf"].(map[string]any)
	if wm["enabled"] != true {
		t.Fatalf("waf location = %v", wm)
	}
	rules, ok := wm["rules"].([]any)
	if !ok || len(rules) != 2 {
		t.Fatalf("waf rules = %v", wm["rules"])
	}
	r1, _ := rules[0].(map[string]any)
	if r1["id"] != "r1" || r1["name"] != "name1" || r1["enabled"] != true {
		t.Fatalf("r1 = %v", r1)
	}
	am, _ := r1["action"].(map[string]any)
	if am["type"] != "deny" || am["code"] != 451 || am["message"] != "msg1" {
		t.Fatalf("r1 action = %v", am)
	}
	cg, ok := r1["conditions"].([]any)
	if !ok || len(cg) != 2 {
		t.Fatalf("r1 groups = %v", r1["conditions"])
	}
	cond0, _ := cg[0].([]any)
	cm0, _ := cond0[0].(map[string]any)
	if cm0["param"] != "header" || cm0["param_name"] != "Content-Length" || cm0["operator"] != "gt" || cm0["value"] != "100" {
		t.Fatalf("r1 cond = %v", cm0)
	}
	cond1, _ := cg[1].([]any)
	cm1, _ := cond1[0].(map[string]any)
	if _, has := cm1["param_name"]; has {
		t.Fatalf("empty param_name must be omitted, got %v", cm1)
	}

	// Disabled location still gets a waf block (buildRuleSet skips it later).
	second := locs[1].(map[string]any)
	if second["target"] != "lean" || second["location"] != "/l" {
		t.Fatalf("second location = %v", second)
	}
	sm, _ := second["waf"].(map[string]any)
	if sm["enabled"] != false {
		t.Fatalf("disabled location waf = %v", sm)
	}
}

func TestBuildPluginConfigsWAFNoGlobal(t *testing.T) {
	// Global waf block absent but a location declares a policy: the plugin is
	// considered enabled with an empty geolite directory.
	targets := []config.TargetConfig{{
		Name: "x",
		Locations: []config.LocationConfig{
			{Path: "/", WAF: &config.WAFLocationConfig{Enabled: true, Rules: []config.WAFRule{}}},
		},
	}}
	configs := buildPluginConfigs(&config.ProxyConfig{}, targets)
	v, ok := configs["waf"]
	if !ok {
		t.Fatalf("waf config missing: %v", configs)
	}
	if v["enabled"] != true || v["geolite_dir"] != "" {
		t.Fatalf("waf no-global = %v", v)
	}
	if locs := v["locations"].([]any); len(locs) != 1 {
		t.Fatalf("waf locations = %v", v["locations"])
	}
}

func TestBuildPluginConfigsWAFNone(t *testing.T) {
	targets := []config.TargetConfig{{Name: "x", Locations: []config.LocationConfig{{Path: "/"}}}}
	configs := buildPluginConfigs(&config.ProxyConfig{}, targets)
	v, ok := configs["waf"]
	if !ok {
		t.Fatalf("waf config missing: %v", configs)
	}
	if v["enabled"] != false {
		t.Fatalf("waf none = %v", v)
	}
	if _, has := v["locations"]; has {
		t.Fatalf("no locations expected, got %v", v)
	}
}

func TestStructToMap(t *testing.T) {
	if m := structToMap(42); m != nil {
		t.Fatalf("expected nil for unsupported type, got %v", m)
	}
	if m := structToMap(&config.UpstreamConfig{URL: "http://x"}); m != nil {
		t.Fatalf("expected nil for pointer-to-unsupported type, got %v", m)
	}
	if m := structToMap(&config.OptimizeConfig{Enabled: false}); m == nil || m["enabled"] != false {
		t.Fatalf("optimize disabled = %v", m)
	}
	if m := structToMap(&config.RewriteConfig{}); m == nil {
		t.Fatal("rewrite empty map expected")
	} else if m["enabled"] != nil {
		t.Fatalf("rewrite should have no enabled key, got %v", m)
	}
	if m := structToMap(&config.HeadersConfig{}); m == nil {
		t.Fatalf("headers empty map expected, got nil")
	}
}

func TestStartHTTP3NoAddr(t *testing.T) {
	ps := &proxyServer{proxyCfg: &config.ProxyConfig{}, router: router.NewHostRouter()}
	if err := ps.startHTTP3(&tls.Config{}); err != nil {
		t.Fatalf("startHTTP3 with empty addr: %v", err)
	}
	if ps.h3Srv != nil || ps.h3Conn != nil {
		t.Fatalf("expected no h3 server, got %+v / %+v", ps.h3Srv, ps.h3Conn)
	}
}

func TestStartHTTP3ListenError(t *testing.T) {
	// Occupy a UDP port so the start fails to bind.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	port := pc.LocalAddr().(*net.UDPAddr).Port

	ps := &proxyServer{
		proxyCfg: &config.ProxyConfig{HTTP3Addr: fmt.Sprintf("127.0.0.1:%d", port)},
		router:   router.NewHostRouter(),
	}
	if err := ps.startHTTP3(&tls.Config{}); err == nil || !strings.Contains(err.Error(), "listen http3") {
		t.Fatalf("err = %v", err)
	}
}

func TestStartHTTP3ServeAndShutdown(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCertFiles(t, dir)
	tlsCfg := &tls.Config{Certificates: mustLoadCert(t, cert, key)}

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "http3 hello")
	}))
	defer up.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "front:%s", up.URL)
	})

	port := freeUDPPort(t)
	ps := &proxyServer{
		proxyCfg: &config.ProxyConfig{HTTP3Addr: fmt.Sprintf("127.0.0.1:%d", port)},
		router:   router.NewHostRouter(),
	}
	// Give the router a handler mapping for the "front" test so we can fetch.
	_ = mux

	if err := ps.startHTTP3(tlsCfg); err != nil {
		t.Fatalf("startHTTP3: %v", err)
	}
	defer ps.shutdown(context.Background())

	// Wait for the QUIC listener to accept. Poll with the http3 client.
	client := &http.Client{
		Transport: &http3.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	defer client.CloseIdleConnections()

	var got *http.Response
	var gerr error
	for i := 0; i < 50; i++ {
		got, gerr = client.Get(fmt.Sprintf("https://127.0.0.1:%d/admin", port))
		if gerr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if gerr != nil {
		t.Fatalf("http3 get: %v", gerr)
	}
	defer got.Body.Close()

	// With no routing targets, the built-in router returns 404 but still
	// serves over HTTP/3; proving the QUIC transport round-trips.
	if got.StatusCode == 0 {
		t.Fatalf("no status")
	}
	if got.ProtoMajor != 3 {
		t.Fatalf("Proto = %q, want HTTP/3", got.Proto)
	}
}

func TestStartHTTP3ServeError(t *testing.T) {
	called := make(chan struct{})
	orig := h3Serve
	h3Serve = func(*http3.Server, net.PacketConn) error {
		close(called)
		return fmt.Errorf("boom")
	}
	defer func() { h3Serve = orig }()

	dir := t.TempDir()
	cert, key := genCertFiles(t, dir)
	port := freeUDPPort(t)
	ps := &proxyServer{
		proxyCfg: &config.ProxyConfig{HTTP3Addr: fmt.Sprintf("127.0.0.1:%d", port)},
		router:   router.NewHostRouter(),
	}
	if err := ps.startHTTP3(&tls.Config{Certificates: mustLoadCert(t, cert, key)}); err != nil {
		t.Fatalf("startHTTP3: %v", err)
	}
	// Wait for the goroutine to have read the (now swapped) h3Serve variable
	// so the deferred restore cannot race with it.
	<-called
	if ps.h3Srv == nil || ps.h3Conn == nil {
		t.Fatalf("expected h3 server and conn to be set")
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
func TestStartServeTLSAndHTTP3(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCertFiles(t, dir)

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "upstream:%s", r.URL.Path)
	}))
	defer up.Close()

	port := freePort(t)
	h3port := freeUDPPort(t)
	targets := []config.TargetConfig{
		{
			Name:      "tg",
			Listen:    "127.0.0.1",
			Upstreams: []config.UpstreamConfig{{URL: up.URL}},
			Locations: []config.LocationConfig{{Path: "/", Cache: false}},
		},
	}
	ps := newProxyServer(&config.ProxyConfig{
		Listen:      fmt.Sprintf("127.0.0.1:%d", port),
		HTTP3Addr:   fmt.Sprintf("127.0.0.1:%d", h3port),
		TLSCertFile: cert,
		TLSKeyFile:  key,
	}, targets, newDiskCache(t, dir), make(chan struct{}, 8), nil, 0)

	done := make(chan error, 1)
	go func() { done <- ps.start() }()

	// HTTP/1.1 over TLS must work.
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	var resp *http.Response
	var err error
	for i := 0; i < 100; i++ {
		resp, err = client.Get(fmt.Sprintf("https://127.0.0.1:%d/hello", port))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		ps.shutdown(context.Background())
		t.Fatalf("https get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "upstream:/hello" {
		ps.shutdown(context.Background())
		t.Fatalf("status=%d body=%q", resp.StatusCode, string(body))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ps.shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	select {
	case err := <-done:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("start returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("start() never returned")
	}
}

// start() with TLS enabled but an unavailable HTTP/3 UDP port must fail.
func TestStartHTTP3BindErrorReturnsStartError(t *testing.T) {
	dir := t.TempDir()
	cert, key := genCertFiles(t, dir)

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	h3port := pc.LocalAddr().(*net.UDPAddr).Port

	ps := &proxyServer{
		proxyCfg: &config.ProxyConfig{
			Listen:      fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			HTTP3Addr:   fmt.Sprintf("127.0.0.1:%d", h3port),
			TLSCertFile: cert,
			TLSKeyFile:  key,
		},
	}
	if err := ps.start(); err == nil || !strings.Contains(err.Error(), "listen http3") {
		t.Fatalf("err = %v", err)
	}
}

func TestStartHTTP3SelfSigned(t *testing.T) {
	port := freePort(t)
	h3port := freeUDPPort(t)
	ps := &proxyServer{
		proxyCfg: &config.ProxyConfig{
			Listen:    fmt.Sprintf("127.0.0.1:%d", port),
			HTTP3Addr: fmt.Sprintf("127.0.0.1:%d", h3port),
		},
		targets: nil,
	}
	done := make(chan error, 1)
	go func() { done <- ps.start() }()

	// HTTPS over the self-signed certificate must work. With no routing
	// targets the built-in router still answers (404), proving the TLS
	// frontend (HTTP/1.1 and HTTP/2) round-trips.
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	var resp *http.Response
	var err error
	for i := 0; i < 100; i++ {
		resp, err = client.Get(fmt.Sprintf("https://127.0.0.1:%d/x", port))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		ps.shutdown(context.Background())
		t.Fatalf("https get: %v", err)
	}
	if resp.StatusCode == 0 {
		t.Fatalf("no status")
	}
	resp.Body.Close()

	// HTTP/3 (QUIC) must also be served via the self-signed certificate.
	h3t := &http3.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	h3client := &http.Client{Transport: h3t}
	var got *http.Response
	var gerr error
	for i := 0; i < 50; i++ {
		got, gerr = h3client.Get(fmt.Sprintf("https://127.0.0.1:%d/admin", h3port))
		if gerr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer got.Body.Close()
	if gerr != nil {
		ps.shutdown(context.Background())
		t.Fatalf("http3 get: %v", gerr)
	}
	if got.ProtoMajor != 3 {
		t.Fatalf("Proto = %q, want HTTP/3", got.Proto)
	}
	h3t.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ps.shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case err := <-done:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("start returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("start() never returned")
	}
}

func TestStartHTTP3DefaultAddr(t *testing.T) {
	// When TLS is active and http3_addr is not set, HTTP/3 is enabled by
	// default on the same port as the TLS frontend.
	dir := t.TempDir()
	cert, key := genCertFiles(t, dir)

	port := freePort(t)
	ps := &proxyServer{
		proxyCfg: &config.ProxyConfig{
			Listen:      fmt.Sprintf("127.0.0.1:%d", port),
			TLSCertFile: cert,
			TLSKeyFile:  key,
		},
		targets: nil,
	}
	if ps.proxyCfg.HTTP3Addr != "" {
		t.Fatal("expected empty http3_addr")
	}

	done := make(chan error, 1)
	go func() { done <- ps.start() }()

	h3t := &http3.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	h3client := &http.Client{Transport: h3t}
	var got *http.Response
	var gerr error
	for i := 0; i < 50; i++ {
		got, gerr = h3client.Get(fmt.Sprintf("https://127.0.0.1:%d/admin", port))
		if gerr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer got.Body.Close()
	if gerr != nil {
		ps.shutdown(context.Background())
		t.Fatalf("http3 get (default addr): %v", gerr)
	}
	if got.ProtoMajor != 3 {
		t.Fatalf("Proto = %q, want HTTP/3", got.Proto)
	}
	if ps.h3Srv == nil || ps.h3Srv.Addr != fmt.Sprintf("127.0.0.1:%d", port) {
		t.Errorf("default h3 addr = %v", ps.h3Srv)
	}
	h3t.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ps.shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case err := <-done:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("start returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("start() never returned")
	}
}

func TestListenerSpecs(t *testing.T) {
	t.Run("NilCfg", func(t *testing.T) {
		ps := &proxyServer{}
		specs, err := ps.listenerSpecs()
		if err != nil || len(specs) != 1 || specs[0].Addr != ":8081" || specs[0].SSL || specs[0].QUIC {
			t.Fatalf("specs = %v, err = %v", specs, err)
		}
	})
	t.Run("Explicit", func(t *testing.T) {
		ps := &proxyServer{proxyCfg: &config.ProxyConfig{
			Listeners: []string{":8080", ":443 ssl", ":443 ssl h3", ":8081 quic"},
		}}
		specs, err := ps.listenerSpecs()
		if err != nil {
			t.Fatal(err)
		}
		want := []config.ListenersSpec{
			{Addr: ":8080"},
			{Addr: ":443", SSL: true},
			{Addr: ":443", SSL: true, QUIC: true},
			{Addr: ":8081", QUIC: true},
		}
		if !reflect.DeepEqual(specs, want) {
			t.Fatalf("specs = %+v, want %+v", specs, want)
		}
	})
	t.Run("ExplicitError", func(t *testing.T) {
		ps := &proxyServer{proxyCfg: &config.ProxyConfig{Listeners: []string{":80 h2c ssl"}}}
		if _, err := ps.listenerSpecs(); err == nil || !strings.Contains(err.Error(), "conflict") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("LegacyDefault", func(t *testing.T) {
		ps := &proxyServer{proxyCfg: &config.ProxyConfig{}}
		specs, err := ps.listenerSpecs()
		if err != nil || len(specs) != 1 || specs[0].Addr != ":8081" {
			t.Fatalf("specs = %v, err = %v", specs, err)
		}
	})
	t.Run("LegacyCommaTrim", func(t *testing.T) {
		ps := &proxyServer{proxyCfg: &config.ProxyConfig{Listen: "127.0.0.1:9090, :"}}
		specs, err := ps.listenerSpecs()
		if err != nil || len(specs) != 1 || specs[0].Addr != "127.0.0.1:9090" {
			t.Fatalf("specs = %v, err = %v", specs, err)
		}
	})
	t.Run("LegacyTLSDefaultH3", func(t *testing.T) {
		ps := &proxyServer{
			proxyCfg: &config.ProxyConfig{Listen: ":8443", TLSCertFile: "x.pem", TLSKeyFile: "y.pem"},
		}
		specs, err := ps.listenerSpecs()
		if err != nil || len(specs) != 1 || specs[0].Addr != ":8443" || !specs[0].SSL || !specs[0].QUIC {
			t.Fatalf("specs = %v, err = %v", specs, err)
		}
	})
	t.Run("LegacyTLSWithHTTP3Addr", func(t *testing.T) {
		ps := &proxyServer{
			proxyCfg: &config.ProxyConfig{Listen: ":8443", HTTP3Addr: ":9443", TLSCertFile: "x.pem", TLSKeyFile: "y.pem"},
		}
		specs, err := ps.listenerSpecs()
		if err != nil {
			t.Fatal(err)
		}
		want := []config.ListenersSpec{
			{Addr: ":8443", SSL: true},
			{Addr: ":9443", QUIC: true},
		}
		if !reflect.DeepEqual(specs, want) {
			t.Fatalf("specs = %+v, want %+v", specs, want)
		}
	})
}

func TestCloseListeners(t *testing.T) {
	ps := &proxyServer{router: router.NewHostRouter()}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ps.lns = []net.Listener{ln}
	if err := ps.startQuicListener("127.0.0.1:0", &tls.Config{}); err != nil {
		t.Fatal(err)
	}
	conn := ps.h3Conns[0]
	ps.closeListeners()
	if _, err := ln.Accept(); err == nil {
		t.Fatal("expected tcp listener closed")
	}
	buf := make([]byte, 1)
	if _, _, err := conn.ReadFrom(buf); err == nil {
		t.Fatal("expected udp conn closed")
	}
}

func TestStartListenersError(t *testing.T) {
	ps := &proxyServer{proxyCfg: &config.ProxyConfig{Listeners: []string{":8081 h2c ssl"}}}
	err := ps.start()
	if err == nil || !strings.Contains(err.Error(), "listeners") {
		t.Fatalf("err = %v", err)
	}
}

func TestStartListenerRequiresTLS(t *testing.T) {
	orig := generateSelfSignedCert
	generateSelfSignedCert = func() (tls.Certificate, error) { return tls.Certificate{}, fmt.Errorf("nope") }
	defer func() { generateSelfSignedCert = orig }()

	ps := &proxyServer{proxyCfg: &config.ProxyConfig{Listeners: []string{":0 ssl"}}}
	err := ps.start()
	if err == nil || !strings.Contains(err.Error(), "requires TLS") {
		t.Fatalf("err = %v", err)
	}
}

func TestServeErrorStopsPeers(t *testing.T) {
	orig := h3Serve
	h3Serve = func(*http3.Server, net.PacketConn) error { return fmt.Errorf("boom") }
	defer func() { h3Serve = orig }()

	port := freePort(t)
	ps := &proxyServer{proxyCfg: &config.ProxyConfig{Listeners: []string{fmt.Sprintf("127.0.0.1:%d ssl h3", port)}}}
	done := make(chan error, 1)
	go func() { done <- ps.start() }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("start returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("start() never returned after serve error")
	}
}

func TestStartListenersEndToEnd(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "up:%s", r.URL.Path)
	}))
	defer up.Close()

	httpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sslLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpPort := httpLn.Addr().(*net.TCPAddr).Port
	sslPort := sslLn.Addr().(*net.TCPAddr).Port
	httpLn.Close()
	sslLn.Close()
	dir := t.TempDir()
	targets := []config.TargetConfig{{
		Name:      "tg",
		Upstreams: []config.UpstreamConfig{{URL: up.URL}},
		Locations: []config.LocationConfig{{Path: "/", Cache: false}},
	}}
	ps := newProxyServer(&config.ProxyConfig{
		Listeners: []string{
			fmt.Sprintf("127.0.0.1:%d", httpPort),
			fmt.Sprintf("127.0.0.1:%d ssl h3", sslPort),
			fmt.Sprintf("127.0.0.1:%d quic", freeUDPPort(t)),
		},
	}, targets, newDiskCache(t, dir), make(chan struct{}, 8), nil, 0)

	done := make(chan error, 1)
	go func() { done <- ps.start() }()

	// Plaintext HTTP on the first listener.
	resp := waitForServer(t, fmt.Sprintf("http://127.0.0.1:%d/x", httpPort))
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "up:/x" {
		ps.shutdown(context.Background())
		t.Fatalf("plaintext: status=%d body=%q", resp.StatusCode, string(body))
	}

	// HTTPS via self-signed certificate on the ssl listener.
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	var tresp *http.Response
	var terr error
	for i := 0; i < 100; i++ {
		tresp, terr = client.Get(fmt.Sprintf("https://127.0.0.1:%d/hello", sslPort))
		if terr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if terr != nil {
		ps.shutdown(context.Background())
		t.Fatalf("https get: %v", terr)
	}
	tbody, _ := io.ReadAll(tresp.Body)
	tresp.Body.Close()
	if tresp.StatusCode != 200 || string(tbody) != "up:/hello" {
		ps.shutdown(context.Background())
		t.Fatalf("https: status=%d body=%q", tresp.StatusCode, string(tbody))
	}

	// HTTP/3 on the same port as the ssl listener.
	h3t := &http3.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	h3client := &http.Client{Transport: h3t}
	var hresp *http.Response
	var herr error
	for i := 0; i < 50; i++ {
		hresp, herr = h3client.Get(fmt.Sprintf("https://127.0.0.1:%d/admin", sslPort))
		if herr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if herr != nil {
		h3t.CloseIdleConnections()
		ps.shutdown(context.Background())
		t.Fatalf("http3 get: %v", herr)
	}
	hb, _ := io.ReadAll(hresp.Body)
	hresp.Body.Close()
	if hresp.ProtoMajor != 3 || string(hb) != "up:/admin" {
		h3t.CloseIdleConnections()
		ps.shutdown(context.Background())
		t.Fatalf("proto=%q body=%q, want HTTP/3 up:/admin", hresp.Proto, string(hb))
	}
	h3t.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ps.shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case err := <-done:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("start returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("start() never returned")
	}
}

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

func TestServeErrorShutdownFailureLogs(t *testing.T) {
	origH3 := h3Serve
	h3Serve = func(*http3.Server, net.PacketConn) error { return fmt.Errorf("boom") }
	origShutdown := serverShutdown
	serverShutdown = func(ctx context.Context, srv *http.Server) error {
		_ = srv.Shutdown(ctx)
		return fmt.Errorf("shutdown-fail")
	}
	defer func() { h3Serve = origH3; serverShutdown = origShutdown }()

	port := freePort(t)
	ps := &proxyServer{proxyCfg: &config.ProxyConfig{Listeners: []string{fmt.Sprintf("127.0.0.1:%d ssl h3", port)}}}
	done := make(chan error, 1)
	go func() { done <- ps.start() }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("start returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("start() never returned after serve/shutdown failure")
	}
}
