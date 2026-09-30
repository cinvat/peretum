package proxyserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/plugin/manager"
	"github.com/cinvat/peretum/internal/router"
)

func TestNewProxyServer(t *testing.T) {
	cfg := &config.ProxyConfig{Listeners: []string{":8081"}}
	tgt := []config.TargetConfig{{ServerName: "tg"}}
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
			ServerName: "a",
			Upstreams:  []config.UpstreamConfig{{URL: "http://a:1", Weight: 1}},
			Locations:  []config.LocationConfig{{Path: "/", Cache: true}, {Path: "/", Cache: true}},
		},
		{
			ServerName: "b.example",
			Upstreams:  []config.UpstreamConfig{{URL: "http://b:2"}},
			Locations:  []config.LocationConfig{{Path: "/api"}},
		},
		{
			ServerName: "plainhost",
			Upstreams:  []config.UpstreamConfig{{URL: "http://c:3"}},
			Locations:  []config.LocationConfig{{Path: "/", Cache: true}},
		},
		{
			ServerName: "d",
			Upstreams:  []config.UpstreamConfig{{URL: "http://d:4"}},
			Locations:  []config.LocationConfig{{Path: "/x"}},
		},
		{
			ServerName: "bad",
			Upstreams:  []config.UpstreamConfig{{URL: "not-a-url"}},
			Locations:  []config.LocationConfig{{Path: "/"}},
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
			{ServerName: "notls"},
			{ServerName: "empty", TLS: &config.TargetTLSConfig{CertFile: "", KeyFile: ""}},
			{ServerName: "nokey", TLS: &config.TargetTLSConfig{CertFile: cert}},
			{ServerName: "badpair", TLS: &config.TargetTLSConfig{CertFile: bad, KeyFile: bad}},
			{ServerName: "good", TLS: &config.TargetTLSConfig{CertFile: cert, KeyFile: key}},
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
	if !slices.Contains(tc.NextProtos, "h2") || !slices.Contains(tc.NextProtos, "http/1.1") {
		t.Fatalf("base NextProtos = %v, want h2 + http/1.1", tc.NextProtos)
	}
	got, err := tc.GetConfigForClient(nil)
	if err != nil || got == nil {
		t.Fatalf("GetConfigForClient = %v, %v", got, err)
	}
	// The per-handshake config replaces the base config, so it must repeat
	// the ALPN list: otherwise h2 is never negotiated over TLS.
	if !slices.Contains(got.NextProtos, "h2") || !slices.Contains(got.NextProtos, "http/1.1") {
		t.Fatalf("GetConfigForClient NextProtos = %v, want h2 + http/1.1", got.NextProtos)
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
		ps := &proxyServer{proxyCfg: &config.ProxyConfig{Listeners: []string{":8443 quic"}}}
		if tc := ps.buildTLSConfig(); tc != nil {
			t.Fatalf("expected nil tls.Config when self-signed generation fails, got %v", tc)
		}
	})

	t.Run("GlobalAndTargetCerts", func(t *testing.T) {
		ps := &proxyServer{
			proxyCfg: &config.ProxyConfig{TLSCertFile: cert, TLSKeyFile: key},
			targets: []config.TargetConfig{
				{ServerName: "good", TLS: &config.TargetTLSConfig{CertFile: cert, KeyFile: key}},
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
				{ServerName: "a", TLS: nil},
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
		{"http3Addr", &config.ProxyConfig{Listeners: []string{":8443 quic"}}, nil, true},
		{"globalCert", &config.ProxyConfig{TLSCertFile: "c.pem"}, nil, true},
		{"globalKey", &config.ProxyConfig{TLSKeyFile: "k.pem"}, nil, true},
		{"targetTLS", &config.ProxyConfig{}, []config.TargetConfig{{ServerName: "t", TLS: &config.TargetTLSConfig{CertFile: "c.pem"}}}, true},
		{"targetNoTLS", &config.ProxyConfig{}, []config.TargetConfig{{ServerName: "t"}}, false},
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

func TestBuildHealthCheckConfig(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		hc := buildHealthCheckConfig(&config.HealthCheckConfig{Path: "/healthz"})
		if hc.Path != "/healthz" {
			t.Fatalf("Path = %q", hc.Path)
		}
		if hc.Interval != 10*time.Second {
			t.Fatalf("Interval = %v, want 10s", hc.Interval)
		}
		if hc.Timeout != 3*time.Second {
			t.Fatalf("Timeout = %v, want 3s", hc.Timeout)
		}
		if hc.ExpectedStatus != 200 {
			t.Fatalf("ExpectedStatus = %d, want the implicit default 200", hc.ExpectedStatus)
		}
	})

	t.Run("explicit values", func(t *testing.T) {
		hc := buildHealthCheckConfig(&config.HealthCheckConfig{
			Path:           "/ready",
			Interval:       "1m",
			Timeout:        "5s",
			ExpectedStatus: 204,
			Headers:        map[string]string{"X-Probe": "1"},
		})
		if hc.Interval != time.Minute || hc.Timeout != 5*time.Second {
			t.Fatalf("Interval/Timeout = %v/%v", hc.Interval, hc.Timeout)
		}
		if hc.ExpectedStatus != 204 {
			t.Fatalf("ExpectedStatus = %d, want 204", hc.ExpectedStatus)
		}
		if hc.Headers["X-Probe"] != "1" {
			t.Fatalf("Headers = %v", hc.Headers)
		}
	})

	t.Run("invalid durations fall back to defaults", func(t *testing.T) {
		// A typo in the duration must not produce a zero interval, which would
		// spin the health-check ticker.
		hc := buildHealthCheckConfig(&config.HealthCheckConfig{
			Path:     "/healthz",
			Interval: "not-a-duration",
			Timeout:  "also-bad",
		})
		if hc.Interval != 10*time.Second || hc.Timeout != 3*time.Second {
			t.Fatalf("Interval/Timeout = %v/%v, want the defaults", hc.Interval, hc.Timeout)
		}
	})
}

// The whole point of the store-backed router: a store holding many thousands of
// targets must produce a routing table that stays empty, and serving them must
// stay within the bounded LRU.
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
		ps := &proxyServer{proxyCfg: &config.ProxyConfig{Listeners: []string{"127.0.0.1:9090"}}}
		specs, err := ps.listenerSpecs()
		if err != nil || len(specs) != 1 || specs[0].Addr != "127.0.0.1:9090" {
			t.Fatalf("specs = %v, err = %v", specs, err)
		}
	})
	t.Run("LegacyTLSDefaultH3", func(t *testing.T) {
		ps := &proxyServer{
			proxyCfg: &config.ProxyConfig{Listeners: []string{":8443 ssl quic"}, TLSCertFile: "x.pem", TLSKeyFile: "y.pem"},
		}
		specs, err := ps.listenerSpecs()
		if err != nil || len(specs) != 1 || specs[0].Addr != ":8443" || !specs[0].SSL || !specs[0].QUIC {
			t.Fatalf("specs = %v, err = %v", specs, err)
		}
	})
	t.Run("LegacyTLSWithHTTP3Addr", func(t *testing.T) {
		ps := &proxyServer{
			proxyCfg: &config.ProxyConfig{Listeners: []string{":8443 ssl", ":9443 quic"}, TLSCertFile: "x.pem", TLSKeyFile: "y.pem"},
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
