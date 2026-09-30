package proxyserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cinvat/peretum/internal/config"
	"github.com/quic-go/quic-go/http3"
	"golang.org/x/net/http2"
)

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
	proxyCfg := &config.ProxyConfig{Listeners: []string{fmt.Sprintf("127.0.0.1:%d", port)}, CacheDir: dir}
	targets := []config.TargetConfig{
		{
			ServerName: "tg",
			Upstreams:  []config.UpstreamConfig{{URL: up.URL}},
			TLS:        &config.TargetTLSConfig{CertFile: "dummy.pem"},
			Locations:  []config.LocationConfig{{Path: "/", Cache: true}},
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
	err := Run(ctx, "/definitely/missing/config.yaml", "")
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
	err := Run(ctx, cfgPath, filepath.Join(dir, "missing-targets"))
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
		"server_name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n")
	writeFile(t, cfgPath, runConfigYAML(":0", "/dev/null/cache", false))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := Run(ctx, cfgPath, targetsDir)
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
		"server_name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n")
	writeFile(t, cfgPath, runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), false))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = Run(ctx, cfgPath, targetsDir)
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
		"server_name: tg\nupstreams:\n  - url: http://x:1\nlocations:\n  - path: /\n")
	var b strings.Builder
	b.WriteString(runConfigYAML(":0", filepath.Join(dir, "cache"), false))
	fmt.Fprintf(&b, "json_log:\n  enabled: true\n  access_log: \"/dev/null/a.jsonl\"\n  error_log: \"/dev/null/e.jsonl\"\n  stdout: false\n")
	writeFile(t, cfgPath, b.String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := Run(ctx, cfgPath, targetsDir)
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
		fmt.Sprintf("server_name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n", up.URL))
	writeFile(t, cfgPath, runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), false))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfgPath, targetsDir) }()

	resp := waitForServer(t, fmt.Sprintf("http://127.0.0.1:%d/x", port))
	resp.Body.Close()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run() never returned after cancel")
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
		fmt.Sprintf("server_name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n    cache: true\n", up.URL))
	cfg := runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), false)
	cfg += "max_write_workers: -1\nmax_response_body_size: \"50B\"\n"
	writeFile(t, cfgPath, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfgPath, targetsDir) }()

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
		t.Fatal("Run() never returned after cancel")
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
		fmt.Sprintf("server_name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n    cache: true\n", up.URL))
	cfg := runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), false)
	cfg += "max_write_workers: -1\nmax_response_body_size: \"-1\"\n"
	writeFile(t, cfgPath, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfgPath, targetsDir) }()

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
		t.Fatal("Run() never returned after cancel")
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
		fmt.Sprintf("server_name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n    cache: true\n", up.URL))
	writeFile(t, cfgPath, runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), true))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfgPath, targetsDir) }()

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
		t.Fatal("Run() never returned after cancel")
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
		fmt.Sprintf("server_name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n", up.URL))
	cfg := runConfigYAML(fmt.Sprintf("127.0.0.1:%d ssl", port), filepath.Join(dir, "cache"), false)
	cfg += fmt.Sprintf("tls_cert_file: %s\ntls_key_file: %s\n", cert, key)
	writeFile(t, cfgPath, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfgPath, targetsDir) }()

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
		t.Fatal("Run() never returned after cancel")
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
		fmt.Sprintf("server_name: tg\nupstreams:\n  - url: %s\nlocations:\n  - path: /\n", up.URL))
	writeFile(t, cfgPath, runConfigYAML(fmt.Sprintf("127.0.0.1:%d", port), filepath.Join(dir, "cache"), false))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfgPath, targetsDir) }()

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
		t.Fatal("Run() never returned after cancel")
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
		ServerName: "tg",
		Upstreams:  []config.UpstreamConfig{{URL: up.URL}},
		Locations:  []config.LocationConfig{{Path: "/", Cache: false}},
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

// --- Lazy mode (cluster.lazy) tests ---
