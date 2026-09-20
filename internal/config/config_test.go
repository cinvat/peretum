package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writeTemp: %v", err)
	}
	return path
}

func TestParseMaxCacheSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"1GIB", 1024 * 1024 * 1024},
		{"2 MIB", 2 * 1024 * 1024},
		{"4KIB", 4096},
		{"3GB", 3 * 1024 * 1024 * 1024},
		{"500MB", 500 * 1024 * 1024},
		{"10KB", 10 * 1024},
		{"64B", 64},
		{"2mb", 2 * 1024 * 1024},
		{"3mib", 3 * 1024 * 1024},
		{"1024", 1024},
		{"1.5KB", 1536},
		{" 1 MB ", 1024 * 1024},
	}
	for _, c := range cases {
		pc := &ProxyConfig{MaxCacheSize: c.in}
		got, err := pc.ParseMaxCacheSize()
		if err != nil {
			t.Fatalf("ParseMaxCacheSize(%q) error: %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("ParseMaxCacheSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestParseMaxCacheSizeSuffixFloatErrors(t *testing.T) {
	pc := &ProxyConfig{MaxCacheSize: "abcMB"}
	if _, err := pc.ParseMaxCacheSize(); err == nil {
		t.Fatal("expected error for invalid number with suffix")
	}

	pc2 := &ProxyConfig{MaxCacheSize: "not-a-size"}
	if _, err := pc2.ParseMaxCacheSize(); err == nil {
		t.Fatal("expected error for plain invalid size")
	}

	pc3 := &ProxyConfig{}
	if _, err := pc3.ParseMaxCacheSize(); err == nil {
		t.Fatal("expected error for empty size")
	}
}

func TestParseMaxResponseBodySize(t *testing.T) {
	pc := &ProxyConfig{MaxResponseBodySize: "-1"}
	got, err := pc.ParseMaxResponseBodySize()
	if err != nil {
		t.Fatalf("ParseMaxResponseBodySize(-1) error: %v", err)
	}
	if got != -1 {
		t.Fatalf("ParseMaxResponseBodySize(-1) = %d, want -1 (infinity)", got)
	}

	pc2 := &ProxyConfig{MaxResponseBodySize: "10MB"}
	got2, err := pc2.ParseMaxResponseBodySize()
	if err != nil {
		t.Fatalf("ParseMaxResponseBodySize(10MB) error: %v", err)
	}
	if got2 != 10*1024*1024 {
		t.Fatalf("ParseMaxResponseBodySize(10MB) = %d", got2)
	}

	pc3 := &ProxyConfig{}
	if got3, err := pc3.ParseMaxResponseBodySize(); err != nil || got3 != 0 {
		t.Fatalf("empty size: got %d, err %v", got3, err)
	}

	pc4 := &ProxyConfig{MaxResponseBodySize: "nope"}
	if _, err := pc4.ParseMaxResponseBodySize(); err == nil {
		t.Fatal("expected error for invalid size")
	}
}

func TestParseMaxCacheAge(t *testing.T) {
	pc := &ProxyConfig{MaxCacheAge: "30s"}
	got, err := pc.ParseMaxCacheAge()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 30*time.Second {
		t.Fatalf("got %v", got)
	}

	if _, err := (&ProxyConfig{MaxCacheAge: "bogus"}).ParseMaxCacheAge(); err == nil {
		t.Fatal("expected error for invalid duration")
	}
}

func TestParseUpstreams(t *testing.T) {
	tc := &TargetConfig{Upstreams: []UpstreamConfig{
		{URL: "http://a:8080", Weight: 1},
		{URL: "https://b:9090", Weight: 2},
	}}
	urls, err := tc.ParseUpstreams()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(urls) != 2 {
		t.Fatalf("got %d urls", len(urls))
	}
	if urls[0].String() != "http://a:8080" || urls[1].String() != "https://b:9090" {
		t.Fatalf("unexpected urls: %v", urls)
	}

	tc2 := &TargetConfig{Upstreams: []UpstreamConfig{{URL: "no-scheme"}}}
	if _, err := tc2.ParseUpstreams(); err == nil {
		t.Fatal("expected error for invalid url")
	}
}

func TestParseCacheTTL(t *testing.T) {
	l := &LocationConfig{CacheTTL: ""}
	if got, err := l.ParseCacheTTL(); err != nil || got != 0 {
		t.Fatalf("empty ttl: got %v, err %v", got, err)
	}

	l2 := &LocationConfig{CacheTTL: "5m"}
	if got, err := l2.ParseCacheTTL(); err != nil || got != 5*time.Minute {
		t.Fatalf("ttl: got %v, err %v", got, err)
	}

	l3 := &LocationConfig{CacheTTL: "nope"}
	if _, err := l3.ParseCacheTTL(); err == nil {
		t.Fatal("expected error for invalid ttl")
	}
}

func TestMatches(t *testing.T) {
	exact := &LocationConfig{MatchType: MatchExact, Path: "/api"}
	if !exact.Matches("/api") {
		t.Fatal("exact match failed")
	}
	if exact.Matches("/api/x") {
		t.Fatal("exact match should fail")
	}

	rgx := &LocationConfig{MatchType: MatchRegex, Path: "^/v\\d+/"}
	if !rgx.Matches("/v2/users") {
		t.Fatal("regex match failed")
	}
	if rgx.Matches("/other") {
		t.Fatal("regex should not match")
	}
	// Regex compiled once; second call reuses compiledRegex.
	if !rgx.Matches("/v1/x") {
		t.Fatal("regex reuse failed")
	}

	// Only reached with MatchRegex + empty Path leaves compiledRegex nil.
	rgxEmpty := &LocationConfig{MatchType: MatchRegex, Path: ""}
	if rgxEmpty.Matches("/anything") {
		t.Fatal("nil compiledRegex should not match")
	}

	root := &LocationConfig{MatchType: MatchPrefix, Path: "/"}
	if !root.Matches("/anything/at/all") {
		t.Fatal("root prefix should always match")
	}

	pref := &LocationConfig{MatchType: MatchPrefix, Path: "/api/"}
	if !pref.Matches("/api/users") {
		t.Fatal("prefix match failed")
	}
	if pref.Matches("/other") {
		t.Fatal("prefix should not match")
	}

	// Default case falls through to prefix handling.
	def := &LocationConfig{MatchType: MatchType("unknown"), Path: "/v"}
	if !def.Matches("/val") {
		t.Fatal("default prefix handling failed")
	}
	if def.Matches("/xval") {
		t.Fatal("default should not match mismatched prefix")
	}
}

func TestParseURL(t *testing.T) {
	u, err := ParseURL("http://example.com:80/path")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if u.Scheme != "http" || u.Host != "example.com:80/path" {
		t.Fatalf("parsed: %+v", u)
	}
	if u.String() != "http://example.com:80/path" {
		t.Fatalf("String() = %s", u.String())
	}

	empty := (&URL{}).String()
	if empty != "://" {
		t.Fatalf("empty URL String() = %q", empty)
	}

	if _, err := ParseURL("host-only"); err != ErrInvalidURL {
		t.Fatalf("expected ErrInvalidURL, got %v", err)
	}
	if ErrInvalidURL.Error() == "" {
		t.Fatal("ErrInvalidURL has no message")
	}
}

func TestConfigError(t *testing.T) {
	e := configError{msg: "boom"}
	if e.Error() != "boom" {
		t.Fatalf("configError.Error() = %q", e.Error())
	}
}

func TestLoadProxy(t *testing.T) {
	path := writeTemp(t, "proxy.yaml", "listeners:\n  - :8080\n  - :8443 quic\nmetrics_addr: :9090\nmax_cache_size: 1GB\nmax_write_workers: -1\nmax_response_body_size: 50MB\n")
	cfg, err := LoadProxy(path)
	if err != nil {
		t.Fatalf("LoadProxy error: %v", err)
	}
	if len(cfg.Listeners) != 2 || cfg.Listeners[0] != ":8080" || cfg.Listeners[1] != ":8443 quic" {
		t.Fatalf("LoadProxy parsed: %+v", cfg)
	}
	if cfg.MetricsAddr != ":9090" {
		t.Fatalf("LoadProxy parsed: %+v", cfg)
	}
	if cfg.MaxWriteWorkers != -1 || cfg.MaxResponseBodySize != "50MB" {
		t.Fatalf("LoadProxy parsed new fields: %+v", cfg)
	}

	if _, err := LoadProxy("/nonexistent/proxy.yaml"); err == nil {
		t.Fatal("expected error for missing file")
	}

	bad := writeTemp(t, "bad.yaml", "listen: [unclosed")
	if _, err := LoadProxy(bad); err == nil {
		t.Fatal("expected error for invalid yaml")
	}
}

func TestLoadTarget(t *testing.T) {
	path := writeTemp(t, "target.yaml", "name: svc-a\nlisten: :8081\nhost: upstream.example.com\nupstreams:\n  - url: http://a:80\n")
	tgt, err := LoadTarget(path)
	if err != nil {
		t.Fatalf("LoadTarget error: %v", err)
	}
	if tgt.Name != "svc-a" || len(tgt.Upstreams) != 1 {
		t.Fatalf("LoadTarget parsed: %+v", tgt)
	}
	if tgt.Host != "upstream.example.com" {
		t.Fatalf("Host = %q, want upstream.example.com", tgt.Host)
	}

	if _, err := LoadTarget("/nonexistent/target.yaml"); err == nil {
		t.Fatal("expected error for missing file")
	}

	bad := writeTemp(t, "bad.yaml", "upstreams: [x")
	if _, err := LoadTarget(bad); err == nil {
		t.Fatal("expected error for invalid yaml")
	}
}

func TestLoadTargets(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.yaml"), []byte("name: a\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "b.yml"), []byte("name: b\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "c.txt"), []byte("name: c\n"), 0o644)
	os.Mkdir(filepath.Join(dir, "subdir"), 0o755)
	os.WriteFile(filepath.Join(dir, "subdir", "d.yaml"), []byte("name: d\n"), 0o644)

	targets, err := LoadTargets(dir)
	if err != nil {
		t.Fatalf("LoadTargets error: %v", err)
	}
	names := map[string]bool{}
	for _, tg := range targets {
		names[tg.Name] = true
	}
	if !names["a"] || !names["b"] {
		t.Fatalf("expected targets a and b, got %v", names)
	}
	if len(targets) != 2 {
		t.Fatalf("expected 2 targets, got %d", len(targets))
	}

	if _, err := LoadTargets("/nonexistent/dir"); err == nil {
		t.Fatal("expected error for missing dir")
	}

	emptyDir := t.TempDir()
	os.WriteFile(filepath.Join(emptyDir, "c.txt"), []byte("name: c\n"), 0o644)
	os.Mkdir(filepath.Join(emptyDir, "sub"), 0o755)
	if _, err := LoadTargets(emptyDir); err != ErrNoTargetFiles {
		t.Fatalf("expected ErrNoTargetFiles, got %v", err)
	}

	badDir := t.TempDir()
	os.WriteFile(filepath.Join(badDir, "a.yaml"), []byte("nope: [broken"), 0o644)
	if _, err := LoadTargets(badDir); err == nil {
		t.Fatal("expected error for invalid target yaml")
	}
}

func TestParseListener(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    *ListenersSpec
		wantErr string
	}{
		{"Plain", ":8081", &ListenersSpec{Addr: ":8081"}, ""},
		{"SSLWithPort", ":443 ssl", &ListenersSpec{Addr: ":443", SSL: true}, ""},
		{"SSLNoPortDefaults", "10.0.0.1 ssl", &ListenersSpec{Addr: "10.0.0.1:443", SSL: true}, ""},
		{"PlainNoPortDefaults", "10.0.0.1", &ListenersSpec{Addr: "10.0.0.1:80"}, ""},
		{"BarePort", "8443", &ListenersSpec{Addr: ":8443"}, ""},
		{"BarePortSSL", "8443 ssl", &ListenersSpec{Addr: ":8443", SSL: true}, ""},
		{"QuicImpliesTLS", ":8443 quic", &ListenersSpec{Addr: ":8443", QUIC: true}, ""},
		{"CombinedH3", ":443 ssl h3", &ListenersSpec{Addr: ":443", SSL: true, QUIC: true}, ""},
		{"H2Informational", ":8081 h2c http2", &ListenersSpec{Addr: ":8081"}, ""},
		{"IPv6", "[::1]:8443 ssl", &ListenersSpec{Addr: "[::1]:8443", SSL: true}, ""},
		{"Empty", "", nil, "empty listen directive"},
		{"UnknownFlag", ":8081 foobar", nil, "unknown listen flag"},
		{"Conflict", ":8081 h2c ssl", nil, "conflict"},
		{"Duplicate", ":8081 ssl ssl", nil, "duplicate listen flag"},
		{"PortOutOfRange", "70000", nil, "out of range"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseListener(tc.in)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseListener(%q): %v", tc.in, err)
			}
			if got == nil || *got != *tc.want {
				t.Fatalf("ParseListener(%q) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseListeners(t *testing.T) {
	c := &ProxyConfig{}
	if specs, err := c.ParseListeners(); err != nil || specs != nil {
		t.Fatalf("empty: specs = %v, err = %v", specs, err)
	}
	c = &ProxyConfig{Listeners: []string{":8080", ":443 ssl h3"}}
	specs, err := c.ParseListeners()
	if err != nil {
		t.Fatalf("ParseListeners: %v", err)
	}
	if len(specs) != 2 || specs[0].Addr != ":8080" || !specs[1].SSL || !specs[1].QUIC {
		t.Fatalf("specs = %+v", specs)
	}
	c = &ProxyConfig{Listeners: []string{":80", ":8081 h2c ssl badflag"}}
	if _, err := c.ParseListeners(); err == nil {
		t.Fatal("expected error")
	}
}

func TestResolveListenAddr(t *testing.T) {
	cases := []struct {
		in   string
		def  int
		want string
	}{
		{"", 443, ":443"},
		{"", 80, ":80"},
	}
	for _, tc := range cases {
		got, err := resolveListenAddr(tc.in, tc.def)
		if err != nil || got != tc.want {
			t.Fatalf("resolveListenAddr(%q, %d) = %q, %v; want %q", tc.in, tc.def, got, err, tc.want)
		}
	}
}
