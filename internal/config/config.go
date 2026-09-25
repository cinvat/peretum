package config

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type MatchType string

const (
	MatchPrefix MatchType = "prefix"
	MatchExact  MatchType = "exact"
	MatchRegex  MatchType = "regex"
)

type ProxyConfig struct {
	CacheDir     string `yaml:"cache_dir"`
	MaxCacheSize string `yaml:"max_cache_size"`
	MaxCacheAge  string `yaml:"max_cache_age"`
	TLSCertFile  string `yaml:"tls_cert_file"`
	TLSKeyFile   string `yaml:"tls_key_file"`
	MetricsAddr  string `yaml:"metrics_addr"`

	// Listeners is the nginx-style list of frontend listeners, each an
	// "address[:port] [flags]" string where flags may include `ssl`, `h2c`,
	// `h2`/`http2`, and `h3`/`http3`/`quic`.
	Listeners []string `yaml:"listeners"`

	// MaxWriteWorkers is the maximum number of concurrent cache writes.
	// -1 means unlimited; 0 uses the default (8).
	MaxWriteWorkers int `yaml:"max_write_workers"`
	// MaxResponseBodySize is the maximum buffered response body size before
	// proxying a non-streaming (HTTP/2 candidate) response. "-1" means
	// unlimited; empty uses the default (50MB). Accepts the same size
	// suffixes as max_cache_size ("50MB", "1GB", ...).
	MaxResponseBodySize string `yaml:"max_response_body_size"`

	// JSON log plugin (nginx-style access + error logs)
	JSONLog *JSONLogConfig `yaml:"json_log"`

	// Error page plugin: branded HTML error pages for client-facing errors.
	ErrorPage *ErrorPageConfig `yaml:"error_page"`

	// WAF plugin: global settings. GeoLite DB paths are general and shared
	// across all per-location WAF configs.
	WAF *WAFConfig `yaml:"waf"`

	// Cluster enables CDN-scale features: sharding, delta reload, lazy loading, config streaming.
	Cluster *ClusterConfig `yaml:"cluster"`
}

// WAFConfig holds the global WAF plugin settings. Geo databases are common
// across every location; per-location rules live in LocationConfig.WAF.
type WAFConfig struct {
	Enabled    bool   `yaml:"enabled"`
	GeoLiteDir string `yaml:"geolite_dir"`
}

// ClusterConfig holds cluster feature configuration.
type ClusterConfig struct {
	Enabled bool `yaml:"enabled"`
	// NATSURI points at the external NATS JetStream cluster holding target
	// state. Empty means the edge seeds itself from the local config.d
	// directory and runs no consumer.
	NATSURI string `yaml:"nats_uri"`

	// Lazy enables on-disk target storage backed by Pebble: cold configs stay
	// off-RAM and a target's compiled handlers are materialized on first
	// request into a bounded LRU. Required for CDN-scale (10M+ target) edges.
	Lazy    bool   `yaml:"lazy"`
	DataDir string `yaml:"data_dir"`
	LRUSize int    `yaml:"lru_size"`
}

type ErrorPageConfig struct {
	Enabled bool `yaml:"enabled"`
	// Statuses are the HTTP status codes rendered as branded pages.
	// Default: [404, 500, 502, 503, 504].
	Statuses []int `yaml:"statuses"`
}

type JSONLogConfig struct {
	Enabled   bool   `yaml:"enabled"`
	AccessLog string `yaml:"access_log"`
	ErrorLog  string `yaml:"error_log"`
	Stdout    bool   `yaml:"stdout"`
}

type TargetConfig struct {
	ServerName  string           `yaml:"server_name" json:"server_name"`
	HostHeader  string           `yaml:"host_header" json:"host_header"`
	Upstreams   []UpstreamConfig `yaml:"upstreams" json:"upstreams"`
	LBAlgorithm string           `yaml:"lb_algorithm" json:"lb_algorithm"`
	Locations   []LocationConfig `yaml:"locations" json:"locations"`
	TLS         *TargetTLSConfig `yaml:"tls" json:"tls"`
}

type TargetTLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type UpstreamConfig struct {
	URL         string             `yaml:"url"`
	Weight      int                `yaml:"weight"`
	HealthCheck *HealthCheckConfig `yaml:"health_check"`
}

type HealthCheckConfig struct {
	Path           string            `yaml:"path"`
	Interval       string            `yaml:"interval"`        // e.g., "10s"
	Timeout        string            `yaml:"timeout"`         // e.g., "3s"
	ExpectedStatus int               `yaml:"expected_status"` // default 200
	Headers        map[string]string `yaml:"headers"`         // custom headers
}

type LocationConfig struct {
	Path          string    `yaml:"path"`
	MatchType     MatchType `yaml:"match_type"`
	Cache         bool      `yaml:"cache"`
	CacheTTL      string    `yaml:"cache_ttl"`
	CacheExcludes []string  `yaml:"cache_excludes"` // paths/extensions to bypass cache

	// CORS
	CORS *CORSConfig `yaml:"cors"`

	// Rewrite
	Rewrite *RewriteConfig `yaml:"rewrite"`

	// Headers
	Headers *HeadersConfig `yaml:"headers"`

	// Proxy settings
	Proxy *ProxyLocationConfig `yaml:"proxy"`

	// Optimizations (applied before caching)
	Optimize *OptimizeConfig `yaml:"optimize"`

	// Compression
	Compression *CompressionConfig `yaml:"compression"`

	// WAF policy applied to requests matching this location.
	WAF *WAFLocationConfig `yaml:"waf"`

	compiledRegex *regexp.Regexp
}

// WAFLocationConfig is the per-location WAF policy. Rules are declared inline
// as YAML under the location (no separate rule files) and carry their own
// action (deny/allow/log with optional status code and message).
type WAFLocationConfig struct {
	Enabled bool `yaml:"enabled"`

	Rules []WAFRule `yaml:"rules"`
}

// WAFRule is a single WAF rule. Conditions use the DNF shape inherited from
// the Lua WAF: rule.conditions is a list of groups (OR), each group is a list
// of conditions (AND).
type WAFRule struct {
	ID      string        `yaml:"id"`
	Name    string        `yaml:"name"`
	Enabled *bool         `yaml:"enabled"`
	Action  WAFRuleAction `yaml:"action"`
	// Conditions is a list of AND-groups; the rule matches when ANY group
	// matches (OR between groups).
	Conditions [][]WAFCondition `yaml:"conditions"`
}

// WAFRuleAction mirrors the Lua actions handler. Type is one of
// "deny", "allow", or "log".
type WAFRuleAction struct {
	Type    string `yaml:"type"`
	Code    int    `yaml:"code"`
	Message string `yaml:"message"`
}

// WAFCondition mirrors the Lua conditions: a parameter getter, an operator,
// and a value/pattern. Operators: contains, equals, startswith, endswith,
// matches (regex), In, not_in, gt, lt, exists, not_exists.
type WAFCondition struct {
	Param     string `yaml:"param"`
	ParamName string `yaml:"param_name"`
	Operator  string `yaml:"operator"`
	Value     string `yaml:"value"`
}

type CompressionConfig struct {
	Enabled   bool     `yaml:"enabled"`
	Level     int      `yaml:"level"`      // 1-9 (default: 5)
	MinLength int      `yaml:"min_length"` // minimum response size to compress (default: 1024)
	Types     []string `yaml:"types"`      // MIME types to compress
}

type OptimizeConfig struct {
	// CSS/JS minification
	MinifyCSS bool `yaml:"minify_css"`
	MinifyJS  bool `yaml:"minify_js"`
	UglifyJS  bool `yaml:"uglify_js"`

	// Image optimization
	Images *ImageOptimizeConfig `yaml:"images"`

	// General
	Enabled bool `yaml:"enabled"`
}

type ImageOptimizeConfig struct {
	Enabled       bool   `yaml:"enabled"`
	MaxWidth      int    `yaml:"max_width"`
	MaxHeight     int    `yaml:"max_height"`
	Quality       int    `yaml:"quality"` // 1-100
	Format        string `yaml:"format"`  // "webp", "avif", "jpeg", "png"
	StripMetadata bool   `yaml:"strip_metadata"`
	Progressive   bool   `yaml:"progressive"`
}

type CORSConfig struct {
	Enabled          bool     `yaml:"enabled"`
	AllowOrigins     []string `yaml:"allow_origins"`
	AllowMethods     []string `yaml:"allow_methods"`
	AllowHeaders     []string `yaml:"allow_headers"`
	ExposeHeaders    []string `yaml:"expose_headers"`
	AllowCredentials bool     `yaml:"allow_credentials"`
	MaxAge           int      `yaml:"max_age"`
}

type RewriteConfig struct {
	// Regex pattern to match
	Pattern string `yaml:"pattern"`
	// Replacement string (supports $1, $2, etc.)
	Replacement string `yaml:"replacement"`
	// Whether to break after rewrite (like nginx 'break')
	Break bool `yaml:"break"`
	// Whether to redirect (like nginx 'redirect' or 'permanent')
	Redirect string `yaml:"redirect"` // "", "redirect", "permanent"
}

type HeadersConfig struct {
	// Request headers to add/remove before proxying
	RequestAdd    map[string]string `yaml:"request_add"`
	RequestRemove []string          `yaml:"request_remove"`
	// Response headers to add/remove
	ResponseAdd    map[string]string `yaml:"response_add"`
	ResponseRemove []string          `yaml:"response_remove"`
}

type ProxyLocationConfig struct {
	// Override upstream for this location
	Upstream string `yaml:"upstream"`
	// Timeout settings
	ConnectTimeout string `yaml:"connect_timeout"`
	ReadTimeout    string `yaml:"read_timeout"`
	SendTimeout    string `yaml:"send_timeout"`
	// Buffering
	Buffering  bool   `yaml:"buffering"`
	BufferSize string `yaml:"buffer_size"`
	// WebSocket support
	WebSocket bool `yaml:"websocket"`
	// Pass host header
	PassHostHeader bool `yaml:"pass_host_header"`
}

func (c *ProxyConfig) ParseMaxCacheSize() (int64, error) {
	return parseSize(c.MaxCacheSize)
}

func (c *ProxyConfig) ParseMaxResponseBodySize() (int64, error) {
	if c.MaxResponseBodySize == "" {
		return 0, nil
	}
	return parseSize(c.MaxResponseBodySize)
}

// ListenersSpec is a single resolved frontend listener: the bind address
// plus whether it serves TLS on TCP and/or HTTP/3 over QUIC/UDP.
type ListenersSpec struct {
	Addr string
	SSL  bool
	QUIC bool
}

// ParseListeners parses every `listeners:` entry into resolved specs.
// It returns nothing when the field is empty so callers fall back to the
// legacy `listen` shorthand.
func (c *ProxyConfig) ParseListeners() ([]ListenersSpec, error) {
	if len(c.Listeners) == 0 {
		return nil, nil
	}
	specs := make([]ListenersSpec, 0, len(c.Listeners))
	for i, raw := range c.Listeners {
		spec, err := ParseListener(raw)
		if err != nil {
			return nil, fmt.Errorf("listeners[%d]: %w", i, err)
		}
		specs = append(specs, *spec)
	}
	return specs, nil
}

// ParseListener parses a single nginx-style listen directive such as
// ":443 ssl h3" and resolves it to a bind address. Accepted flags:
//
//	ssl       serve TLS on the TCP listener
//	h2c       unencrypted HTTP/2 (conflicts with ssl)
//	h2/http2  HTTP/2 over TLS via ALPN (informational; enabled anyway)
//	h3/http3/quic  serve HTTP/3 over QUIC/UDP (implies TLS)
//
// A quic-only directive binds UDP only; combine it with `ssl` to also open
// the matching TCP listener. When no port is given it defaults to 443 for
// TLS/QUIC listeners and 80 otherwise.
func ParseListener(raw string) (*ListenersSpec, error) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return nil, fmt.Errorf("empty listen directive")
	}
	var ssl, quic, h2c bool
	seen := make(map[string]bool)
	for _, f := range fields[1:] {
		if seen[f] {
			return nil, fmt.Errorf("duplicate listen flag %q", f)
		}
		seen[f] = true
		switch f {
		case "ssl":
			ssl = true
		case "h3", "http3", "quic":
			quic = true
		case "h2c":
			h2c = true
		case "h2", "http2":
			// HTTP/2 is always negotiable: over TLS via ALPN, as h2c on
			// plaintext listeners. Accepted for nginx parity.
		default:
			return nil, fmt.Errorf("unknown listen flag %q", f)
		}
	}
	if ssl && h2c {
		return nil, fmt.Errorf("listen flags conflict: h2c cannot be combined with ssl")
	}
	defaultPort := 80
	if ssl || quic {
		defaultPort = 443
	}
	addr, err := resolveListenAddr(fields[0], defaultPort)
	if err != nil {
		return nil, err
	}
	return &ListenersSpec{Addr: addr, SSL: ssl, QUIC: quic}, nil
}

// resolveListenAddr normalizes the address token of a listen directive into
// a form usable by net.Listen: "host:port", ":port", or a bare port/host
// with the given default port filled in.
func resolveListenAddr(raw string, defaultPort int) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fmt.Sprintf(":%d", defaultPort), nil
	}
	if _, _, err := net.SplitHostPort(raw); err == nil {
		return raw, nil
	}
	if p, err := strconv.Atoi(strings.TrimPrefix(raw, ":")); err == nil {
		if p < 0 || p > 65535 {
			return "", fmt.Errorf("listen port %d out of range", p)
		}
		return fmt.Sprintf(":%d", p), nil
	}
	return net.JoinHostPort(raw, strconv.Itoa(defaultPort)), nil
}

func (c *ProxyConfig) ParseMaxCacheAge() (time.Duration, error) {
	return time.ParseDuration(c.MaxCacheAge)
}

func (t *TargetConfig) ParseUpstreams() ([]*URL, error) {
	var urls []*URL
	for _, u := range t.Upstreams {
		url, err := ParseURL(u.URL)
		if err != nil {
			return nil, err
		}
		urls = append(urls, url)
	}
	return urls, nil
}

func (l *LocationConfig) ParseCacheTTL() (time.Duration, error) {
	if l.CacheTTL == "" {
		return 0, nil
	}
	return time.ParseDuration(l.CacheTTL)
}

func (l *LocationConfig) Matches(path string) bool {
	switch l.MatchType {
	case MatchExact:
		return l.Path == path
	case MatchRegex:
		if l.compiledRegex == nil && l.Path != "" {
			l.compiledRegex = regexp.MustCompile(l.Path)
		}
		if l.compiledRegex != nil {
			return l.compiledRegex.MatchString(path)
		}
		return false
	case MatchPrefix:
		fallthrough
	default:
		if l.Path == "/" {
			return true
		}
		return strings.HasPrefix(path, strings.TrimSuffix(l.Path, "/"))
	}
}

type URL struct {
	Scheme string
	Host   string
}

func ParseURL(raw string) (*URL, error) {
	parts := strings.SplitN(raw, "://", 2)
	if len(parts) != 2 {
		return nil, ErrInvalidURL
	}
	return &URL{Scheme: parts[0], Host: parts[1]}, nil
}

func (u *URL) String() string {
	return u.Scheme + "://" + u.Host
}

var ErrInvalidURL = &configError{"invalid upstream URL"}

type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	s = strings.ToUpper(s)

	// Check longer suffixes first to avoid "B" matching before "MB"
	suffixes := []struct {
		suffix string
		mult   int64
	}{
		{"GIB", 1024 * 1024 * 1024},
		{"MIB", 1024 * 1024},
		{"KIB", 1024},
		{"GB", 1024 * 1024 * 1024},
		{"MB", 1024 * 1024},
		{"KB", 1024},
		{"B", 1},
	}

	for _, sm := range suffixes {
		if strings.HasSuffix(s, sm.suffix) {
			num := strings.TrimSuffix(s, sm.suffix)
			val, err := parseFloat(num)
			if err != nil {
				return 0, err
			}
			return int64(val * float64(sm.mult)), nil
		}
	}

	val, err := parseFloat(s)
	if err != nil {
		return 0, err
	}
	return int64(val), nil
}

func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(s), 64)
}
