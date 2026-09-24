package handler

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"regexp"
	"strings"
	"sync"
	"time"

	disk "github.com/cinvat/peretum/internal/cache/disk"
	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/loadbalancer"
	"github.com/cinvat/peretum/internal/plugin/manager"
	"github.com/cinvat/peretum/plugins/base"
	"golang.org/x/net/http2"
	"k8s.io/klog/v2"
)

const (
	maxBodyReadSize = 50 << 20
	maxWriteWorkers = 8

	// grpcContentTypePrefix identifies gRPC requests (application/grpc,
	// application/grpc+proto, application/grpc+json, ...).
	grpcContentTypePrefix = "application/grpc"
)

// requestLogger is implemented structurally by plugins (e.g. jsonlog)
// that want every request traced: StartRequest wraps the ResponseWriter
// so status/bytes can be captured, and FinishRequest emits the access
// log line once the request completes.
type requestLogger interface {
	StartRequest(w http.ResponseWriter, r *http.Request, target, location string) http.ResponseWriter
	FinishRequest(w http.ResponseWriter, r *http.Request, target, location string, cached bool)
}

type capturingTransport struct {
	transport        http.RoundTripper
	lb               loadbalancer.LoadBalancer
	pinned           string // location-pinned upstream ("http://host"), or "" to balance
	statusCode       int
	headers          http.Header
	body             []byte
	selectedUpstream string
	maxBodySize      int64
}

// upstreamIdentityHeaders are response headers that disclose the upstream
// server, CDN, or edge cache behind the proxy. A reverse proxy should not
// forward them to clients verbatim: they are stripped before the response is
// buffered or replayed from cache, so clients can only see peretum.
var upstreamIdentityHeaders = []string{
	"Server",
	"Via",
	"Age",
	"X-Powered-By",
	"X-Cache",
	"X-Cache-Hits",
	"X-Cache-Status",
	"X-Served-By",
	"X-Backend-Server",
	"X-Backend-Host",
	"Cf-Ray",
	"Cf-Cache-Status",
	"Cf-Worker",
}

// sanitizeResponseHeaders removes upstream-identity headers and replaces the
// Server value so clients cannot tell which origin or CDN served the body.
func sanitizeResponseHeaders(hdr http.Header) {
	for _, k := range upstreamIdentityHeaders {
		hdr.Del(k)
	}
	hdr.Set("Server", "peretum")
}

func (ct *capturingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	klog.Infof("capturingTransport.RoundTrip: %s %s", req.Method, req.URL.String())
	targetURL := ct.pinned
	var lbUpstream *loadbalancer.Upstream
	if targetURL == "" {
		lbUpstream = ct.lb.Next(req)
		if lbUpstream == nil {
			return nil, fmt.Errorf("no healthy upstreams")
		}
		targetURL = lbUpstream.URL
	}
	ct.selectedUpstream = targetURL

	// Forward to the resolved upstream. The reverse proxy's Rewrite pins
	// req.URL to a placeholder (the first upstream, or a location-pinned
	// one), so when load balancing, re-point the request at the
	// actually-selected host. A pinned location upstream already has the
	// correct URL in req.
	outReq := req
	if lbUpstream != nil {
		outReq = req.Clone(req.Context())
		if scheme, rest, ok := strings.Cut(targetURL, "://"); ok {
			u := *req.URL
			u.Scheme = scheme
			u.Host = rest
			outReq.URL = &u
		}
	}

	resp, err := ct.transport.RoundTrip(outReq)
	if err != nil {
		klog.Infof("RoundTrip error: %v", err)
		ct.lb.MarkHealthy(targetURL, false)
		return nil, err
	}

	klog.Infof("Upstream response: %d", resp.StatusCode)

	ct.statusCode = resp.StatusCode
	ct.headers = resp.Header

	// Limit the amount of body buffered. A limit < 0 means unlimited; 0
	// falls back to the built-in default cap.
	limit := ct.maxBodySize
	if limit == 0 {
		limit = maxBodyReadSize
	}
	var reader io.Reader = resp.Body
	if limit >= 0 {
		reader = io.LimitReader(resp.Body, limit+1)
	}
	var buf bytes.Buffer
	tee := io.TeeReader(reader, &buf)
	body, err := io.ReadAll(tee)
	resp.Body.Close()

	if err != nil {
		klog.Infof("ReadAll error: %v", err)
		ct.lb.MarkHealthy(targetURL, false)
		return nil, err
	}
	if limit >= 0 && int64(len(body)) > limit {
		ct.lb.MarkHealthy(targetURL, false)
		return nil, fmt.Errorf("response body exceeds max read size")
	}

	ct.lb.MarkHealthy(targetURL, true)
	ct.body = buf.Bytes()
	// The body is fully buffered, so its length is known even when the
	// upstream streamed it (chunked transfer encoding, ContentLength == -1).
	// Report the real length: otherwise httputil.ReverseProxy treats the
	// response as an unbounded stream and flushes after every write, which
	// emits the response headers before the compression plugin has set
	// Content-Encoding — losing the header and delivering raw gzip bytes.
	resp.ContentLength = int64(len(ct.body))
	// Do not forward upstream/CDN identity headers (Server, cf-ray, ...)
	// either live or into the cache.
	sanitizeResponseHeaders(resp.Header)
	// Snapshot the sanitized headers. ct.headers is what gets stored to
	// cache and handed to plugins, while resp.Header is the live map that
	// ReverseProxy.ModifyResponse later mutates (X-Cache, location
	// response_add). Sharing the map would bake those per-request headers
	// into the cached entry and replay them on every hit.
	ct.headers = resp.Header.Clone()
	resp.Body = io.NopCloser(bytes.NewReader(ct.body))
	return resp, nil
}

// grpcTransport selects an upstream through the load balancer and forwards
// the request without reading or buffering the response body, so gRPC
// server-streaming and bidi-streaming are preserved. Health is updated on
// transport-level errors only; body-gated health checking would stall
// long-lived streams.
//
// gRPC backends speak HTTP/2 prior-knowledge (h2c) over plaintext, so those
// requests use an h2c-aware RoundTripper instead of a plain HTTP/1.1 one;
// TLS backends keep http.DefaultTransport, which negotiates h2 via ALPN.
type grpcTransport struct {
	lb     loadbalancer.LoadBalancer
	pinned string // location-pinned upstream, or "" to balance
}

// h2cRT is a shared HTTP/2 RoundTripper that dials plaintext upstreams with
// h2c prior-knowledge (no TLS, no Upgrade handshake). Safe for concurrent use.
var h2cRT http.RoundTripper = &http2.Transport{
	AllowHTTP: true,
	DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	},
}

// proxyTransport is the RoundTripper used for proxied (non-gRPC) upstream
// requests. Compression is disabled at the transport layer: the Rewrite drops
// the client's Accept-Encoding so cached bodies stay uncompressed, and Go
// would otherwise re-add "Accept-Encoding: gzip" itself. That advertises gzip
// the proxy never asked for, and CDNs such as Cloudflare then answer with a
// gzip body without a Content-Encoding header — which Go does not
// auto-decompress — leaving the client with raw compressed bytes.
var proxyTransport http.RoundTripper = func() http.RoundTripper {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableCompression = true
	return tr
}()

func (gt *grpcTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	targetURL := gt.pinned
	var upstream *loadbalancer.Upstream
	if targetURL == "" {
		upstream = gt.lb.Next(req)
		if upstream == nil {
			return nil, fmt.Errorf("no healthy upstreams")
		}
		targetURL = upstream.URL
		// Point the request at the LB-selected upstream: the gRPC reverse
		// proxy's Rewrite pinned the URL to a placeholder, but balancing is
		// decided here.
		req = req.Clone(req.Context())
		if scheme, rest, ok := strings.Cut(targetURL, "://"); ok {
			u := *req.URL
			u.Scheme = scheme
			u.Host = rest
			req.URL = &u
		}
	}
	var rt http.RoundTripper
	if req.URL.Scheme == "https" {
		rt = http.DefaultTransport
	} else {
		rt = h2cRT
	}
	resp, err := rt.RoundTrip(req)
	if err != nil {
		gt.lb.MarkHealthy(targetURL, false)
		return nil, err
	}
	gt.lb.MarkHealthy(targetURL, true)
	return resp, nil
}

// isGRPCRequest reports whether the request is a gRPC call, identified by
// its Content-Type (application/grpc or application/grpc+proto, ...).
func isGRPCRequest(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Content-Type"), grpcContentTypePrefix)
}

type TargetHandler struct {
	target      *config.TargetConfig
	location    *config.LocationConfig
	diskCache   *disk.DiskCache
	writeSem    chan struct{}
	lb          loadbalancer.LoadBalancer
	rewriter    *regexp.Regexp
	cacheTTL    time.Duration
	maxBodySize int64

	// Plugin manager for hooks
	pluginMgr *manager.PluginManager

	// Request coalescing: prevents multiple concurrent requests for the same key
	// from all hitting the upstream. Uses single-flight pattern.
	flightMu sync.Map // key -> *singleFlight
}

type singleFlight struct {
	ch        chan struct{}
	resp      *http.Response
	err       error
	done      bool
	closeOnce sync.Once
}

func NewTargetHandler(target *config.TargetConfig, location *config.LocationConfig, diskCache *disk.DiskCache, writeSem chan struct{}, lb loadbalancer.LoadBalancer, pluginMgr *manager.PluginManager, maxBodySize int64) *TargetHandler {
	h := &TargetHandler{
		target:      target,
		location:    location,
		diskCache:   diskCache,
		writeSem:    writeSem,
		lb:          lb,
		pluginMgr:   pluginMgr,
		maxBodySize: maxBodySize,
	}

	// Pre-compute the per-location cache TTL so cache lookups can expire
	// entries sooner than the global max_cache_age.
	if location.CacheTTL != "" {
		if d, err := location.ParseCacheTTL(); err == nil {
			h.cacheTTL = d
		}
	}

	// Pre-compile rewrite regex if configured
	if location.Rewrite != nil && location.Rewrite.Pattern != "" {
		h.rewriter = regexp.MustCompile(location.Rewrite.Pattern)
	}

	return h
}

// resolveMaxBodySize returns the effective buffered-body limit: -1 means
// unlimited, 0 falls back to the built-in default, and any positive value
// is used verbatim.
func (th *TargetHandler) resolveMaxBodySize() int64 {
	switch {
	case th.maxBodySize < 0:
		return -1
	case th.maxBodySize == 0:
		return maxBodyReadSize
	default:
		return th.maxBodySize
	}
}

func (th *TargetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	klog.Infof("ServeHTTP called: %s %s", r.Method, r.URL.Path)

	// Attach a stable request ID so access and error log entries for the
	// same request can be correlated.
	r, _ = base.EnsureRequestID(r)

	// Wrap the response writer so plugins (e.g. jsonlog) can log every
	// completed request, including those served from cache. The deferred
	// FinishRequest runs even if a later step panics.
	var rl requestLogger
	if th.pluginMgr != nil {
		for _, p := range th.pluginMgr.GetPlugins() {
			if lg, ok := p.(requestLogger); ok {
				rl = lg
				break
			}
		}
	}

	cached := false
	if rl != nil {
		w = rl.StartRequest(w, r, th.targetName(), th.location.Path)
		defer func() {
			rl.FinishRequest(w, r, th.targetName(), th.location.Path, cached)
		}()
	}

	// Run plugin BeforeProxy hooks
	if th.pluginMgr != nil {
		if err := th.pluginMgr.RunBeforeProxy(w, r, th.targetName(), th.location.Path); err != nil {
			return
		}
	}

	// gRPC is proxied on a dedicated streaming passthrough path: it must
	// not be buffered, cached, rewritten, or transformed.
	if isGRPCRequest(r) {
		th.serveGRPC(w, r)
		return
	}

	// Handle CORS preflight (legacy)
	if th.location.CORS != nil && th.location.CORS.Enabled {
		th.handleCORS(w, r)
		if r.Method == http.MethodOptions {
			return
		}
	}

	// Apply request header modifications
	if th.location.Headers != nil {
		th.applyRequestHeaders(r)
	}

	key := disk.CacheKey(th.targetName(), r.URL.RequestURI())

	// Check if this path is excluded from caching (e.g., .m3u8 for live streaming)
	if th.isCacheExcluded(r.URL.Path) {
		th.setNoCacheHeaders(w.Header())
	} else {
		// Check cache
		if th.location.Cache {
			// Apply the location's response header rules before the cached
			// response is written (mutations after the response is committed
			// never reach the client), but only when a fresh entry will really
			// be streamed. On a miss we must not pre-populate w.Header():
			// ReverseProxy normally adds its headers with Add, so a pre-set value
			// plus the proxy copy would duplicate every response header.
			if th.diskCache.Peek(key, th.cacheTTL) {
				th.applyResponseHeadersTo(w.Header())
			}
			if err := th.diskCache.StreamCachedResponseTTL(w, key, th.cacheTTL); err == nil {
				cached = true
				th.diskCache.RecordHit()
				if th.pluginMgr != nil {
					th.pluginMgr.RunRecordCacheHit(th.targetName(), th.location.Path)
					th.pluginMgr.RunAfterCacheHit(w, r, key, th.targetName(), th.location.Path)
				}
				return
			}
			th.diskCache.RecordMiss()
			if th.pluginMgr != nil {
				th.pluginMgr.RunRecordCacheMiss(th.targetName(), th.location.Path)
			}
		}
	}

	// Request coalescing: if another request is already fetching this key,
	// wait for it instead of hitting the upstream again.
	flightOwner := false
	if th.location.Cache && !th.isCacheExcluded(r.URL.Path) {
		flightVal, loaded := th.flightMu.LoadOrStore(key, &singleFlight{ch: make(chan struct{})})
		flight := flightVal.(*singleFlight)

		if !loaded {
			// This is the flight owner. Only the owner performs the upstream
			// fetch, writes the cache, and then releases waiters. The defer
			// guarantees waiters are always unblocked, even if a later step
			// panics. It runs after the cache has been written, so waiters
			// can serve straight from cache instead of re-fetching upstream.
			flightOwner = true
			defer func() {
				if flightVal, ok := th.flightMu.Load(key); ok {
					flight := flightVal.(*singleFlight)
					flight.closeOnce.Do(func() {
						close(flight.ch)
					})
				}
				th.flightMu.Delete(key)
			}()
		} else {
			// Wait for the in-flight request to finish. If it succeeded and
			// wrote the cache, serve the waiter straight from the cached
			// entry; otherwise fall through to fetch the upstream ourselves.
			<-flight.ch
			if th.serveWaiterFromFlight(w, r, key, flight) {
				cached = true
				return
			}
		}
	}

	// Apply rewrite if configured
	path := r.URL.Path
	rawQuery := r.URL.RawQuery
	if th.location.Rewrite != nil && th.location.Rewrite.Pattern != "" {
		if th.rewriter == nil {
			th.rewriter = regexp.MustCompile(th.location.Rewrite.Pattern)
		}
		if th.rewriter != nil {
			newPath := th.rewriter.ReplaceAllString(path, th.location.Rewrite.Replacement)
			if newPath != path {
				path = newPath
				r.URL.Path = path
			}
		}
	}

	// Handle redirect rewrites
	if th.location.Rewrite != nil && th.location.Rewrite.Redirect != "" {
		status := http.StatusMovedPermanently
		if th.location.Rewrite.Redirect == "redirect" {
			status = http.StatusFound
		}
		newPath := path
		if th.rewriter != nil {
			newPath = th.rewriter.ReplaceAllString(path, th.location.Rewrite.Replacement)
		}
		http.Redirect(w, r, newPath+"?"+rawQuery, status)
		return
	}

	upstreamURL := th.getUpstreamURL(r)
	klog.Infof("Upstream URL: %s, path: %s", upstreamURL, path)

	capture := &capturingTransport{
		transport:   proxyTransport,
		lb:          th.lb,
		pinned:      th.pinnedUpstream(),
		maxBodySize: th.resolveMaxBodySize(),
	}

	rp := &httputil.ReverseProxy{
		Rewrite: func(preq *httputil.ProxyRequest) {
			preq.Out.URL.Scheme = upstreamURL[:strings.Index(upstreamURL, "://")]
			preq.Out.URL.Host = upstreamURL[strings.Index(upstreamURL, "://")+3:]
			preq.Out.URL.Path = path
			preq.Out.URL.RawQuery = rawQuery

			if th.target.HostHeader != "" {
				preq.Out.Host = th.target.HostHeader
			} else if th.location.Proxy != nil && th.location.Proxy.PassHostHeader {
				preq.Out.Host = r.Host
			} else {
				preq.Out.Host = preq.Out.URL.Host
			}
			preq.Out.Header.Del("Accept-Encoding")
		},
		Transport: capture,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			klog.Errorf("proxy error for %s: %v", r.URL.Path, err)
			th.logError("ERROR", r, "proxy error", err)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("Bad Gateway\n"))
		},
	}

	// Tag the response with the proxy's own cache status and apply the
	// location's response header rules before the reverse proxy writes the
	// response, so the extra headers actually reach the client.
	rp.ModifyResponse = func(resp *http.Response) error {
		if th.location.Proxy != nil && th.location.Proxy.WebSocket && resp.StatusCode == http.StatusSwitchingProtocols {
			resp.Header.Set("Connection", "upgrade")
			resp.Header.Set("Upgrade", "websocket")
		}
		resp.Header.Set("X-Cache", "MISS")
		th.applyResponseHeadersTo(resp.Header)
		return nil
	}

	// Wrap with compression if configured
	var finalHandler http.Handler = rp
	if th.pluginMgr != nil {
		// Check if compression plugin is available
		for _, p := range th.pluginMgr.GetPlugins() {
			if p.Name() == "compression" {
				if cp, ok := p.(interface {
					WrapHandler(http.Handler) http.Handler
				}); ok {
					klog.Infof("Using compression plugin for location %s", th.location.Path)
					finalHandler = cp.WrapHandler(rp)
					break
				}
			}
		}
	}

	klog.Infof("Serving request with finalHandler: %T", finalHandler)
	finalHandler.ServeHTTP(w, r)
	klog.Infof("After ServeHTTP, statusCode=%d", capture.statusCode)

	// Run plugin AfterProxy hooks
	if th.pluginMgr != nil {
		resp := &http.Response{
			StatusCode: capture.statusCode,
			Header:     capture.headers,
		}
		th.pluginMgr.RunAfterProxy(w, r, th.targetName(), th.location.Path, resp)
	}

	if th.location.Cache && !th.isCacheExcluded(r.URL.Path) && capture.statusCode >= 200 && capture.statusCode < 300 && len(capture.body) > 0 {
		bodyCopy := make([]byte, len(capture.body))
		copy(bodyCopy, capture.body)

		// Run plugin TransformResponseBody hooks (e.g. optimizer) so the
		// cached representation is the optimized one.
		contentType := capture.headers.Get("Content-Type")
		if th.pluginMgr != nil {
			transformed, err := th.pluginMgr.RunTransformResponseBody(th.targetName(), th.location.Path, contentType, bodyCopy)
			if err != nil {
				klog.Warningf("response body transformation failed: %v", err)
				th.logError("WARN", r, "response body transformation failed", err)
			} else {
				if len(transformed) != len(bodyCopy) {
					klog.V(2).Infof("Transformed %s: %d -> %d bytes", contentType, len(bodyCopy), len(transformed))
				}
				bodyCopy = transformed
			}
		}

		// Run plugin BeforeCacheStore hooks
		storeBody := bodyCopy
		shouldCache := true
		if th.pluginMgr != nil {
			transformed, err := th.pluginMgr.RunBeforeCacheStore(key, th.targetName(), th.location.Path, capture.statusCode, capture.headers, storeBody)
			if err != nil {
				klog.Warningf("before cache store failed: %v", err)
				th.logError("WARN", r, "before cache store failed", err)
			} else {
				storeBody = transformed
			}
			shouldCache = th.pluginMgr.RunShouldCache(th.targetName(), th.location.Path, capture.statusCode, capture.headers, storeBody)
			if !shouldCache {
				klog.V(2).Infof("Plugin decided not to cache %s", r.URL.Path)
			}
		}

		if shouldCache {
			write := func() {
				if err := th.diskCache.SetResponseToCache(key, th.targetName(), r.URL.RequestURI(), capture.statusCode, capture.headers, storeBody); err != nil {
					klog.Warningf("failed to cache: %v", err)
					th.logError("WARN", r, "cache write failed", err)
				}
			}
			if th.writeSem == nil {
				// Unlimited write workers (-1): cache without gating.
				write()
			} else {
				select {
				case th.writeSem <- struct{}{}:
					write()
					<-th.writeSem
				default:
					klog.V(2).Infof("write pool full, skipping cache for %s", r.URL.Path)
				}
			}
		}

		// Record the upstream response on the flight so waiters can serve
		// from the freshly written cache once this request returns.
		if flightOwner {
			if flightVal, ok := th.flightMu.Load(key); ok {
				flightVal.(*singleFlight).resp = &http.Response{
					StatusCode: capture.statusCode,
					Header:     capture.headers,
				}
			}
		}
	}
}

// serveWaiterFromFlight serves a request that was waiting on an
// in-progress flight. It returns true only when the flight succeeded
// (resp recorded) and the cached entry could be streamed directly,
// allowing the waiter to be served without touching the upstream.
func (th *TargetHandler) serveWaiterFromFlight(w http.ResponseWriter, r *http.Request, key string, flight *singleFlight) bool {
	if flight.err != nil || flight.resp == nil {
		return false
	}
	th.diskCache.RecordHit()
	if th.pluginMgr != nil {
		th.pluginMgr.RunRecordCacheHit(th.targetName(), th.location.Path)
		th.pluginMgr.RunAfterCacheHit(w, r, key, th.targetName(), th.location.Path)
	}
	th.applyResponseHeadersTo(w.Header())
	if err := th.diskCache.StreamCachedResponseTTL(w, key, th.cacheTTL); err == nil {
		return true
	}
	return false
}

// serveGRPC handles gRPC requests with a streaming reverse proxy. The gRPC
// method path (/pkg.Service/Method) is forwarded verbatim, and the response
// is streamed (FlushInterval -1) so server-streaming and bidi-streaming work.
// Health is updated on transport errors only.
func (th *TargetHandler) serveGRPC(w http.ResponseWriter, r *http.Request) {
	upstreamURL := th.getUpstreamURL(r)
	if upstreamURL == "" {
		th.logError("ERROR", r, "gRPC proxy error", fmt.Errorf("no upstream configured"))
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("Bad Gateway\n"))
		return
	}

	rp := &httputil.ReverseProxy{
		Rewrite: func(preq *httputil.ProxyRequest) {
			preq.Out.URL.Scheme = upstreamURL[:strings.Index(upstreamURL, "://")]
			preq.Out.URL.Host = upstreamURL[strings.Index(upstreamURL, "://")+3:]
			if th.target.HostHeader != "" {
				preq.Out.Host = th.target.HostHeader
			} else if th.location.Proxy != nil && th.location.Proxy.PassHostHeader {
				preq.Out.Host = r.Host
			} else {
				preq.Out.Host = preq.Out.URL.Host
			}
		},
		Transport: &grpcTransport{
			lb:     th.lb,
			pinned: th.pinnedUpstream(),
		},
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			klog.Errorf("gRPC proxy error for %s: %v", r.URL.Path, err)
			th.logError("ERROR", r, "gRPC proxy error", err)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("Bad Gateway\n"))
		},
	}
	rp.ServeHTTP(w, r)
}

func (th *TargetHandler) getUpstreamURL(r *http.Request) string {
	if th.location.Proxy != nil && th.location.Proxy.Upstream != "" {
		return th.location.Proxy.Upstream
	}

	upstreams, _ := th.target.ParseUpstreams()
	if len(upstreams) > 0 {
		return upstreams[0].String()
	}
	return ""
}

// pinnedUpstream returns the location's per-location upstream override, or ""
// when the location does not pin one (in which case the load balancer picks).
func (th *TargetHandler) pinnedUpstream() string {
	if th.location.Proxy != nil && th.location.Proxy.Upstream != "" {
		return th.location.Proxy.Upstream
	}
	return ""
}

func (th *TargetHandler) targetName() string {
	return th.target.ServerName
}

// logError routes an operational error to registered error hooks (e.g.
// the jsonlog plugin). Request details are attached so error and access
// log entries can be correlated by request_id.
func (th *TargetHandler) logError(level string, r *http.Request, msg string, err error) {
	if th.pluginMgr == nil {
		return
	}
	if err != nil {
		msg = fmt.Sprintf("%s: %v", msg, err)
	}
	fields := map[string]any{}
	if r != nil {
		fields["method"] = r.Method
		fields["uri"] = r.URL.RequestURI()
		fields["host"] = r.Host
		fields["remote_addr"] = r.RemoteAddr
	}
	var requestID string
	if r != nil {
		requestID = base.RequestID(r)
	}
	th.pluginMgr.RunLogError(level, th.targetName(), th.location.Path, requestID, msg, fields)
}

func (th *TargetHandler) Location() *config.LocationConfig {
	return th.location
}

func (th *TargetHandler) handleCORS(w http.ResponseWriter, r *http.Request) {
	cors := th.location.CORS

	origin := r.Header.Get("Origin")
	if origin == "" {
		return
	}

	allowed := false
	for _, o := range cors.AllowOrigins {
		if o == "*" || o == origin {
			allowed = true
			break
		}
	}
	if !allowed {
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", origin)

	if cors.AllowCredentials {
		w.Header().Set("Access-Control-Allow-Credentials", "true")
	}

	if len(cors.AllowMethods) > 0 {
		w.Header().Set("Access-Control-Allow-Methods", strings.Join(cors.AllowMethods, ", "))
	} else {
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
	}

	if len(cors.AllowHeaders) > 0 {
		w.Header().Set("Access-Control-Allow-Headers", strings.Join(cors.AllowHeaders, ", "))
	} else {
		reqHeaders := r.Header.Get("Access-Control-Request-Headers")
		if reqHeaders != "" {
			w.Header().Set("Access-Control-Allow-Headers", reqHeaders)
		}
	}

	if len(cors.ExposeHeaders) > 0 {
		w.Header().Set("Access-Control-Expose-Headers", strings.Join(cors.ExposeHeaders, ", "))
	}

	if cors.MaxAge > 0 {
		w.Header().Set("Access-Control-Max-Age", fmt.Sprintf("%d", cors.MaxAge))
	}
}

func (th *TargetHandler) applyRequestHeaders(r *http.Request) {
	headers := th.location.Headers
	if headers.RequestAdd != nil {
		for k, v := range headers.RequestAdd {
			r.Header.Set(k, v)
		}
	}
	for _, k := range headers.RequestRemove {
		r.Header.Del(k)
	}
}

// isCacheExcluded checks if the request path matches any cache exclude pattern.
// Patterns can be exact paths, path prefixes, or file extensions (e.g., ".m3u8").
func (th *TargetHandler) isCacheExcluded(path string) bool {
	if th.location == nil || len(th.location.CacheExcludes) == 0 {
		return false
	}
	for _, pattern := range th.location.CacheExcludes {
		if pattern == "" {
			continue
		}
		// Extension match: ".m3u8" matches "/stream/video.m3u8"
		if strings.HasPrefix(pattern, ".") {
			if strings.HasSuffix(path, pattern) {
				return true
			}
			continue
		}
		// Exact or prefix match
		if pattern == path || strings.HasPrefix(path, strings.TrimSuffix(pattern, "/")) {
			return true
		}
	}
	return false
}

// setNoCacheHeaders adds headers to prevent browser and intermediary caching.
// Used for live streaming manifests (.m3u8) and other dynamic content.
func (th *TargetHandler) setNoCacheHeaders(h http.Header) {
	h.Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
	h.Set("Pragma", "no-cache")
	h.Set("Expires", "0")
}

// applyResponseHeadersTo applies the location's configured response header
// additions and removals to an arbitrary header set. It must run before the
// response headers are committed to the client (via ReverseProxy.ModifyResponse
// for proxied responses, or before StreamCachedResponseTTL for cache hits);
// mutating a ResponseWriter after the response is sent has no effect.
func (th *TargetHandler) applyResponseHeadersTo(h http.Header) {
	headers := th.location.Headers
	if headers == nil {
		return
	}
	if headers.ResponseAdd != nil {
		for k, v := range headers.ResponseAdd {
			h.Set(k, v)
		}
	}
	for _, k := range headers.ResponseRemove {
		h.Del(k)
	}
}
