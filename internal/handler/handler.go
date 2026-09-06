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
	statusCode       int
	headers          http.Header
	body             []byte
	selectedUpstream string
	maxBodySize      int64
}

func (ct *capturingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	klog.Infof("capturingTransport.RoundTrip: %s %s", req.Method, req.URL.String())
	upstream := ct.lb.Next(req)
	if upstream == nil {
		return nil, fmt.Errorf("no healthy upstreams")
	}
	ct.selectedUpstream = upstream.URL

	resp, err := ct.transport.RoundTrip(req)
	if err != nil {
		klog.Infof("RoundTrip error: %v", err)
		ct.lb.MarkHealthy(upstream.URL, false)
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
		ct.lb.MarkHealthy(upstream.URL, false)
		return nil, err
	}
	if limit >= 0 && int64(len(body)) > limit {
		ct.lb.MarkHealthy(upstream.URL, false)
		return nil, fmt.Errorf("response body exceeds max read size")
	}

	ct.lb.MarkHealthy(upstream.URL, true)
	ct.body = buf.Bytes()
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
	lb loadbalancer.LoadBalancer
}

// h2cRT is a shared HTTP/2 RoundTripper that dials plaintext upstreams with
// h2c prior-knowledge (no TLS, no Upgrade handshake). Safe for concurrent use.
var h2cRT http.RoundTripper = &http2.Transport{
	AllowHTTP: true,
	DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	},
}

func (gt *grpcTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	upstream := gt.lb.Next(req)
	if upstream == nil {
		return nil, fmt.Errorf("no healthy upstreams")
	}
	var rt http.RoundTripper
	if req.URL.Scheme == "https" {
		rt = http.DefaultTransport
	} else {
		rt = h2cRT
	}
	resp, err := rt.RoundTrip(req)
	if err != nil {
		gt.lb.MarkHealthy(upstream.URL, false)
		return nil, err
	}
	gt.lb.MarkHealthy(upstream.URL, true)
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

	// Check cache
	if th.location.Cache {
		if err := th.diskCache.StreamCachedResponseTTL(w, key, th.cacheTTL); err == nil {
			cached = true
			th.diskCache.RecordHit()
			if th.pluginMgr != nil {
				th.pluginMgr.RunRecordCacheHit(th.targetName(), th.location.Path)
				th.pluginMgr.RunAfterCacheHit(w, r, key, th.targetName(), th.location.Path)
			}
			th.applyResponseHeaders(w)
			return
		}
		th.diskCache.RecordMiss()
		if th.pluginMgr != nil {
			th.pluginMgr.RunRecordCacheMiss(th.targetName(), th.location.Path)
		}
	}

	// Request coalescing: if another request is already fetching this key,
	// wait for it instead of hitting the upstream again.
	flightOwner := false
	if th.location.Cache {
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
		transport:   http.DefaultTransport,
		lb:          th.lb,
		maxBodySize: th.resolveMaxBodySize(),
	}

	rp := &httputil.ReverseProxy{
		Rewrite: func(preq *httputil.ProxyRequest) {
			preq.Out.URL.Scheme = upstreamURL[:strings.Index(upstreamURL, "://")]
			preq.Out.URL.Host = upstreamURL[strings.Index(upstreamURL, "://")+3:]
			preq.Out.URL.Path = path
			preq.Out.URL.RawQuery = rawQuery

			if th.location.Proxy != nil && th.location.Proxy.PassHostHeader {
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

	if th.location.Proxy != nil && th.location.Proxy.WebSocket {
		rp.ModifyResponse = func(resp *http.Response) error {
			if resp.StatusCode == http.StatusSwitchingProtocols {
				resp.Header.Set("Connection", "upgrade")
				resp.Header.Set("Upgrade", "websocket")
			}
			return nil
		}
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

	// Apply response header modifications
	th.applyResponseHeaders(w)

	// Run plugin AfterProxy hooks
	if th.pluginMgr != nil {
		resp := &http.Response{
			StatusCode: capture.statusCode,
			Header:     capture.headers,
		}
		th.pluginMgr.RunAfterProxy(w, r, th.targetName(), th.location.Path, resp)
	}

	if th.location.Cache && capture.statusCode >= 200 && capture.statusCode < 300 && len(capture.body) > 0 {
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
	th.applyResponseHeaders(w)
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
			if th.location.Proxy != nil && th.location.Proxy.PassHostHeader {
				preq.Out.Host = r.Host
			} else {
				preq.Out.Host = preq.Out.URL.Host
			}
		},
		Transport: &grpcTransport{
			lb: th.lb,
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

func (th *TargetHandler) targetName() string {
	return th.target.Name
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

func (th *TargetHandler) applyResponseHeaders(w http.ResponseWriter) {
	headers := th.location.Headers
	if headers == nil {
		return
	}
	if headers.ResponseAdd != nil {
		for k, v := range headers.ResponseAdd {
			w.Header().Set(k, v)
		}
	}
	for _, k := range headers.ResponseRemove {
		w.Header().Del(k)
	}
}
