package handler

import (
	"fmt"
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
