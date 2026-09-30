package handler

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/cinvat/peretum/internal/loadbalancer"
	"golang.org/x/net/http2"
	"k8s.io/klog/v2"
)

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
