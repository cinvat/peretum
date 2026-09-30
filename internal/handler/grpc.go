package handler

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"strings"

	"k8s.io/klog/v2"
)

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
