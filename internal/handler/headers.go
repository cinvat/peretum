package handler

import (
	"fmt"
	"net/http"
	"strings"
)

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
