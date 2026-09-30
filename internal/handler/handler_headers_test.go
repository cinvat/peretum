package handler

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cinvat/peretum/internal/config"
	"github.com/cinvat/peretum/internal/plugin/manager"
	"github.com/cinvat/peretum/plugins/base"
)

func TestServeHTTP_CORS(t *testing.T) {
	full := &config.LocationConfig{
		Path: "/",
		CORS: &config.CORSConfig{
			Enabled:          true,
			AllowOrigins:     []string{"*"},
			AllowMethods:     []string{"GET"},
			AllowHeaders:     []string{"X-Custom"},
			ExposeHeaders:    []string{"X-Expose"},
			AllowCredentials: true,
			MaxAge:           10,
		},
	}
	upURL, _ := newUpstream(t)
	th, _ := setupHandler(t, fixture{upstreamURL: upURL, location: full})

	t.Run("preflight all branches", func(t *testing.T) {
		req := httptest.NewRequest("OPTIONS", "http://example.com/api", nil)
		req.Header.Set("Origin", "http://client")
		req.Header.Set("Access-Control-Request-Headers", "X-Anything")
		rec := httptest.NewRecorder()
		th.ServeHTTP(rec, req)

		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://client" {
			t.Errorf("allow-origin=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET" {
			t.Errorf("allow-methods=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "X-Custom" {
			t.Errorf("allow-headers=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Expose-Headers"); got != "X-Expose" {
			t.Errorf("expose-headers=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
			t.Errorf("credentials=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Max-Age"); got != "10" {
			t.Errorf("max-age=%q", got)
		}
	})

	t.Run("preflight default branches", func(t *testing.T) {
		def := upURL + ""
		thd, _ := setupHandler(t, fixture{
			upstreamURL: def,
			location: &config.LocationConfig{
				Path: "/",
				CORS: &config.CORSConfig{Enabled: true, AllowOrigins: []string{"http://good"}},
			},
		})
		req := httptest.NewRequest("OPTIONS", "http://example.com/api", nil)
		req.Header.Set("Origin", "http://good")
		req.Header.Set("Access-Control-Request-Headers", "X-A, X-B")
		rec := httptest.NewRecorder()
		thd.ServeHTTP(rec, req)

		if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "GET, POST, PUT, DELETE, OPTIONS, HEAD" {
			t.Errorf("default methods=%q", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Headers"); got != "X-A, X-B" {
			t.Errorf("echoed headers=%q", got)
		}
		if rec.Header().Get("Access-Control-Allow-Credentials") != "" {
			t.Error("no credentials expected")
		}
		if rec.Header().Get("Access-Control-Max-Age") != "" {
			t.Error("no max-age expected")
		}
	})

	t.Run("non-OPTIONS continues to origin", func(t *testing.T) {
		req := httptest.NewRequest("GET", "http://example.com/g", nil)
		req.Header.Set("Origin", "http://client")
		rec := httptest.NewRecorder()
		th.ServeHTTP(rec, req)
		if rec.Body.String() != "UPSTREAM:/g" {
			t.Errorf("body=%q", rec.Body.String())
		}
		if rec.Header().Get("Access-Control-Allow-Origin") != "http://client" {
			t.Errorf("allow-origin=%q", rec.Header().Get("Access-Control-Allow-Origin"))
		}
	})

	t.Run("no origin", func(t *testing.T) {
		req := httptest.NewRequest("OPTIONS", "http://example.com/api", nil)
		rec := httptest.NewRecorder()
		th.ServeHTTP(rec, req)
		if rec.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Error("no CORS headers without Origin")
		}
	})

	t.Run("disallowed origin", func(t *testing.T) {
		thd, _ := setupHandler(t, fixture{
			location: &config.LocationConfig{
				Path: "/",
				CORS: &config.CORSConfig{Enabled: true, AllowOrigins: []string{"http://good"}},
			},
		})
		req := httptest.NewRequest("OPTIONS", "http://example.com/api", nil)
		req.Header.Set("Origin", "http://evil")
		rec := httptest.NewRecorder()
		thd.ServeHTTP(rec, req)
		if rec.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Error("origin should not be allowed")
		}
	})
}

func TestServeHTTP_CORS_Disabled(t *testing.T) {
	upURL, _ := newUpstream(t)
	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location:    &config.LocationConfig{Path: "/", CORS: &config.CORSConfig{Enabled: false}},
	})
	req := httptest.NewRequest("OPTIONS", "http://example.com/api", nil)
	req.Header.Set("Origin", "http://client")
	rec := httptest.NewRecorder()
	th.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Error("CORS disabled but headers set")
	}
}

// --- headers --------------------------------------------------------------

func TestServeHTTP_Rewrite(t *testing.T) {
	upURL, state := newUpstream(t)
	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location: &config.LocationConfig{
			Path: "/",
			Rewrite: &config.RewriteConfig{
				Pattern:     "^/old/(.*)$",
				Replacement: "/new/$1",
			},
		},
	})
	rec := doRequest(th, "GET", "http://example.com/old/abc?q=1")
	if rec.Body.String() != "UPSTREAM:/new/abc" {
		t.Errorf("body=%q", rec.Body.String())
	}
	if state.requests[0].Path != "/new/abc" {
		t.Errorf("upstream path=%q", state.requests[0].Path)
	}
	if state.requests[0].Query != "q=1" {
		t.Errorf("upstream query=%q", state.requests[0].Query)
	}
}

func TestServeHTTP_Rewrite_LazyCompile(t *testing.T) {
	upURL, state := newUpstream(t)
	th, _ := setupHandler(t, fixture{
		upstreamURL: upURL,
		location: &config.LocationConfig{
			Path: "/",
			Rewrite: &config.RewriteConfig{
				Pattern:     "^/old/(.*)$",
				Replacement: "/new/$1",
			},
		},
		direct: true, // rewriter must be compiled on first request
	})
	rec := doRequest(th, "GET", "http://example.com/old/z")
	if state.requests[0].Path != "/new/z" {
		t.Errorf("upstream path=%q", state.requests[0].Path)
	}
	if rec.Body.String() != "UPSTREAM:/new/z" {
		t.Errorf("body=%q", rec.Body.String())
	}
}

func TestServeHTTP_Redirect(t *testing.T) {
	t.Run("redirect=302", func(t *testing.T) {
		upURL, _ := newUpstream(t)
		th, _ := setupHandler(t, fixture{
			upstreamURL: upURL,
			location: &config.LocationConfig{
				Path: "/",
				Rewrite: &config.RewriteConfig{
					Pattern:     "^/old/(.*)$",
					Replacement: "/new/$1",
					Redirect:    "redirect",
				},
			},
		})
		rec := doRequest(th, "GET", "http://example.com/old/abc?q=1")
		if rec.Code != http.StatusFound {
			t.Errorf("code=%d, want 302", rec.Code)
		}
		if got := rec.Header().Get("Location"); got != "/new/abc?q=1" {
			t.Errorf("location=%q", got)
		}
	})

	t.Run("redirect=permanent", func(t *testing.T) {
		upURL, _ := newUpstream(t)
		th, _ := setupHandler(t, fixture{
			upstreamURL: upURL,
			location: &config.LocationConfig{
				Path: "/",
				Rewrite: &config.RewriteConfig{
					Pattern:     "^/old/(.*)$",
					Replacement: "/new/$1",
					Redirect:    "permanent",
				},
			},
		})
		rec := doRequest(th, "GET", "http://example.com/old/abc?q=1")
		if rec.Code != http.StatusMovedPermanently {
			t.Errorf("code=%d, want 301", rec.Code)
		}
	})

	t.Run("no rewriter", func(t *testing.T) {
		upURL, _ := newUpstream(t)
		th, _ := setupHandler(t, fixture{
			upstreamURL: upURL,
			location: &config.LocationConfig{
				Path: "/",
				Rewrite: &config.RewriteConfig{
					Redirect: "redirect",
				},
			},
			direct: true,
		})
		rec := doRequest(th, "GET", "http://example.com/same?q=1")
		if rec.Code != http.StatusFound {
			t.Errorf("code=%d", rec.Code)
		}
		if got := rec.Header().Get("Location"); got != "/same?q=1" {
			t.Errorf("location=%q", got)
		}
	})
}

// --- websocket ------------------------------------------------------------

func TestServeHTTP_WebSocket(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Respond 101 then immediately close the upgraded connection so
		// the proxy's body reader sees EOF.
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		fmt.Fprint(buf, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		buf.Flush()
		conn.Close()
	}))
	t.Cleanup(srv.Close)

	stub := mkStub()
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(stub)

	th, _ := setupHandler(t, fixture{
		upstreamURL: srv.URL,
		location: &config.LocationConfig{
			Path:  "/",
			Proxy: &config.ProxyLocationConfig{WebSocket: true},
		},
		pluginMgr: pm,
	})

	req := httptest.NewRequest("GET", "http://example.com/ws", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	rec := httptest.NewRecorder()
	th.ServeHTTP(rec, req)

	// Recorder can't hijack, so the 101 upgrade fails -> 502 via handler.
	if rec.Code != http.StatusBadGateway {
		t.Errorf("code=%d, want 502", rec.Code)
	}
	if len(stub.logged) == 0 {
		t.Error("expected a logged proxy error for failed websocket upgrade")
	}
}

// --- compression wrap -----------------------------------------------------

func TestServeHTTP_CompressionWrap(t *testing.T) {
	upURL, _ := newUpstream(t)
	wrap := &wrapPlugin{BasePlugin: base.NewBasePlugin("compression")}
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(wrap)

	th, _ := setupHandler(t, fixture{upstreamURL: upURL, pluginMgr: pm})
	rec := doRequest(th, "GET", "http://example.com/c")

	if rec.Header().Get("X-Wrapped") != "yes" {
		t.Error("handler was not wrapped")
	}
	if wrap.calls != 1 {
		t.Errorf("wraps=%d", wrap.calls)
	}
	if rec.Body.String() != "UPSTREAM:/c" {
		t.Errorf("body=%q", rec.Body.String())
	}
}

func TestServeHTTP_CompressionNoWrapper(t *testing.T) {
	upURL, _ := newUpstream(t)
	pm := manager.NewPluginManager()
	pm.RegisterPlugin(&plainPlugin{BasePlugin: base.NewBasePlugin("compression")})

	th, _ := setupHandler(t, fixture{upstreamURL: upURL, pluginMgr: pm})
	rec := doRequest(th, "GET", "http://example.com/c")
	if rec.Header().Get("X-Wrapped") != "" {
		t.Error("unexpected wrap")
	}
	if rec.Body.String() != "UPSTREAM:/c" {
		t.Errorf("body=%q", rec.Body.String())
	}
}
