// Package errorpage renders branded HTML error pages (Cloudflare-style)
// for client-facing errors (404, 5xx, ...) instead of the plain-text
// bodies produced by the router and reverse proxy.
//
// The plugin wraps the server's top-level router via the base.RouterWrapper
// contract so it can intercept every response, including router 404s that
// never reach a target handler. Each rendered page embeds the request ID
// and the time the page was served so users and operators can correlate a
// failing request across logs.
package errorpage

import (
	"bufio"
	_ "embed"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cinvat/peretum/plugins/base"
)

//go:embed logo.svg
var logoSVG string

const pluginName = "error_page"

var defaultStatuses = []int{
	http.StatusNotFound,
	http.StatusInternalServerError,
	http.StatusBadGateway,
	http.StatusServiceUnavailable,
	http.StatusGatewayTimeout,
}

// ErrorPagePlugin renders branded error pages for a configured set of
// status codes. It implements base.RouterWrapper.
type ErrorPagePlugin struct {
	*base.BasePlugin
	enabled  bool
	statuses map[int]bool
	hostname string
}

func NewErrorPagePlugin() *ErrorPagePlugin {
	host, _ := os.Hostname()
	return &ErrorPagePlugin{
		BasePlugin: base.NewBasePlugin(pluginName),
		statuses:   make(map[int]bool),
		hostname:   host,
	}
}

func (p *ErrorPagePlugin) Init(config map[string]any) error {
	p.BasePlugin.Init(config)
	p.enabled = base.GetBool(config, "enabled")
	p.statuses = make(map[int]bool)
	for _, s := range parseStatuses(config["statuses"]) {
		p.statuses[s] = true
	}
	if len(p.statuses) == 0 {
		for _, s := range defaultStatuses {
			p.statuses[s] = true
		}
	}
	return nil
}

// WrapRouter wraps the frontend router so error responses are replaced
// with branded HTML pages. When disabled it returns the handler untouched.
func (p *ErrorPagePlugin) WrapRouter(h http.Handler) http.Handler {
	if !p.enabled {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = p.ensureRequestID(r)
		ew := &errorPageWriter{ResponseWriter: w, plugin: p, request: r}
		h.ServeHTTP(ew, r)
		ew.finish()
	})
}

// ensureRequestID attaches a stable request ID and exposes it as
// X-Request-ID so the reverse proxy and error page agree on the same ID.
func (p *ErrorPagePlugin) ensureRequestID(r *http.Request) *http.Request {
	if base.RequestID(r) != "" {
		return r
	}
	nr, id := base.EnsureRequestID(r)
	nr.Header.Set("X-Request-ID", id)
	return nr
}

func (p *ErrorPagePlugin) shouldRender(code int) bool {
	return p.statuses[code]
}

func (p *ErrorPagePlugin) requestID(r *http.Request) string {
	if id := base.RequestID(r); id != "" {
		return id
	}
	if id := r.Header.Get("X-Request-ID"); id != "" {
		return id
	}
	return "-"
}

// errorPageWriter replaces the body of configured error responses with a
// branded HTML page. Successful responses pass straight through.
type errorPageWriter struct {
	http.ResponseWriter
	plugin      *ErrorPagePlugin
	request     *http.Request
	status      int
	wroteHeader bool
	render      bool
	done        bool
}

func (ew *errorPageWriter) WriteHeader(code int) {
	if ew.wroteHeader {
		return
	}
	ew.wroteHeader = true
	ew.status = code
	if ew.plugin.shouldRender(code) {
		ew.render = true
		return
	}
	ew.ResponseWriter.WriteHeader(code)
}

func (ew *errorPageWriter) Write(b []byte) (int, error) {
	if !ew.wroteHeader {
		ew.WriteHeader(http.StatusOK)
	}
	if ew.render {
		ew.writePage()
		return len(b), nil
	}
	return ew.ResponseWriter.Write(b)
}

// finish renders the error page for handlers that wrote a header but no
// body (e.g. `w.WriteHeader(404)` with nothing else).
func (ew *errorPageWriter) finish() {
	if !ew.wroteHeader {
		return
	}
	if ew.render {
		ew.writePage()
	}
}

func (ew *errorPageWriter) writePage() {
	if ew.done {
		return
	}
	ew.done = true
	hdr := ew.Header()
	hdr.Set("Content-Type", "text/html; charset=utf-8")
	hdr.Del("Content-Length")
	hdr.Del("Content-Encoding")
	hdr.Set("X-Request-ID", ew.plugin.requestID(ew.request))
	hdr.Set("Cache-Control", "no-store")
	ew.ResponseWriter.WriteHeader(ew.status)
	if ew.request != nil && ew.request.Method == http.MethodHead {
		return
	}
	_, _ = ew.ResponseWriter.Write(ew.plugin.renderPage(ew.status, ew.request))
}

func (ew *errorPageWriter) Flush() {
	if ew.render {
		ew.writePage()
		return
	}
	if f, ok := ew.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (ew *errorPageWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := ew.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (ew *errorPageWriter) Unwrap() http.ResponseWriter {
	return ew.ResponseWriter
}

// parseStatuses normalizes a statuses config value that may arrive as a
// []int (from buildPluginConfigs) or a []any of numbers (from YAML).
func parseStatuses(v any) []int {
	switch val := v.(type) {
	case nil:
		return nil
	case []int:
		return val
	case []any:
		out := make([]int, 0, len(val))
		for _, item := range val {
			switch n := item.(type) {
			case int:
				out = append(out, n)
			case int64:
				out = append(out, int(n))
			case float64:
				out = append(out, int(n))
			}
		}
		return out
	default:
		return nil
	}
}

func messageForStatus(code int) string {
	switch code {
	case http.StatusBadRequest:
		return "The server could not understand your request."
	case http.StatusForbidden:
		return "You do not have permission to access the requested page."
	case http.StatusNotFound:
		return "The page you requested could not be found on this server."
	case http.StatusInternalServerError:
		return "An unexpected error occurred while processing your request."
	case http.StatusBadGateway:
		return "The upstream server returned an invalid or incomplete response."
	case http.StatusServiceUnavailable:
		return "The service is temporarily unavailable. Please try again shortly."
	case http.StatusGatewayTimeout:
		return "The upstream server did not respond in time. Please try again shortly."
	default:
		return "An error occurred while processing your request."
	}
}

// renderPage builds the error page HTML. All interpolated values are
// escaped so attacker-influenced request data cannot inject markup.
func (p *ErrorPagePlugin) renderPage(status int, r *http.Request) []byte {
	code := strconv.Itoa(status)
	statusText := http.StatusText(status)
	if statusText == "" {
		statusText = "Error"
	}
	msg := messageForStatus(status)

	var reqID, path, method, remote string
	if r != nil {
		reqID = escape(p.requestID(r))
		path = escape(r.URL.Path)
		method = escape(r.Method)
		remote = escape(hostOnly(r.RemoteAddr))
	} else {
		reqID = "-"
	}
	now := time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
	host := escape(p.hostname)

	page := errorPageTemplate
	page = strings.ReplaceAll(page, "{{LOGO}}", logoSVG)
	page = strings.ReplaceAll(page, "{{CODE}}", code)
	page = strings.ReplaceAll(page, "{{STATUS}}", escape(statusText))
	page = strings.ReplaceAll(page, "{{MESSAGE}}", escape(msg))
	page = strings.ReplaceAll(page, "{{REQUEST_ID}}", reqID)
	page = strings.ReplaceAll(page, "{{TIME}}", now)
	page = strings.ReplaceAll(page, "{{HOST}}", host)
	page = strings.ReplaceAll(page, "{{METHOD}}", method)
	page = strings.ReplaceAll(page, "{{PATH}}", path)
	page = strings.ReplaceAll(page, "{{REMOTE}}", remote)
	return []byte(page)
}

func escape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&quot;")
	s = strings.ReplaceAll(s, "'", "&#39;")
	return s
}

func hostOnly(remoteAddr string) string {
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	if remoteAddr == "" {
		return "-"
	}
	return strings.TrimSpace(remoteAddr)
}

const errorPageTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>{{CODE}} {{STATUS}}</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
      "Helvetica Neue", Arial, sans-serif;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    background: radial-gradient(1200px 800px at 50% -10%, #1b2a4a 0%, #0f172a 55%, #0b1222 100%);
    color: #e2e8f0;
    padding: 24px;
  }
  .card {
    max-width: 560px;
    width: 100%;
    text-align: center;
    padding: 48px 40px;
    background: rgba(15, 23, 42, 0.72);
    border: 1px solid rgba(148, 163, 184, 0.16);
    border-radius: 20px;
    backdrop-filter: blur(8px);
    box-shadow: 0 30px 60px rgba(2, 6, 23, 0.55);
  }
  .logo { margin: 0 auto 28px; width: 84px; height: 84px; }
  .logo svg { width: 100%; height: 100%; color: #7dd3fc; }
  .code {
    font-size: 88px;
    font-weight: 800;
    line-height: 1;
    letter-spacing: 2px;
    color: #0ea5e9;
    text-shadow: 0 0 32px rgba(14, 165, 233, 0.4);
  }
  .status {
    margin-top: 8px;
    font-size: 22px;
    font-weight: 600;
    letter-spacing: 4px;
    text-transform: uppercase;
    color: #cbd5e1;
  }
  .message {
    margin-top: 16px;
    font-size: 15px;
    line-height: 1.7;
    color: #94a3b8;
  }
  .request {
    margin-top: 18px;
    font-size: 13px;
    color: #64748b;
  }
  .request code {
    color: #7dd3fc;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    background: rgba(125, 211, 252, 0.08);
    padding: 3px 8px;
    border-radius: 6px;
  }
  .meta {
    margin-top: 28px;
    padding-top: 20px;
    border-top: 1px solid rgba(148, 163, 184, 0.14);
    display: flex;
    flex-wrap: wrap;
    justify-content: center;
    gap: 8px 18px;
    font-size: 12px;
    color: #64748b;
  }
  .meta span { white-space: nowrap; }
  .brand {
    margin-top: 22px;
    font-size: 12px;
    letter-spacing: 5px;
    text-transform: uppercase;
    color: #3b4a63;
  }
  @media (max-width: 520px) {
    .card { padding: 36px 20px; }
    .code { font-size: 64px; }
  }
</style>
</head>
<body>
  <div class="card">
    <div class="logo">{{LOGO}}</div>
    <div class="code">{{CODE}}</div>
    <div class="status">{{STATUS}}</div>
    <p class="message">{{MESSAGE}}</p>
    <div class="request">Request ID: <code>{{REQUEST_ID}}</code></div>
    <div class="meta">
      <span>Time: {{TIME}}</span>
      <span>Host: {{HOST}}</span>
      <span>Method: {{METHOD}}</span>
      <span>Path: {{PATH}}</span>
      <span>Client: {{REMOTE}}</span>
    </div>
    <div class="brand">Peretum Edge Gateway</div>
  </div>
</body>
</html>
`
