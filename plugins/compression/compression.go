package compression

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/cinvat/peretum/plugins/base"
	"k8s.io/klog/v2"
)

type CompressionPlugin struct {
	*base.BasePlugin
	config CompressionConfig
	pool   sync.Pool
}

type CompressionConfig struct {
	Enabled   bool
	Level     int
	MinLength int
	Types     []string
	typeMap   map[string]bool
}

func NewCompressionPlugin() *CompressionPlugin {
	return &CompressionPlugin{
		BasePlugin: base.NewBasePlugin("compression"),
	}
}

func (p *CompressionPlugin) Init(config map[string]any) error {
	p.BasePlugin.Init(config)

	p.config.Enabled = config["enabled"].(bool)
	p.config.Level = config["level"].(int)
	p.config.MinLength = config["min_length"].(int)
	p.config.Types = config["types"].([]string)

	p.config.typeMap = make(map[string]bool)

	defaultTypes := []string{
		"text/html", "text/css", "text/javascript", "application/javascript",
		"application/json", "application/xml", "text/xml", "text/plain",
		"application/wasm", "image/svg+xml",
	}
	for _, t := range defaultTypes {
		p.config.typeMap[t] = true
	}
	for _, t := range p.config.Types {
		p.config.typeMap[strings.ToLower(strings.TrimSpace(t))] = true
	}

	p.pool = sync.Pool{
		New: func() any {
			w, _ := gzip.NewWriterLevel(&bytes.Buffer{}, p.config.Level)
			return w
		},
	}

	return nil
}

func (p *CompressionPlugin) Start(ctx context.Context) error { return nil }
func (p *CompressionPlugin) Stop(ctx context.Context) error  { return nil }

// closeGzipWriter is a test seam so the wrap handler's error branch can be
// exercised without simulating impossible underlying writer failures.
var closeGzipWriter = func(gz *gzip.Writer) error { return gz.Close() }

type gzipResponseWriter struct {
	http.ResponseWriter
	writer       *gzip.Writer
	buf          *bytes.Buffer
	raw          *bytes.Buffer
	status       int
	wroteHeader  bool
	originalSize int64
	contentType  string
}

func (p *CompressionPlugin) BeforeProxy(w http.ResponseWriter, r *http.Request, target, location string) error {
	if !p.config.Enabled {
		return nil
	}
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		return nil
	}

	gz := p.pool.Get().(*gzip.Writer)

	gzw := &gzipResponseWriter{
		ResponseWriter: w,
		writer:         gz,
		buf:            &bytes.Buffer{},
	}
	gz.Reset(gzw.buf)

	return nil
}

func (p *CompressionPlugin) AfterProxy(w http.ResponseWriter, r *http.Request, target, location string, resp *http.Response) error {
	return nil
}

func (p *CompressionPlugin) WrapHandler(h http.Handler) http.Handler {
	if !p.config.Enabled {
		return h
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			h.ServeHTTP(w, r)
			return
		}

		gz := p.pool.Get().(*gzip.Writer)
		defer p.pool.Put(gz)

		raw := &bytes.Buffer{}
		gzw := &gzipResponseWriter{
			ResponseWriter: w,
			writer:         gz,
			buf:            &bytes.Buffer{},
			raw:            raw,
		}
		gz.Reset(gzw.buf)

		h.ServeHTTP(gzw, r)

		// Flush the gzip trailer into the buffer so the compressed bytes
		// are readable before deciding whether compression applies.
		if err := closeGzipWriter(gz); err != nil {
			klog.Warningf("compression: failed to finalize gzip stream: %v", err)
		}

		if gzw.status != 0 && gzw.wroteHeader && p.shouldCompress(gzw.contentType, gzw.originalSize) {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Vary", "Accept-Encoding")
			w.Header().Del("Content-Length")
			w.WriteHeader(gzw.status)
			_, _ = w.Write(gzw.buf.Bytes())
			return
		}

		// Payload is not worth compressing: serve the original bytes.
		w.WriteHeader(gzw.status)
		_, _ = w.Write(raw.Bytes())
	})
}

func (p *CompressionPlugin) shouldCompress(contentType string, bodyLen int64) bool {
	if bodyLen < int64(p.config.MinLength) {
		return false
	}
	ct := strings.ToLower(strings.TrimSpace(contentType))
	ct = strings.Split(ct, ";")[0]
	return p.config.typeMap[ct]
}

func (grw *gzipResponseWriter) WriteHeader(status int) {
	grw.status = status
	grw.wroteHeader = true
	grw.contentType = grw.ResponseWriter.Header().Get("Content-Type")
}

func (grw *gzipResponseWriter) Write(data []byte) (int, error) {
	if !grw.wroteHeader {
		grw.WriteHeader(http.StatusOK)
	}
	grw.originalSize += int64(len(data))
	if grw.raw != nil {
		grw.raw.Write(data)
	}
	return grw.writer.Write(data)
}

func (grw *gzipResponseWriter) Flush() {
	if grw.writer != nil {
		grw.writer.Flush()
	}
	if f, ok := grw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (grw *gzipResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := grw.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}
