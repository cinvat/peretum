// Package jsonlog provides nginx-style access and error logging that
// emits JSON Lines (one compact JSON object per line) so the output can
// be tailed by Fluent Bit and shipped to Elasticsearch / VictoriaLogs.
//
// The handler discovers StartRequest/FinishRequest via structural typing:
// every request (including cache hits) is wrapped at the top of
// ServeHTTP, traced to completion, and emitted as a single access log
// line. Errors are routed through the base.ErrorHook interface.
package jsonlog

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cinvat/peretum/plugins/base"
	"k8s.io/klog/v2"
)

const (
	defaultAccessPath = "access.jsonl"
	defaultErrorPath  = "error.jsonl"
	bufferSize        = 64 * 1024
	stdoutPath        = "-"
)

type JSONLogPlugin struct {
	*base.BasePlugin

	mu         sync.Mutex
	enabled    bool
	accessPath string
	errorPath  string
	stdout     bool

	accessFile *os.File
	errorFile  *os.File
	accessBuf  *bufio.Writer
	errorBuf   *bufio.Writer
	started    bool
}

func NewJSONLogPlugin() *JSONLogPlugin {
	return &JSONLogPlugin{
		BasePlugin: base.NewBasePlugin("jsonlog"),
	}
}

func (p *JSONLogPlugin) Init(config map[string]any) error {
	p.BasePlugin.Init(config)
	p.enabled = base.GetBool(config, "enabled")
	p.accessPath = base.GetString(config, "access_log")
	p.errorPath = base.GetString(config, "error_log")
	p.stdout = base.GetBool(config, "stdout")
	if p.accessPath == "" {
		p.accessPath = defaultAccessPath
	}
	if p.errorPath == "" {
		p.errorPath = defaultErrorPath
	}
	return nil
}

func (p *JSONLogPlugin) Start(ctx context.Context) error {
	if !p.enabled {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.accessBuf == nil {
		f, err := openLogFile(p.accessPath)
		if err != nil {
			return fmt.Errorf("access log: %w", err)
		}
		p.accessFile = f
		p.accessBuf = bufio.NewWriterSize(f, bufferSize)
	}
	if p.errorBuf == nil {
		f, err := openLogFile(p.errorPath)
		if err != nil {
			return fmt.Errorf("error log: %w", err)
		}
		p.errorFile = f
		p.errorBuf = bufio.NewWriterSize(f, bufferSize)
	}
	p.started = true
	return nil
}

func (p *JSONLogPlugin) Stop(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.accessBuf != nil {
		_ = p.accessBuf.Flush()
		p.accessBuf = nil
	}
	if p.errorBuf != nil {
		_ = p.errorBuf.Flush()
		p.errorBuf = nil
	}
	if p.accessFile != nil && p.accessFile != os.Stdout {
		_ = p.accessFile.Close()
		p.accessFile = nil
	}
	if p.errorFile != nil && p.errorFile != os.Stdout {
		_ = p.errorFile.Close()
		p.errorFile = nil
	}
	p.started = false
	return nil
}

// Reopen closes and reopens the underlying log files. It is called on
// SIGHUP so an external logrotate can rename the files between
// invocations. Writers are only swapped when the output is a real file;
// stdout destinations are left untouched.
func (p *JSONLogPlugin) Reopen() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.enabled {
		return nil
	}
	if p.accessBuf != nil {
		_ = p.accessBuf.Flush()
	}
	if p.errorBuf != nil {
		_ = p.errorBuf.Flush()
	}

	if p.accessPath != "" && p.accessPath != stdoutPath {
		f, err := openLogFile(p.accessPath)
		if err != nil {
			return fmt.Errorf("access log reopen: %w", err)
		}
		old := p.accessFile
		p.accessFile = f
		p.accessBuf = bufio.NewWriterSize(f, bufferSize)
		if old != nil {
			_ = old.Close()
		}
	}
	if p.errorPath != "" && p.errorPath != stdoutPath {
		f, err := openLogFile(p.errorPath)
		if err != nil {
			return fmt.Errorf("error log reopen: %w", err)
		}
		old := p.errorFile
		p.errorFile = f
		p.errorBuf = bufio.NewWriterSize(f, bufferSize)
		if old != nil {
			_ = old.Close()
		}
	}
	return nil
}

// StartRequest wraps the response writer so status code, bytes written,
// and timing can be captured for the access log. The wrapper is passed
// back into FinishRequest so the counters can be read out safely.
func (p *JSONLogPlugin) StartRequest(w http.ResponseWriter, r *http.Request, target, location string) http.ResponseWriter {
	if !p.enabled {
		return w
	}
	return &accessRecorder{
		ResponseWriter: w,
		requestID:      base.RequestID(r),
		target:         target,
		location:       location,
		start:          time.Now(),
		request:        r,
	}
}

func (p *JSONLogPlugin) FinishRequest(w http.ResponseWriter, r *http.Request, target, location string, cached bool) {
	if !p.enabled {
		return
	}
	rec, ok := w.(*accessRecorder)
	if !ok {
		return
	}

	cacheStatus := "MISS"
	if cached {
		cacheStatus = "HIT"
	}

	entry := rec.entry()
	entry["level"] = "INFO"
	entry["type"] = "access"
	entry["time"] = time.Now().UTC().Format(time.RFC3339Nano)
	entry["status"] = rec.statusCode()
	entry["bytes_sent"] = rec.bytes()
	entry["request_time"] = time.Since(rec.start).Seconds()
	entry["cache"] = cacheStatus
	entry["content_type"] = rec.Header().Get("Content-Type")

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := emit(p.accessBuf, p.stdout, entry); err != nil {
		klog.Warningf("jsonlog: access write failed: %v", err)
	}
}

func (p *JSONLogPlugin) LogError(level, target, location, requestID, msg string, fields map[string]any) {
	if !p.enabled {
		return
	}
	entry := map[string]any{
		"level":    level,
		"type":     "error",
		"time":     time.Now().UTC().Format(time.RFC3339Nano),
		"msg":      msg,
		"target":   target,
		"location": location,
	}
	if requestID != "" {
		entry["request_id"] = requestID
	}
	for k, v := range fields {
		entry[k] = v
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := emit(p.errorBuf, p.stdout, entry); err != nil {
		klog.Warningf("jsonlog: error write failed: %v", err)
	}
}

// emit serializes data as a compact JSON object terminated by a newline
// and flushes it so tailers observe complete lines immediately.
func emit(buf *bufio.Writer, mirror bool, data any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	b = append(b, '\n')

	if buf != nil {
		if _, err := buf.Write(b); err != nil {
			return err
		}
		if err := buf.Flush(); err != nil {
			return err
		}
	}
	if mirror {
		_, err := os.Stdout.Write(b)
		return err
	}
	return nil
}

func openLogFile(path string) (*os.File, error) {
	if path == stdoutPath {
		return os.Stdout, nil
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

// accessRecorder tracks status, bytes, and timing for a single request.
// Optional interfaces are forwarded to the underlying ResponseWriter so
// streaming (SSE), websocket upgrades, and http.Pusher keep working.
type accessRecorder struct {
	http.ResponseWriter
	request   *http.Request
	requestID string
	target    string
	location  string
	start     time.Time

	mu          sync.Mutex
	status      int
	bytesW      int64
	wroteHeader bool
}

func (rec *accessRecorder) statusCode() int {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.status == 0 {
		return http.StatusOK
	}
	return rec.status
}

func (rec *accessRecorder) bytes() int64 {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.bytesW
}

func (rec *accessRecorder) WriteHeader(code int) {
	rec.mu.Lock()
	if rec.wroteHeader {
		rec.mu.Unlock()
		return
	}
	rec.status = code
	rec.wroteHeader = true
	rec.mu.Unlock()
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *accessRecorder) Write(b []byte) (int, error) {
	rec.mu.Lock()
	if !rec.wroteHeader {
		rec.status = http.StatusOK
		rec.wroteHeader = true
	}
	rec.mu.Unlock()

	n, err := rec.ResponseWriter.Write(b)

	rec.mu.Lock()
	rec.bytesW += int64(n)
	rec.mu.Unlock()
	return n, err
}

func (rec *accessRecorder) Flush() {
	if f, ok := rec.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (rec *accessRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := rec.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return h.Hijack()
}

func (rec *accessRecorder) Push(target string, opts *http.PushOptions) error {
	if p, ok := rec.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (rec *accessRecorder) Unwrap() http.ResponseWriter {
	return rec.ResponseWriter
}

func (rec *accessRecorder) entry() map[string]any {
	proto := rec.request.Proto
	if proto == "" {
		proto = "HTTP/1.1"
	}
	return map[string]any{
		"method":      rec.request.Method,
		"uri":         rec.request.URL.RequestURI(),
		"host":        rec.request.Host,
		"remote_addr": hostOnly(rec.request.RemoteAddr),
		"protocol":    proto,
		"target":      rec.target,
		"location":    rec.location,
		"user_agent":  rec.request.UserAgent(),
		"referer":     rec.request.Referer(),
		"request_id":  rec.requestID,
	}
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
