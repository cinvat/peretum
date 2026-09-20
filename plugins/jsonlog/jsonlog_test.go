package jsonlog

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cinvat/peretum/plugins/base"
)

type errWriter struct{ err error }

func (e errWriter) Write(p []byte) (int, error) { return 0, e.err }

// stubRW is a minimal ResponseWriter that also implements http.Flusher,
// http.Hijacker, and http.Pusher.
type stubRW struct {
	header  http.Header
	code    int
	body    bytes.Buffer
	flushed bool
}

func (s *stubRW) Header() http.Header {
	if s.header == nil {
		s.header = make(http.Header)
	}
	return s.header
}
func (s *stubRW) WriteHeader(code int) { s.code = code }
func (s *stubRW) Write(b []byte) (int, error) {
	return s.body.Write(b)
}
func (s *stubRW) Flush() { s.flushed = true }
func (s *stubRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, nil
}
func (s *stubRW) Push(target string, opts *http.PushOptions) error {
	return nil
}

// plainRW is a ResponseWriter with none of the optional interfaces.
type bareRW struct{}

func (bareRW) Header() http.Header         { return make(http.Header) }
func (bareRW) WriteHeader(int)             {}
func (bareRW) Write(b []byte) (int, error) { return len(b), nil }

func newEnabledPlugin(t *testing.T, access, errlog string) *JSONLogPlugin {
	t.Helper()
	p := NewJSONLogPlugin()
	if err := p.Init(map[string]any{"enabled": true, "access_log": access, "error_log": errlog}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })
	return p
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read %s: %v", path, err)
	}
	if strings.TrimSpace(string(b)) == "" {
		return nil
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func TestNewJSONLogPlugin_Name(t *testing.T) {
	if got := NewJSONLogPlugin().Name(); got != "jsonlog" {
		t.Errorf("name=%q, want jsonlog", got)
	}
}

func TestInit_FullConfig(t *testing.T) {
	p := NewJSONLogPlugin()
	cfg := map[string]any{
		"enabled":    true,
		"access_log": "/tmp/acc.jsonl",
		"error_log":  "/tmp/err.jsonl",
		"stdout":     true,
	}
	if err := p.Init(cfg); err != nil {
		t.Fatal(err)
	}
	if !p.enabled || p.accessPath != "/tmp/acc.jsonl" || p.errorPath != "/tmp/err.jsonl" || !p.stdout {
		t.Errorf("unexpected fields: %+v", p)
	}
}

func TestInit_Defaults(t *testing.T) {
	p := NewJSONLogPlugin()
	if err := p.Init(nil); err != nil {
		t.Fatal(err)
	}
	if p.accessPath != defaultAccessPath || p.errorPath != defaultErrorPath {
		t.Errorf("unexpected defaults: %q %q", p.accessPath, p.errorPath)
	}
}

func TestStart_Disabled(t *testing.T) {
	p := NewJSONLogPlugin()
	if err := p.Init(map[string]any{"enabled": false}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if p.started {
		t.Error("started should be false when disabled")
	}
}

func TestStart_OpensFilesAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	p := newEnabledPlugin(t, filepath.Join(dir, "access.jsonl"), filepath.Join(dir, "error.jsonl"))

	if _, err := os.Stat(filepath.Join(dir, "access.jsonl")); err != nil {
		t.Errorf("access file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "error.jsonl")); err != nil {
		t.Errorf("error file missing: %v", err)
	}
	if !p.started {
		t.Error("started should be true")
	}

	// Second start must be a no-op (buffers already set).
	oldAccess := p.accessFile
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	if p.accessFile != oldAccess {
		t.Error("Start replaced an existing access file")
	}
}

func TestStart_AccessOpenError(t *testing.T) {
	p := NewJSONLogPlugin()
	bad := filepath.Join("/dev/null", "access.jsonl")
	if err := p.Init(map[string]any{"enabled": true, "access_log": bad, "error_log": filepath.Join(t.TempDir(), "e.jsonl")}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err == nil {
		t.Error("expected error opening access log below a non-directory")
	}
}

func TestStart_ErrorOpenError(t *testing.T) {
	p := NewJSONLogPlugin()
	dir := t.TempDir()
	if err := p.Init(map[string]any{"enabled": true, "access_log": filepath.Join(dir, "a.jsonl"), "error_log": filepath.Join("/dev/null", "e.jsonl")}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err == nil {
		t.Error("expected error opening error log below a non-directory")
	}
}

func TestStop_FlushesAndCloses(t *testing.T) {
	dir := t.TempDir()
	p := newEnabledPlugin(t, filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "e.jsonl"))

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if p.accessBuf != nil || p.errorBuf != nil || p.accessFile != nil || p.errorFile != nil || p.started {
		t.Error("state should be reset after Stop")
	}
}

func TestStop_StdoutNotClosed(t *testing.T) {
	p := NewJSONLogPlugin()
	if err := p.Init(map[string]any{"enabled": true, "access_log": stdoutPath, "error_log": stdoutPath}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.accessFile != os.Stdout {
		t.Fatal("expected access file to be os.Stdout")
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.accessFile != os.Stdout {
		t.Error("stdout file should not have been cleared")
	}
}

func TestStop_NoWriters(t *testing.T) {
	p := NewJSONLogPlugin()
	if err := p.Init(map[string]any{"enabled": true}); err != nil {
		t.Fatal(err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("Stop on unstarted plugin: %v", err)
	}
}

func TestReopen_RotatesFiles(t *testing.T) {
	dir := t.TempDir()
	acc := filepath.Join(dir, "access.jsonl")
	errPath := filepath.Join(dir, "error.jsonl")
	p := newEnabledPlugin(t, acc, errPath)

	req := httptest.NewRequest("GET", "/first", nil)
	w1 := p.StartRequest(httptest.NewRecorder(), req, "t", "/loc")
	p.FinishRequest(w1, req, "t", "/loc", true)

	if err := os.Rename(acc, acc+".1"); err != nil {
		t.Fatal(err)
	}
	if err := p.Reopen(); err != nil {
		t.Fatalf("Reopen: %v", err)
	}

	w2 := p.StartRequest(httptest.NewRecorder(), req, "t", "/loc")
	p.FinishRequest(w2, req, "t", "/loc", false)

	if got := readLines(t, acc+".1"); len(got) != 1 {
		t.Errorf("rotated file should have 1 line, got %d", len(got))
	}
	if got := readLines(t, acc); len(got) != 1 {
		t.Errorf("new access file should have 1 line, got %d", len(got))
	}
}

func TestReopen_Disabled(t *testing.T) {
	p := NewJSONLogPlugin()
	if err := p.Init(nil); err != nil {
		t.Fatal(err)
	}
	if err := p.Reopen(); err != nil {
		t.Errorf("Reopen when disabled: %v", err)
	}
}

func TestReopen_Error(t *testing.T) {
	dir := t.TempDir()
	p := newEnabledPlugin(t, filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "e.jsonl"))

	// Make the access path's parent a file so the renewed open fails.
	f := filepath.Join(dir, "blocker")
	if err := os.WriteFile(f, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	p.accessPath = filepath.Join(f, "a.jsonl")
	if err := p.Reopen(); err == nil {
		t.Error("expected Reopen error")
	}
}

func TestReopen_ErrorPathTree(t *testing.T) {
	dir := t.TempDir()
	p := newEnabledPlugin(t, filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "e.jsonl"))

	// Access path stays on a real dir; make the error path's ancestor a file.
	f := filepath.Join(dir, "blocker")
	if err := os.WriteFile(f, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	p.errorPath = filepath.Join(f, "e.jsonl")
	if err := p.Reopen(); err == nil {
		t.Error("expected Reopen error on error log")
	}
}

func TestReopen_StdoutSkipped(t *testing.T) {
	p := NewJSONLogPlugin()
	if err := p.Init(map[string]any{"enabled": true, "access_log": stdoutPath, "error_log": stdoutPath}); err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := p.Reopen(); err != nil {
		t.Errorf("Reopen with stdout paths: %v", err)
	}
	if p.accessFile != os.Stdout || p.accessBuf == nil {
		t.Error("stdout writer should be untouched")
	}
}

func TestStartRequest_Disabled(t *testing.T) {
	p := NewJSONLogPlugin()
	w := httptest.NewRecorder()
	if got := p.StartRequest(w, httptest.NewRequest("GET", "/", nil), "t", "/"); got != w {
		t.Error("StartRequest should return w unchanged when disabled")
	}
}

func TestStartRequest_Enabled(t *testing.T) {
	dir := t.TempDir()
	p := newEnabledPlugin(t, filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "e.jsonl"))
	req := httptest.NewRequest("GET", "/x", nil)
	req.Header.Set("X-Request-ID", "rid-1")
	req, _ = base.EnsureRequestID(req)
	w := httptest.NewRecorder()
	rec, ok := p.StartRequest(w, req, "t", "/x").(*accessRecorder)
	if !ok {
		t.Fatal("expected *accessRecorder")
	}
	if rec.requestID != "rid-1" || rec.target != "t" || rec.location != "/x" {
		t.Errorf("recorder fields wrong: %+v", rec)
	}
}

func TestFinishRequest_Disabled(t *testing.T) {
	p := NewJSONLogPlugin()
	p.FinishRequest(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "t", "/", false) // no panic
}

func TestFinishRequest_NotRecorder(t *testing.T) {
	dir := t.TempDir()
	p := newEnabledPlugin(t, filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "e.jsonl"))
	p.FinishRequest(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), "t", "/", false) // no panic
}

func TestFinishRequest_EmitsEntry(t *testing.T) {
	dir := t.TempDir()
	p := newEnabledPlugin(t, filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "e.jsonl"))

	req := httptest.NewRequest("GET", "/api?x=1", nil)
	req.Host = "example.com"
	req.RemoteAddr = "1.2.3.4:5678"

	w := p.StartRequest(httptest.NewRecorder(), req, "t", "/api")
	rec := w.(*accessRecorder)
	rec.WriteHeader(201)
	rec.Write([]byte("hello"))

	p.FinishRequest(w, req, "t", "/api", true)

	lines := readLines(t, filepath.Join(dir, "a.jsonl"))
	if len(lines) != 1 {
		t.Fatalf("expected 1 log line, got %d", len(lines))
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["status"] != float64(201) {
		t.Errorf("status=%v", entry["status"])
	}
	if entry["bytes_sent"] != float64(5) {
		t.Errorf("bytes_sent=%v", entry["bytes_sent"])
	}
	if entry["cache"] != "HIT" || entry["type"] != "access" || entry["level"] != "INFO" {
		t.Errorf("entry=%v", entry)
	}
	if entry["method"] != "GET" || entry["uri"] != "/api?x=1" || entry["host"] != "example.com" {
		t.Errorf("entry=%v", entry)
	}
	if entry["remote_addr"] != "1.2.3.4" {
		t.Errorf("remote_addr=%v", entry["remote_addr"])
	}
}

func TestFinishRequest_WriteAutoStatus(t *testing.T) {
	dir := t.TempDir()
	p := newEnabledPlugin(t, filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "e.jsonl"))

	req := httptest.NewRequest("GET", "/", nil)
	w := p.StartRequest(httptest.NewRecorder(), req, "t", "/")
	rec := w.(*accessRecorder)
	rec.Write([]byte("x"))

	p.FinishRequest(w, req, "t", "/", false)

	lines := readLines(t, filepath.Join(dir, "a.jsonl"))
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["status"] != float64(200) {
		t.Errorf("expected implicit 200, got %v", entry["status"])
	}
	if entry["cache"] != "MISS" {
		t.Errorf("cache=%v", entry["cache"])
	}
}

func TestFinishRequest_WriteError(t *testing.T) {
	p := NewJSONLogPlugin()
	p.enabled = true
	p.accessBuf = bufio.NewWriter(errWriter{errors.New("boom")})

	req := httptest.NewRequest("GET", "/", nil)
	w := p.StartRequest(httptest.NewRecorder(), req, "t", "/")
	p.FinishRequest(w, req, "t", "/", false) // should not panic
}

func TestLogError_WithRequestID(t *testing.T) {
	dir := t.TempDir()
	p := newEnabledPlugin(t, filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "e.jsonl"))

	p.LogError("ERROR", "t", "/loc", "rid-9", "boom", map[string]any{"status": 502})

	lines := readLines(t, filepath.Join(dir, "e.jsonl"))
	if len(lines) != 1 {
		t.Fatalf("expected 1 error line, got %d", len(lines))
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["msg"] != "boom" || entry["request_id"] != "rid-9" || entry["target"] != "t" {
		t.Errorf("entry=%v", entry)
	}
	if entry["status"] != float64(502) {
		t.Errorf("field passthrough failed: %v", entry)
	}
}

func TestLogError_EmptyRequestID(t *testing.T) {
	dir := t.TempDir()
	p := newEnabledPlugin(t, filepath.Join(dir, "a.jsonl"), filepath.Join(dir, "e.jsonl"))
	p.LogError("WARN", "t", "/", "", "x", nil)
	for _, line := range readLines(t, filepath.Join(dir, "e.jsonl")) {
		if strings.Contains(line, "request_id") {
			t.Error("request_id should be omitted when empty")
		}
	}
}

func TestLogError_Disabled(t *testing.T) {
	p := NewJSONLogPlugin()
	p.LogError("ERROR", "t", "/", "", "x", nil) // no panic
}

func TestLogError_WriteError(t *testing.T) {
	p := NewJSONLogPlugin()
	p.enabled = true
	p.errorBuf = bufio.NewWriter(errWriter{errors.New("boom")})
	p.LogError("ERROR", "t", "/", "r", "x", nil) // no panic
}

func TestEmit_Errors(t *testing.T) {
	// json.Marshal failure.
	if err := emit(nil, false, map[string]any{"f": func() {}}); err == nil {
		t.Error("expected marshaling error")
	}

	// Buffer Write failure (buffer smaller than payload).
	if err := emit(bufio.NewWriterSize(errWriter{errors.New("w")}, 4), false, map[string]any{"a": "b"}); err == nil {
		t.Error("expected write error")
	}

	// Flush failure (payload buffered, flush hits failing writer).
	if err := emit(bufio.NewWriter(errWriter{errors.New("f")}), false, map[string]any{"a": "b"}); err == nil {
		t.Error("expected flush error")
	}

	// Nil buffer, no mirror: writes are skipped, no error.
	if err := emit(nil, false, map[string]any{"a": "b"}); err != nil {
		t.Errorf("emit(nil, false, ...): %v", err)
	}

	// Mirror to stdout writes through os.Stdout.
	buf := bufio.NewWriter(&bytes.Buffer{})
	if err := emit(buf, true, map[string]any{"ok": true}); err != nil {
		t.Errorf("mirror to stdout: %v", err)
	}
	if err := emit(nil, true, map[string]any{"ok": true}); err != nil {
		t.Errorf("mirror with nil buffer: %v", err)
	}
}

func TestOpenLogFile_Branches(t *testing.T) {
	f, err := openLogFile(stdoutPath)
	if err != nil || f != os.Stdout {
		t.Errorf("stdoutPath open: file=%v err=%v", f, err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "a.jsonl")
	f, err = openLogFile(path)
	if err != nil {
		t.Fatalf("open nested: %v", err)
	}
	f.Close()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("nested dir/file not created: %v", err)
	}

	if _, err := openLogFile(filepath.Join("/dev/null", "x.jsonl")); err == nil {
		t.Error("expected MkdirAll error")
	}
}

func TestAccessRecorder_StatusAndBytes(t *testing.T) {
	underlying := &stubRW{}
	rec := &accessRecorder{ResponseWriter: underlying, request: httptest.NewRequest("GET", "/", nil)}

	if rec.statusCode() != 200 {
		t.Errorf("default status %d", rec.statusCode())
	}
	rec.WriteHeader(418)
	if rec.statusCode() != 418 {
		t.Errorf("status %d", rec.statusCode())
	}
	rec.WriteHeader(500) // second call ignored
	if rec.statusCode() != 418 {
		t.Errorf("second WriteHeader not ignored: %d", rec.statusCode())
	}
	if n, _ := rec.Write([]byte("abc")); n != 3 {
		t.Errorf("Write n=%d", n)
	}
	if rec.bytes() != 3 {
		t.Errorf("bytes=%d", rec.bytes())
	}
}

func TestAccessRecorder_FlushHijackPushUnwrap(t *testing.T) {
	rec := &accessRecorder{ResponseWriter: &stubRW{}, request: httptest.NewRequest("GET", "/", nil)}
	rec.Flush() // flusher present, no panic
	if _, _, err := rec.Hijack(); err != nil {
		t.Errorf("Hijack: %v", err)
	}
	if err := rec.Push("/x", nil); err != nil {
		t.Errorf("Push: %v", err)
	}
	if rec.Unwrap() == nil {
		t.Error("Unwrap returned nil")
	}

	plain := &accessRecorder{ResponseWriter: bareRW{}, request: httptest.NewRequest("GET", "/", nil)}
	plain.Flush() // no flusher, no panic
	if _, _, err := plain.Hijack(); err != http.ErrNotSupported {
		t.Errorf("Hijack without hijacker: %v", err)
	}
	if err := plain.Push("/x", nil); err != http.ErrNotSupported {
		t.Errorf("Push without pusher: %v", err)
	}
}

func TestAccessRecorder_Entry(t *testing.T) {
	req := httptest.NewRequest("GET", "/u?q=1", nil)
	req.Host = "h"
	req.RemoteAddr = "1.2.3.4:9"
	req.Header.Set("User-Agent", "ua")
	req.Header.Set("Referer", "ref")
	rec := &accessRecorder{request: req, target: "t", location: "/u", requestID: "rid"}
	e := rec.entry()
	if e["method"] != "GET" || e["uri"] != "/u?q=1" || e["host"] != "h" || e["user_agent"] != "ua" || e["referer"] != "ref" || e["request_id"] != "rid" || e["protocol"] != "HTTP/1.1" {
		t.Errorf("entry=%v", e)
	}

	noProto := httptest.NewRequest("GET", "/x", nil)
	noProto.Proto = ""
	noProto.RemoteAddr = ""
	rec2 := &accessRecorder{request: noProto, target: "t", location: "/"}
	e2 := rec2.entry()
	if e2["protocol"] != "HTTP/1.1" {
		t.Errorf("default proto=%v", e2["protocol"])
	}
	if e2["remote_addr"] != "-" {
		t.Errorf("empty remote_addr=%v", e2["remote_addr"])
	}
}

func TestHostOnly(t *testing.T) {
	if got := hostOnly("1.2.3.4:80"); got != "1.2.3.4" {
		t.Errorf("with port: %q", got)
	}
	if got := hostOnly(" 1.2.3.4 "); got != "1.2.3.4" {
		t.Errorf("no port: %q", got)
	}
	if got := hostOnly(""); got != "-" {
		t.Errorf("empty: %q", got)
	}
}
