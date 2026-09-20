package base

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"
)

type Plugin interface {
	Name() string
	Init(config map[string]any) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type RequestHook interface {
	BeforeProxy(w http.ResponseWriter, r *http.Request, target, location string) error
	AfterProxy(w http.ResponseWriter, r *http.Request, target, location string, resp *http.Response) error
}

type ResponseBodyHook interface {
	TransformResponseBody(target, location, contentType string, body []byte) ([]byte, error)
}

type CacheHook interface {
	BeforeCacheStore(key, target, location string, statusCode int, headers http.Header, body []byte) ([]byte, error)
	AfterCacheHit(w http.ResponseWriter, r *http.Request, key, target, location string) error
	ShouldCache(target, location string, statusCode int, headers http.Header, body []byte) bool
}

type MetricsHook interface {
	RecordRequest(target, location, matchType string, cached bool, duration float64)
	RecordCacheHit(target, location string)
	RecordCacheMiss(target, location string)
	RecordCacheEviction()
	RecordCacheExpiration()
}

type ErrorHook interface {
	LogError(level, target, location, requestID, msg string, fields map[string]any)
}

// RouterWrapper optionally wraps the server's top-level router so the
// plugin can intercept responses before they reach the client, including
// router-level 404s and reverse-proxy 5xx errors. It is discovered by
// structural typing in cmd/server.go, mirroring the per-location
// WrapHandler contract used by compression.
type RouterWrapper interface {
	WrapRouter(h http.Handler) http.Handler
}

type BasePlugin struct {
	name   string
	config map[string]any
}

func NewBasePlugin(name string) *BasePlugin {
	return &BasePlugin{name: name}
}

func (bp *BasePlugin) Name() string { return bp.name }
func (bp *BasePlugin) Init(config map[string]any) error {
	bp.config = config
	return nil
}
func (bp *BasePlugin) Start(ctx context.Context) error { return nil }
func (bp *BasePlugin) Stop(ctx context.Context) error  { return nil }

type requestIDCtxKey struct{}

// EnsureRequestID returns a copy of r with a request ID stored in its
// context. The ID is taken from the X-Request-ID header when present,
// otherwise a random one is generated. Callers should reassign the
// result so downstream code and hooks share the same ID.
func EnsureRequestID(r *http.Request) (*http.Request, string) {
	id := r.Header.Get("X-Request-ID")
	if id == "" {
		id = newRequestID()
	}
	return r.WithContext(context.WithValue(r.Context(), requestIDCtxKey{}, id)), id
}

// RequestID returns the request ID attached by EnsureRequestID, or "".
func RequestID(r *http.Request) string {
	if id, ok := r.Context().Value(requestIDCtxKey{}).(string); ok {
		return id
	}
	return ""
}

var readRandom = rand.Read

func newRequestID() string {
	var b [8]byte
	if _, err := readRandom(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
