package controller

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cinvat/peretum/api/v1alpha1/service"
	diskcache "github.com/cinvat/peretum/internal/cache/disk"
	"github.com/gin-gonic/gin"
)

func purgeRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.DELETE("/cache", PurgeCache)
	return r
}

func TestPurgeCacheRoute(t *testing.T) {
	dir := t.TempDir()
	service.SetCacheDir(dir)
	c, err := diskcache.New(dir, 10<<20, 0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	c.SetResponseToCache("k1", "example.com", "/a", 200, http.Header{}, []byte("a"))
	c.Close()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/cache?host=example.com&path=/a", nil)
	purgeRouter().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body.String())
	}
	var got struct {
		Purged int `json:"purged"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Purged != 1 {
		t.Fatalf("purged = %d, want 1", got.Purged)
	}
}

func TestPurgeCacheRouteValidation(t *testing.T) {
	service.SetCacheDir(t.TempDir())
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/cache", nil)
	purgeRouter().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}
