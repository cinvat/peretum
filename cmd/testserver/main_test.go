package main

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestNewTestHandler(t *testing.T) {
	h := newTestHandler()
	cases := []struct {
		path, ctype string
	}{
		{"/style.css", "text/css"},
		{"/script.js", "application/javascript"},
		{"/image.png", "image/png"},
		{"/anything", "text/plain"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest("GET", tc.path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d", tc.path, rec.Code)
		}
		if got := rec.Header().Get("Content-Type"); got != tc.ctype {
			t.Fatalf("%s: content-type = %q, want %q", tc.path, got, tc.ctype)
		}
		if rec.Body.Len() == 0 {
			t.Fatalf("%s: empty body", tc.path)
		}
	}
}

func TestStartServerServes(t *testing.T) {
	port := freePort(t)
	done := make(chan error, 1)
	go func() { done <- StartServer(fmt.Sprintf("127.0.0.1:%d", port)) }()

	url := fmt.Sprintf("http://127.0.0.1:%d/style.css", port)
	var resp *http.Response
	var err error
	for i := 0; i < 100; i++ {
		resp, err = http.Get(url)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestMainPanicsOnListenError(t *testing.T) {
	ln, err := net.Listen("tcp", ":8082")
	if err != nil {
		t.Skipf("port 8082 unavailable: %v", err)
	}
	defer ln.Close()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected main() to panic")
		}
	}()
	main()
}
