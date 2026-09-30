package handler

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cinvat/peretum/internal/loadbalancer"
)

func TestCapturingTransport_SetsContentLengthForChunkedUpstream(t *testing.T) {
	// The upstream streams a chunked (unknown-length) response. Because the
	// capturing transport buffers the whole body, it must report the real
	// ContentLength; otherwise httputil.ReverseProxy treats the response as
	// unbounded and flushes after every write, which can emit response
	// headers before the compression plugin sets Content-Encoding.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte(strings.Repeat("<div>line</div>\n", 100)))
	}))
	defer upstream.Close()

	lb := loadbalancer.New("", []*loadbalancer.Upstream{{URL: upstream.URL, Weight: 1}})
	ct := &capturingTransport{transport: http.DefaultTransport, lb: lb}
	req, err := http.NewRequest("GET", "http://example.com/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ct.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(ct.body) == 0 {
		t.Fatal("expected a buffered body")
	}
	if resp.ContentLength != int64(len(ct.body)) {
		t.Errorf("ContentLength=%d, want %d (fully buffered upstream body)", resp.ContentLength, len(ct.body))
	}

	// Buffered body must be readable back from the replaced response body.
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, ct.body) {
		t.Error("response body does not match buffered body")
	}
}

func TestCapturingTransport_NoUpstreams(t *testing.T) {
	ct := &capturingTransport{transport: http.DefaultTransport, lb: loadbalancer.New("", nil)}
	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Error("expected error with no healthy upstreams")
	}
}

func TestCapturingTransport_UpstreamRecoversAfterDown(t *testing.T) {
	// Regression test for the reported bug: an upstream that failed once was
	// permanently excluded (Next returned nil forever), so requests kept
	// returning 502 until the proxy was restarted even after the upstream
	// came back.
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	rt := &toggleRT{}
	ct := &capturingTransport{transport: rt, lb: lb}

	// Upstream down: first request fails and marks it unhealthy.
	rt.err = errors.New("connection refused")
	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Fatal("expected error while upstream is down")
	}
	if up.Healthy.Load() {
		t.Fatal("upstream should be unhealthy after failure")
	}

	// Upstream comes back: the next request must probe it (fail-back in the
	// load balancer) and restore its health — no restart required.
	rt.err = nil
	rt.resp = &http.Response{
		StatusCode: 200,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader([]byte("ok"))),
	}
	resp, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil))
	if err != nil {
		t.Fatalf("request should succeed after upstream recovers: %v", err)
	}
	resp.Body.Close()
	if !up.Healthy.Load() {
		t.Fatal("upstream should be healthy again after successful probe")
	}
}

func TestCapturingTransport_TransportError(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	ct := &capturingTransport{transport: &fakeRT{err: errors.New("conn refused")}, lb: lb}

	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Fatal("expected transport error")
	}
	if up.Healthy.Load() {
		t.Error("upstream should be marked unhealthy after transport error")
	}
}

func TestCapturingTransport_BodyReadError(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	ct := &capturingTransport{
		transport: &fakeRT{resp: &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       io.NopCloser(errBody{errors.New("read failed")}),
		}},
		lb: lb,
	}
	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Fatal("expected body read error")
	}
	if up.Healthy.Load() {
		t.Error("upstream should be marked unhealthy after body read failure")
	}
}

func TestCapturingTransport_OversizedBody(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	ct := &capturingTransport{
		transport: &fakeRT{resp: &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       io.NopCloser(io.LimitReader(zeroReader{}, maxBodyReadSize+1)),
		}},
		lb: lb,
	}
	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Fatal("expected oversized body error")
	}
	if up.Healthy.Load() {
		t.Error("upstream should be marked unhealthy after oversized response")
	}
}

func TestCapturingTransport_ExplicitLimit(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	ct := &capturingTransport{
		transport: &fakeRT{resp: &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("0123456789")),
		}},
		lb:          lb,
		maxBodySize: 4,
	}
	if _, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil)); err == nil {
		t.Fatal("expected oversized body error for explicit limit")
	}
	if up.Healthy.Load() {
		t.Error("upstream should be marked unhealthy after oversized response")
	}
}

func TestCapturingTransport_Unlimited(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	const payload = "0123456789"
	ct := &capturingTransport{
		transport: &fakeRT{resp: &http.Response{
			StatusCode: 200,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(payload)),
		}},
		lb:          lb,
		maxBodySize: -1,
	}
	resp, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	if !up.Healthy.Load() {
		t.Error("upstream should remain healthy for unlimited body")
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != payload {
		t.Errorf("body=%q", got)
	}
}

func TestCapturingTransport_Success(t *testing.T) {
	up := &loadbalancer.Upstream{URL: "http://a.example"}
	lb := loadbalancer.New("", []*loadbalancer.Upstream{up})
	ct := &capturingTransport{
		transport: &fakeRT{resp: &http.Response{
			StatusCode: 201,
			Header:     http.Header{"Content-Type": []string{"text/plain"}},
			Body:       io.NopCloser(strings.NewReader("hello upstream")),
		}},
		lb: lb,
	}

	resp, err := ct.RoundTrip(httptest.NewRequest("GET", "http://example.com/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	if ct.statusCode != 201 {
		t.Errorf("statusCode=%d", ct.statusCode)
	}
	if ct.headers.Get("Content-Type") != "text/plain" {
		t.Errorf("headers=%v", ct.headers)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "hello upstream" {
		t.Errorf("body=%q", got)
	}
	if string(ct.body) != "hello upstream" {
		t.Errorf("captured=%q", ct.body)
	}
	if ct.selectedUpstream != "http://a.example" {
		t.Errorf("selected=%q", ct.selectedUpstream)
	}
	if !up.Healthy.Load() {
		t.Error("upstream should be healthy after success")
	}
}

// --- test doubles ---------------------------------------------------------

type fakeRT struct {
	resp *http.Response
	err  error
}

func (f *fakeRT) RoundTrip(*http.Request) (*http.Response, error) {
	return f.resp, f.err
}

type toggleRT struct {
	resp *http.Response
	err  error
}

func (t *toggleRT) RoundTrip(*http.Request) (*http.Response, error) {
	return t.resp, t.err
}

type errBody struct{ err error }

func (e errBody) Read([]byte) (int, error) { return 0, e.err }

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
