package loadbalancer

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testReq(t *testing.T, path string) *http.Request {
	t.Helper()
	return httptest.NewRequest(http.MethodGet, "http://example.com"+path, nil)
}

func mkUpstreams() []*Upstream {
	return []*Upstream{
		{URL: "http://a:8080", Weight: 2},
		{URL: "http://b:8080", Weight: 1},
	}
}

func TestNew(t *testing.T) {
	ups := mkUpstreams()
	algos := map[string][]string{
		"weighted_rr": {"weighted_rr", "wrr"},
		"maglev":      {"maglev"},
		"least_conn":  {"least_conn", "least_connections"},
		"round_robin": {"round_robin", "rr", ""},
	}
	impl := map[string]func([]*Upstream) LoadBalancer{
		"weighted_rr": NewWeightedRoundRobin,
		"maglev":      NewMaglev,
		"least_conn":  NewLeastConnections,
		"round_robin": NewRoundRobin,
	}
	for kind, names := range algos {
		for _, a := range names {
			if _, ok := New(a, ups).(*weightedRoundRobin); kind == "weighted_rr" && !ok {
				t.Fatalf("New(%q) not weighted_rr", a)
			}
			if _, ok := New(a, ups).(*maglev); kind == "maglev" && !ok {
				t.Fatalf("New(%q) not maglev", a)
			}
			if _, ok := New(a, ups).(*leastConnections); kind == "least_conn" && !ok {
				t.Fatalf("New(%q) not least_conn", a)
			}
			if _, ok := New(a, ups).(*roundRobin); kind == "round_robin" && !ok {
				t.Fatalf("New(%q) not round_robin", a)
			}
		}
	}
	// Unknown algorithm falls back to round robin.
	if _, ok := New("bogus", ups).(*roundRobin); !ok {
		t.Fatal("New(bogus) not round_robin")
	}
	_ = impl
}

func TestRoundRobin(t *testing.T) {
	ups := mkUpstreams()
	rr := NewRoundRobin(ups)

	for i := 0; i < 4; i++ {
		u := rr.Next(testReq(t, "/"))
		if u == nil {
			t.Fatal("Next returned nil")
		}
		if i%2 == 0 && u.URL != "http://a:8080" {
			t.Fatalf("expected a, got %s", u.URL)
		}
		if i%2 == 1 && u.URL != "http://b:8080" {
			t.Fatalf("expected b, got %s", u.URL)
		}
	}

	// No healthy upstreams -> a probe candidate is retried (fail-back) so
	// the dead upstreams can be re-marked healthy once they recover.
	rr.MarkHealthy("http://a:8080", false)
	rr.MarkHealthy("http://b:8080", false)
	if u := rr.Next(testReq(t, "/")); u == nil {
		t.Fatal("expected a probe candidate when all upstreams are unhealthy")
	}

	// MarkHealthy on an unknown URL is a no-op.
	rr.MarkHealthy("http://nope:1", true)

	// Empty list -> nil.
	empty := NewRoundRobin(nil)
	if u := empty.Next(testReq(t, "/")); u != nil {
		t.Fatal("expected nil for empty")
	}
	if got := empty.GetUpstreams(); got != nil {
		t.Fatal("expected nil upstreams")
	}
}

func TestRoundRobinUnhealthyRecoversViaProbe(t *testing.T) {
	ups := mkUpstreams()
	rr := NewRoundRobin(ups)

	// A single failed request marks the (only) upstream unhealthy.
	rr.MarkHealthy("http://a:8080", false)
	if ups[0].Healthy.Load() {
		t.Fatal("expected a unhealthy after failure")
	}

	// While all upstreams are unhealthy, Next retries one of them instead
	// of permanently returning nil.
	for i := 0; i < 3; i++ {
		u := rr.Next(testReq(t, "/"))
		if u == nil {
			t.Fatal("expected a probe candidate")
		}
	}

	// Downstream comes back; the probe succeeds and restores health.
	rr.MarkHealthy("http://a:8080", true)
	u := rr.Next(testReq(t, "/"))
	if u == nil || u.URL != "http://a:8080" {
		t.Fatalf("expected healthy a after recovery, got %+v", u)
	}
}

func TestProbeCandidate(t *testing.T) {
	old := time.Unix(100, 0)
	recent := time.Unix(200, 0)
	ups := []*Upstream{{URL: "a"}, {URL: "b"}}
	ups[0].LastCheck.Store(old)
	ups[1].LastCheck.Store(recent)
	if u := probeCandidate(ups); u == nil || u.URL != "a" {
		t.Fatalf("expected oldest check candidate a, got %+v", u)
	}

	// Never-probed upstreams (zero LastCheck) win as the probe candidate.
	newer := []*Upstream{{URL: "x"}, {URL: "y"}}
	newer[0].LastCheck.Store(recent)
	if u := probeCandidate(newer); u == nil || u.URL != "y" {
		t.Fatalf("expected never-checked candidate y, got %+v", u)
	}

	if u := probeCandidate(nil); u != nil {
		t.Fatalf("expected nil for empty, got %+v", u)
	}
}

func TestRoundRobinHealthyRestore(t *testing.T) {
	ups := mkUpstreams()
	rr := NewRoundRobin(ups)
	if !ups[0].Healthy.Load() || !ups[1].Healthy.Load() {
		t.Fatal("expected all healthy after New")
	}
	rr.MarkHealthy("http://a:8080", false)
	if ups[0].Healthy.Load() {
		t.Fatal("expected a unhealthy")
	}
	if ups[1].Healthy.Load() && ups[0].LastCheck.IsZero() {
		t.Fatal("LastCheck not set")
	}
	as := rr.GetUpstreams()
	if len(as) != 2 || as[0] != ups[0] || as[1] != ups[1] {
		t.Fatal("GetUpstreams mismatch")
	}
}

func TestWeightedRoundRobin(t *testing.T) {
	ups := []*Upstream{
		{URL: "http://a:8080", Weight: 2},
		{URL: "http://b:8080", Weight: 0},
	}
	wrr := NewWeightedRoundRobin(ups)

	for i := 0; i < 4; i++ {
		u := wrr.Next(testReq(t, "/"))
		if u == nil {
			t.Fatal("unexpected nil")
		}
	}
	// Weight 0 upstream gets 1 internally; verify the <=0 reset branch
	// by running until weights cycle.
	if got := wrr.GetUpstreams(); len(got) != 2 {
		t.Fatalf("GetUpstreams len = %d", len(got))
	}

	all := mkUpstreams()
	wrr2 := NewWeightedRoundRobin(all)
	wrr2.MarkHealthy("http://a:8080", false)
	wrr2.MarkHealthy("http://b:8080", false)
	if u := wrr2.Next(testReq(t, "/")); u == nil {
		t.Fatal("expected a probe candidate when all upstreams are unhealthy")
	}

	empty := NewWeightedRoundRobin(nil)
	if u := empty.Next(testReq(t, "/")); u != nil {
		t.Fatal("expected nil for empty")
	}
	wrr2.MarkHealthy("http://nope:1", true)
}

func TestWeightedRoundRobinSingleZeroWeight(t *testing.T) {
	ups := []*Upstream{{URL: "http://a:8080", Weight: 0}}
	wrr := NewWeightedRoundRobin(ups)
	// First call: weight 1 -> decremented to 0 -> reset loop sets from
	// upstream Weight (0) -> clamped back to 1.
	if u := wrr.Next(testReq(t, "/")); u == nil {
		t.Fatal("unexpected nil")
	}
	if u := wrr.Next(testReq(t, "/")); u == nil {
		t.Fatal("unexpected nil on second call")
	}
}

func TestMaglev(t *testing.T) {
	ups := mkUpstreams()
	m := NewMaglev(ups)
	for i := 0; i < 3; i++ {
		if u := m.Next(testReq(t, "/")); u == nil {
			t.Fatal("unexpected nil")
		}
	}

	// Same URL hashes consistently to the same upstream when healthy.
	u1 := m.Next(testReq(t, "/path?x=1"))
	u2 := m.Next(testReq(t, "/path?x=1"))
	if u1.URL != u2.URL {
		t.Fatalf("expected consistent hash, got %s vs %s", u1.URL, u2.URL)
	}

	// Mark one unhealthy -> rebuild covers the continue-branch in buildTable,
	// and Next falls back to the remaining healthy one.
	m.MarkHealthy("http://a:8080", false)
	if ups[0].Healthy.Load() {
		t.Fatal("expected a unhealthy")
	}
	for i := 0; i < 3; i++ {
		u := m.Next(testReq(t, "/"))
		if u == nil || u.URL != "http://b:8080" {
			t.Fatalf("expected healthy b, got %+v", u)
		}
	}

	// MarkHealthy on an unknown URL triggers a rebuild but no match.
	m.MarkHealthy("http://nope:1", true)

	// Direct construction covers the all-unhealthy / empty nil paths that
	// cannot be reached through MarkHealthy (which would rebuild a table
	// from zero healthy upstreams).
	direct := &maglev{upstreams: []*Upstream{{URL: "a"}}}
	direct.table = make([]int, maglevTableSize)
	if u := direct.Next(testReq(t, "/")); u == nil || u.URL != "a" {
		t.Fatalf("expected probe candidate a, got %+v", u)
	}

	direct2 := &maglev{upstreams: []*Upstream{{URL: "unhealthy"}, {URL: "healthy"}}}
	direct2.upstreams[1].Healthy.Store(true)
	direct2.table = make([]int, maglevTableSize)
	if u := direct2.Next(testReq(t, "/")); u == nil || u.URL != "healthy" {
		t.Fatalf("expected healthy fallback, got %+v", u)
	}

	empty := &maglev{}
	if u := empty.Next(testReq(t, "/")); u != nil {
		t.Fatal("expected nil for empty maglev")
	}
	if got := empty.GetUpstreams(); got != nil {
		t.Fatal("expected nil upstreams")
	}
}

func TestLeastConnections(t *testing.T) {
	ups := mkUpstreams()
	lc := NewLeastConnections(ups).(*leastConnections)
	lc.Increment("http://a:8080")

	// a has 1 active, b has 0 -> Next picks b.
	u := lc.Next(testReq(t, "/"))
	if u == nil || u.URL != "http://b:8080" {
		t.Fatalf("expected b, got %+v", u)
	}
	lc.Decrement("http://a:8080")
	if lc.active[0].Load() != 0 {
		t.Fatal("expected a active 0")
	}

	// All unhealthy -> probe candidate (fail-back), not nil.
	lc.MarkHealthy("http://a:8080", false)
	lc.MarkHealthy("http://b:8080", false)
	if u := lc.Next(testReq(t, "/")); u == nil {
		t.Fatal("expected a probe candidate when all upstreams are unhealthy")
	}

	// Restore both; equal actives -> ties resolve to first visited.
	lc.MarkHealthy("http://a:8080", true)
	lc.MarkHealthy("http://b:8080", true)
	if u := lc.Next(testReq(t, "/")); u == nil || u.URL != "http://a:8080" {
		t.Fatalf("expected tie-break a, got %+v", u)
	}

	lc.MarkHealthy("http://nope:1", true)
	lc.Increment("http://nope:1")
	lc.Decrement("http://nope:1")

	empty := NewLeastConnections(nil)
	if u := empty.Next(testReq(t, "/")); u != nil {
		t.Fatal("expected nil for empty")
	}
	if got := empty.GetUpstreams(); got != nil {
		t.Fatal("expected nil upstreams")
	}
}

func TestLeastConnectionsMin(t *testing.T) {
	ups := mkUpstreams()
	lc := NewLeastConnections(ups).(*leastConnections)
	lc.active[0].Store(5)
	// a(5) then b(0): b wins via curr < bestActive.
	if u := lc.Next(testReq(t, "/")); u == nil || u.URL != "http://b:8080" {
		t.Fatalf("expected b, got %+v", u)
	}
}
