package waf

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// --- helpers.go coverage ---

func TestGetString(t *testing.T) {
	if got := getString(map[string]any{"k": "v"}, "k"); got != "v" {
		t.Fatalf("getString = %q", got)
	}
	if got := getString(map[string]any{}, "k"); got != "" {
		t.Fatalf("getString missing = %q", got)
	}
	if got := getString(map[string]any{"k": 1}, "k"); got != "" {
		t.Fatalf("getString non-string = %q", got)
	}
}

func TestGetBool(t *testing.T) {
	if !getBool(map[string]any{"k": true}, "k") {
		t.Fatal("expected true")
	}
	if getBool(map[string]any{}, "k") {
		t.Fatal("expected false for missing")
	}
	if getBool(map[string]any{"k": "true"}, "k") {
		t.Fatal("expected false for non-bool")
	}
}

func TestGetInt(t *testing.T) {
	if got := getInt(map[string]any{"k": 42}, "k"); got != 42 {
		t.Fatalf("int = %d", got)
	}
	if got := getInt(map[string]any{"k": 3.5}, "k"); got != 3 {
		t.Fatalf("float = %d", got)
	}
	if got := getInt(map[string]any{"k": uint32(7)}, "k"); got != 7 {
		t.Fatalf("uint32 = %d", got)
	}
	if got := getInt(map[string]any{}, "k"); got != 0 {
		t.Fatalf("missing = %d", got)
	}
}

func TestParseCIDR(t *testing.T) {
	if n := parseCIDR("1.2.3.4"); n == nil || !n.IP.Equal(net.ParseIP("1.2.3.4")) {
		t.Fatalf("plain IP: %v", n)
	}
	if n := parseCIDR("::1"); n == nil || n.IP.To16() == nil {
		t.Fatalf("ipv6: %v", n)
	}
	if n := parseCIDR("10.0.0.0/8"); n == nil {
		t.Fatal("cidr nil")
	}
	if n := parseCIDR("bad"); n != nil {
		t.Fatal("invalid should be nil")
	}
	if n := parseCIDR("abc/def"); n != nil {
		t.Fatal("invalid cidr should be nil")
	}
}

func TestParseIPNets(t *testing.T) {
	nets := parseIPNets("1.2.3.4, 10.0.0.0/8, bad, 2001:db8::/32")
	if len(nets) != 3 {
		t.Fatalf("expected 3 nets, got %d", len(nets))
	}
	if !nets[0].IP.Equal(net.ParseIP("1.2.3.4")) {
		t.Fatalf("nets[0] = %v", nets[0])
	}
	if nets[1].String() != "10.0.0.0/8" {
		t.Fatalf("nets[1] = %v", nets[1])
	}
	if nets[2].String() != "2001:db8::/32" {
		t.Fatalf("nets[2] = %v", nets[2])
	}
	if nets2 := parseIPNets("bad,, "); len(nets2) != 0 {
		t.Fatalf("all-bad = %d", len(nets2))
	}
}

func TestIsIPOperator(t *testing.T) {
	for _, op := range []string{"equals", "in", "not_in", "in_ip", "not_in_ip", "In", "IN_IP", "Not_In_Ip"} {
		if !isIPOperator(op) {
			t.Errorf("expected ip operator: %q", op)
		}
	}
	for _, op := range []string{"contains", "matches", "gt", "lt", "exists", ""} {
		if isIPOperator(op) {
			t.Errorf("unexpected ip operator: %q", op)
		}
	}
}

// --- clientIP / requestCtx ---

func TestClientIP(t *testing.T) {
	req := func(xri, xff, ra string) *http.Request {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = ra
		if xff != "" {
			r.Header.Set("X-Forwarded-For", xff)
		}
		if xri != "" {
			r.Header.Set("X-Real-IP", xri)
		}
		return r
	}
	cases := []struct {
		name, xri, xff, ra, want string
	}{
		{"x-real-ip", "2.2.2.2", "", "8.8.8.8:1", "2.2.2.2"},
		{"xff-multi", "", "1.2.3.4, 9.9.9.9", "8.8.8.8:1", "1.2.3.4"},
		{"xff-single", "", "1.2.3.4", "8.8.8.8:1", "1.2.3.4"},
		{"remote-no-port", "", "", "8.8.8.8", "8.8.8.8"},
		{"remote-port", "", "", "8.8.8.8:9999", "8.8.8.8"},
	}
	for _, tc := range cases {
		if got := clientIP(req(tc.xri, tc.xff, tc.ra)); got != tc.want {
			t.Errorf("%s: clientIP = %q, want %q", tc.name, got, tc.want)
		}
	}
}

type errReader struct{}

func (*errReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestRequestCtxGet(t *testing.T) {
	r := httptest.NewRequest("POST", "https://host.com/path?q=hello", bytes.NewBufferString(`{"a":1}`))
	r.Header.Set("User-Agent", "TestUA")
	r.Header.Set("Referer", "http://ref.com")
	r.Header.Set("Cookie", "session=abc")
	r.Header.Set("X-Real-IP", "3.3.3.3")

	ctx := newRequestCtx(r, nil)
	cases := []struct {
		param, name, want string
	}{
		{"host", "", "host.com"},
		{"user_agent", "", "TestUA"},
		{"referer", "", "http://ref.com"},
		{"cookie", "", "session=abc"},
		{"url", "", "/path?q=hello"},
		{"path", "", "/path"},
		{"query", "", "q=hello"},
		{"method", "", "POST"},
		{"ip", "", "3.3.3.3"},
		{"country", "", ""},
		{"asn", "", ""},
		{"asn_org", "", ""},
		{"city", "", ""},
		{"arg", "q", "hello"},
		{"arg", "zz", ""},
		{"header", "X-Real-IP", "3.3.3.3"},
		{"header", "X-Missing", ""},
		{"bogus", "", ""},
	}
	for _, tc := range cases {
		got, present := ctx.get(tc.param, tc.name)
		wantPresent := tc.want != ""
		if got != tc.want || present != wantPresent {
			t.Errorf("get(%q,%q) = (%q,%v), want (%q,%v)", tc.param, tc.name, got, present, tc.want, wantPresent)
		}
	}
	// body present + cached on second read.
	if got, present := ctx.get("body", ""); !present || got != `{"a":1}` {
		t.Fatalf("body = %q %v", got, present)
	}
	if got, present := ctx.get("body", ""); !present || got != `{"a":1}` {
		t.Fatalf("cached body = %q %v", got, present)
	}
	// body read error -> empty.
	ctxErr := newRequestCtx(httptest.NewRequest("POST", "/", &errReader{}), nil)
	if got, present := ctxErr.get("body", ""); present || got != "" {
		t.Fatalf("error body = %q %v", got, present)
	}
}

func TestRequestCtxGeo(t *testing.T) {
	dir := writeFixtureDir(t)
	gs, err := openGeodb(dir)
	if err != nil {
		t.Fatalf("openGeodb: %v", err)
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "8.8.8.8:1234"
	ctx := newRequestCtx(r, gs)
	country, p := ctx.get("country", "")
	if !p || country != "US" {
		t.Fatalf("country = %q %v", country, p)
	}
	asn, p := ctx.get("asn", "")
	if !p || asn != "15169" {
		t.Fatalf("asn = %q %v", asn, p)
	}
	org, p := ctx.get("asn_org", "")
	if !p || org != "GOOGLE" {
		t.Fatalf("asn_org = %q %v", org, p)
	}
	// geo is cached: ensureGeo doesn't re-lookup.
	ctx.ensureGeo()
}

func TestRequestCtxBadIP(t *testing.T) {
	// Non-nil geodb so ensureGeo reaches the IP parse step.
	g, _ := openGeodb("")
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "bad-ip"
	ctx := newRequestCtx(r, g)
	if c, p := ctx.get("country", ""); p || c != "" {
		t.Fatalf("invalid ip country = %q %v", c, p)
	}
}

func TestRequestCtxNoBody(t *testing.T) {
	// Manually built request so Body is truly nil.
	r := &http.Request{
		Method:     "GET",
		URL:        &url.URL{Scheme: "https", Host: "h", Path: "/"},
		Header:     make(http.Header),
		RemoteAddr: "1.2.3.4:1",
	}
	ctx := newRequestCtx(r, nil)
	if b, p := ctx.get("body", ""); p || b != "" {
		t.Fatalf("no-body = %q %v", b, p)
	}
}

// --- applyIndex ---

func TestApplyIndex(t *testing.T) {
	idx := &paramIndex{
		name:     "v",
		equals:   map[string]int{"x": 0},
		prefixes: []prefixCond{{"mo", 1}},
		suffixes: []suffixCond{{"il", 2}},
		ins: []inCond{
			{values: map[string]struct{}{"a": {}}, negate: false, condID: 4},
			{values: map[string]struct{}{"b": {}}, negate: true, condID: 5},
		},
		numGt:     []numCond{{10, 6}},
		numLt:     []numCond{{100, 7}},
		exists:    []int{8},
		notExists: []int{9},
	}
	// Wire up an automaton for contains cond IDs 3,10.
	idx.containsPatterns = []string{"safari", "safari"}
	idx.containsConds = []int{3, 10}
	finalizeIndexes(&ruleSet{params: map[string]*paramIndex{"v": idx}})
	if idx.aho == nil {
		t.Fatal("automaton not built")
	}

	t.Run("not-present", func(t *testing.T) {
		hits := make([]bool, 11)
		applyIndex(idx, hits, "", false)
		if hits[8] || !hits[9] {
			t.Fatalf("exists=%v notExists=%v", hits[8], hits[9])
		}
	})
	t.Run("prefix-suffix-in", func(t *testing.T) {
		hits := make([]bool, 11)
		applyIndex(idx, hits, "mozil", true)
		if hits[0] || !hits[1] || !hits[2] || hits[3] || hits[4] || !hits[5] || hits[6] {
			t.Fatalf("mozil hits=%v", hits)
		}
	})
	t.Run("equals-and-in", func(t *testing.T) {
		hits := make([]bool, 11)
		applyIndex(idx, hits, "x", true)
		// equals "x" hits; In {"a"} misses; not_in {"b"} hits (x not in b).
		if !hits[0] || hits[4] || !hits[5] {
			t.Fatalf("x hits=%v", hits)
		}
	})
	t.Run("numeric-gt", func(t *testing.T) {
		hits := make([]bool, 11)
		applyIndex(idx, hits, "1024", true)
		if !hits[6] || hits[7] {
			t.Fatalf("1024 hits=%v", hits)
		}
	})
	t.Run("numeric-lt", func(t *testing.T) {
		hits := make([]bool, 11)
		applyIndex(idx, hits, "5", true)
		if !hits[7] || hits[6] {
			t.Fatalf("5 hits=%v", hits)
		}
	})
	t.Run("contains-case-insensitive", func(t *testing.T) {
		hits := make([]bool, 11)
		applyIndex(idx, hits, "Mozilla Safari", true)
		if !hits[3] || !hits[10] {
			t.Fatalf("contains hits=%v", hits)
		}
	})
}

// --- applyIndex IP conditions ---

func TestApplyIndexIP(t *testing.T) {
	idx := &paramIndex{
		ipNets: []ipCond{
			{nets: []*net.IPNet{mustCIDR(t, "10.0.0.0/8"), mustCIDR(t, "8.8.8.8")}, negate: false, condID: 0},
			{nets: []*net.IPNet{mustCIDR(t, "8.8.0.0/16")}, negate: true, condID: 1},
		},
	}
	t.Run("inside-equals-net", func(t *testing.T) {
		hits := make([]bool, 2)
		applyIndex(idx, hits, "8.8.8.8", true)
		if !hits[0] || hits[1] {
			t.Fatalf("8.8.8.8 hits=%v", hits)
		}
	})
	t.Run("inside-cidr-only", func(t *testing.T) {
		hits := make([]bool, 2)
		applyIndex(idx, hits, "10.1.2.3", true)
		if !hits[0] || !hits[1] {
			t.Fatalf("10.1.2.3 hits=%v", hits)
		}
	})
	t.Run("outside-negate-misses", func(t *testing.T) {
		hits := make([]bool, 2)
		// 9.9.9.9 is outside both; negated cond 1 fires.
		applyIndex(idx, hits, "9.9.9.9", true)
		if hits[0] || !hits[1] {
			t.Fatalf("9.9.9.9 hits=%v", hits)
		}
	})
	t.Run("non-ip-value", func(t *testing.T) {
		hits := make([]bool, 2)
		applyIndex(idx, hits, "not-an-ip", true)
		if hits[0] || hits[1] {
			t.Fatalf("bad ip hits=%v", hits)
		}
	})
}

func mustCIDR(t *testing.T, s string) *net.IPNet {
	t.Helper()
	n := parseCIDR(s)
	if n == nil {
		t.Fatalf("mustCIDR(%q) = nil", s)
	}
	return n
}

// --- ruleMatches ---

func TestRuleMatches(t *testing.T) {
	hits := make([]bool, 5)
	hits[0], hits[1], hits[2] = true, true, true
	r := &compiledRule{groups: [][]int{{0, 1, 2}, {3}}}
	if !ruleMatches(r, hits) {
		t.Fatal("expected match")
	}
	r = &compiledRule{groups: [][]int{{0, 3}}}
	if ruleMatches(r, hits) {
		t.Fatal("expected no match")
	}
	r = &compiledRule{groups: [][]int{{0, 99}}}
	if ruleMatches(r, hits) {
		t.Fatal("out-of-bounds group should not match")
	}
}

// --- blockConfig ---

func TestBlockConfig(t *testing.T) {
	s, m := blockConfig(nil)
	if s != 403 || m != "Request blocked by WAF" {
		t.Fatalf("defaults = %d %q", s, m)
	}
	rule := &compiledRule{code: 451, message: "nope"}
	s, m = blockConfig(rule)
	if s != 451 || m != "nope" {
		t.Fatalf("rule override = %d %q", s, m)
	}
	s, m = blockConfig(&compiledRule{code: 451})
	if s != 451 || m != "Request blocked by WAF" {
		t.Fatalf("code-only = %d %q", s, m)
	}
	s, m = blockConfig(&compiledRule{message: "nope"})
	if s != 403 || m != "nope" {
		t.Fatalf("message-only = %d %q", s, m)
	}
}

// --- buildRuleSet / compileCondition ---

func TestBuildRuleSetParsing(t *testing.T) {
	cfg := map[string]any{
		"enabled": true,
		"rules": []any{
			"notarule",
			map[string]any{
				"id": "r1", "name": "name1", "enabled": true,
				"action": map[string]any{"type": "deny", "code": 451, "message": "msg1"},
				"conditions": []any{
					[]any{"notagroup"},
					[]any{
						map[string]any{"param": "user_agent", "operator": "contains", "value": "safari"},
						map[string]any{"param": "path", "operator": "in", "value": "/a,/b"},
						map[string]any{"param": "path", "operator": "matches", "value": `^/\w+$`},
						map[string]any{"param": "header", "operator": "gt", "value": "100", "param_name": "CL"},
						map[string]any{"param": "header", "operator": "lt", "value": "999999", "param_name": "CL"},
						map[string]any{"param": "header", "operator": "exists", "value": ""},
						map[string]any{"param": "header", "operator": "not_exists", "value": ""},
						map[string]any{"param": "host", "operator": "equals", "value": "x"},
						map[string]any{"param": "host", "operator": "startswith", "value": "x"},
						map[string]any{"param": "host", "operator": "endswith", "value": ".com"},
						map[string]any{"param": "host", "operator": "not_in", "value": "bad"},
						map[string]any{"param": "host", "operator": "matches", "value": "(((x"},
						map[string]any{"param": "host", "operator": "gt", "value": "xx"},
						map[string]any{"param": "host", "operator": "lt", "value": "yy"},
					},
				},
			},
			map[string]any{"id": "r2", "enabled": false},
			map[string]any{"id": "r3", "conditions": []any{[]any{map[string]any{"param": ""}}}},
			map[string]any{"id": "r4", "action": map[string]any{},
				"conditions": []any{"notgroupany"}},
		},
	}
	rs, err := buildRuleSet(cfg)
	if err != nil {
		t.Fatalf("buildRuleSet: %v", err)
	}
	// r1 is the only rule that ends up with conditions.
	if len(rs.rules) != 1 || rs.rules[0].id != "r1" {
		t.Fatalf("rules = %d", len(rs.rules))
	}
	r1 := rs.rules[0]
	if len(r1.groups) != 1 || len(r1.groups[0]) != 14 {
		t.Fatalf("r1 groups = %v", r1.groups)
	}
	if !r1.enabled || r1.code != 451 || r1.message != "msg1" || r1.name != "name1" {
		t.Fatalf("r1 meta: %v", r1)
	}

	// Defaults when keys are absent.
	rs2, err := buildRuleSet(map[string]any{"enabled": true})
	if err != nil || len(rs2.rules) != 0 {
		t.Fatalf("defaults: %v", rs2)
	}
	// Disabled returns nil.
	rs3, err := buildRuleSet(map[string]any{"enabled": false})
	if err != nil || rs3 != nil {
		t.Fatal("disabled should be nil")
	}
	// rules present but wrong type.
	rs4, err := buildRuleSet(map[string]any{"enabled": true, "rules": "bad"})
	if err == nil || rs4 != nil {
		t.Fatal("bad rules type should error")
	}
}

func TestBuildRuleSetOperatorInRealGroup(t *testing.T) {
	// A rule that requires every operator to actually evaluate true.
	cfg := map[string]any{
		"enabled": true,
		"rules": []any{
			map[string]any{
				"id": "r1",
				"conditions": []any{
					[]any{
						map[string]any{"param": "user_agent", "operator": "contains", "value": "safari"},
						map[string]any{"param": "user_agent", "operator": "equals", "value": "mozilla safari"},
						map[string]any{"param": "user_agent", "operator": "startswith", "value": "mo"},
						map[string]any{"param": "user_agent", "operator": "endswith", "value": "ari"},
						map[string]any{"param": "user_agent", "operator": "matches", "value": `(?i)^MOZILLA`},
						map[string]any{"param": "user_agent", "operator": "In", "value": "mozilla safari,chrome"},
						map[string]any{"param": "header", "operator": "gt", "value": "100", "param_name": "Content-Length"},
						map[string]any{"param": "header", "operator": "lt", "value": "99999", "param_name": "Content-Length"},
						map[string]any{"param": "arg", "operator": "exists", "value": "", "param_name": "q"},
						map[string]any{"param": "arg", "operator": "equals", "value": "hello", "param_name": "q"},
					},
				},
			},
		},
	}
	rs, err := buildRuleSet(cfg)
	if err != nil {
		t.Fatalf("buildRuleSet: %v", err)
	}
	r := httptest.NewRequest("GET", "https://h/p?q=hello", nil)
	r.Header.Set("User-Agent", "mozilla safari")
	r.Header.Set("Content-Length", "256")
	act, _ := rs.evaluate(r, nil)
	if act != ruleSetActionBlock {
		t.Fatal("expected block when all operators hit")
	}
}

// --- finalizeIndexes ---

// --- evaluate / WAFPlugin ---

func requestFor(remote, host, ua, method string) *http.Request {
	u, _ := url.Parse("https://" + host + "/")
	r := httptest.NewRequest(method, u.String(), nil)
	r.Header.Set("User-Agent", ua)
	r.RemoteAddr = remote
	return r
}

func TestEvaluateImmediateBlock(t *testing.T) {
	rs, _ := buildRuleSet(map[string]any{
		"enabled": true,
		"rules": []any{map[string]any{
			"id": "x", "action": map[string]any{"type": "deny", "code": 451, "message": "bad"},
			"conditions": []any{[]any{map[string]any{"param": "user_agent", "operator": "contains", "value": "safari"}}},
		}},
	})
	req := requestFor("8.8.8.8:1", "x.com", "Mozilla Safari 5", "GET")
	act, rule := rs.evaluate(req, nil)
	if act != ruleSetActionBlock || rule.id != "x" {
		t.Fatalf("expected block, got %d %v", act, rule)
	}
}

func TestEvaluateGeoRules(t *testing.T) {
	dir := writeFixtureDirAll(t)
	gs, err := openGeodb(dir)
	if err != nil {
		t.Fatalf("openGeodb: %v", err)
	}
	mk := func(param, op, value string) map[string]any {
		return map[string]any{"param": param, "operator": op, "value": value}
	}
	rule := func(conds ...map[string]any) map[string]any {
		all := make([]any, len(conds))
		for i, c := range conds {
			all[i] = c
		}
		return map[string]any{"id": "r", "conditions": []any{all}}
	}
	cases := []struct {
		name  string
		rules []any
		ip    string
		want  ruleSetAction
	}{
		{"country_equals_us", []any{rule(mk("country", "equals", "US"))}, "8.8.8.8:1", ruleSetActionBlock},
		{"country_equals_miss", []any{rule(mk("country", "equals", "US"))}, "8.8.4.4:1", ruleSetActionAllow},
		{"country_contains", []any{rule(mk("country", "contains", "S"))}, "8.8.8.8:1", ruleSetActionBlock},
		{"country_in", []any{rule(mk("country", "in", "CN,US"))}, "8.8.8.8:1", ruleSetActionBlock},
		{"country_not_in", []any{rule(mk("country", "not_in", "US"))}, "8.8.8.8:1", ruleSetActionAllow},
		{"country_not_in", []any{rule(mk("country", "not_in", "US"))}, "8.8.8.8:1", ruleSetActionAllow},
		{"asn_in", []any{rule(mk("asn", "in", "666,15169"))}, "8.8.8.8:1", ruleSetActionBlock},
		{"asn_not_in", []any{rule(mk("asn", "not_in", "15169"))}, "8.8.8.8:1", ruleSetActionAllow},
		{"asn_gt", []any{rule(mk("asn", "gt", "10000"))}, "8.8.8.8:1", ruleSetActionBlock},
		{"asn_lt", []any{rule(mk("asn", "lt", "10000"))}, "8.8.8.8:1", ruleSetActionAllow},
		{"asn_org_equals", []any{rule(mk("asn_org", "equals", "GOOGLE"))}, "8.8.8.8:1", ruleSetActionBlock},
		{"city_equals", []any{rule(mk("city", "equals", "Mountain View"))}, "8.8.8.8:1", ruleSetActionBlock},
		{"city_miss", []any{rule(mk("city", "equals", "Mountain View"))}, "8.8.4.4:1", ruleSetActionAllow},
		{"geo_and_host", []any{rule(mk("country", "equals", "US"), mk("host", "equals", "x.com"))}, "8.8.8.8:1", ruleSetActionBlock},
	}
	for _, tc := range cases {
		rs, err := buildRuleSet(map[string]any{"enabled": true, "rules": tc.rules})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if act, _ := rs.evaluate(requestFor(tc.ip, "x.com", "", "GET"), gs); act != tc.want {
			t.Errorf("%s: act=%d want=%d", tc.name, act, tc.want)
		}
	}
}

func TestEvaluateIPRules(t *testing.T) {
	mk := func(op, value string) map[string]any {
		return map[string]any{"param": "ip", "operator": op, "value": value}
	}
	rule := func(conds ...map[string]any) map[string]any {
		all := make([]any, len(conds))
		for i, c := range conds {
			all[i] = c
		}
		return map[string]any{"id": "r", "conditions": []any{all}}
	}
	cases := []struct {
		name  string
		rules []any
		ip    string
		want  ruleSetAction
	}{
		{"equals_bare_hit", []any{rule(mk("equals", "8.8.8.8"))}, "8.8.8.8:1", ruleSetActionBlock},
		{"equals_bare_miss", []any{rule(mk("equals", "8.8.8.8"))}, "1.1.1.1:1", ruleSetActionAllow},
		{"equals_cidr", []any{rule(mk("equals", "8.8.0.0/16"))}, "8.8.8.8:1", ruleSetActionBlock},
		{"in_cidr_list", []any{rule(mk("in", "1.1.1.0/24, 8.8.0.0/16"))}, "8.8.4.4:1", ruleSetActionBlock},
		{"in_miss", []any{rule(mk("in", "1.1.1.0/24,8.8.0.0/16"))}, "9.9.9.9:1", ruleSetActionAllow},
		{"in_ip_alias", []any{rule(mk("in_ip", "8.8.8.0/24"))}, "8.8.8.8:1", ruleSetActionBlock},
		{"not_in_ip_outside_hit", []any{rule(mk("not_in_ip", "8.8.8.0/24"))}, "9.9.9.9:1", ruleSetActionBlock},
		{"not_in_ip_inside_miss", []any{rule(mk("not_in_ip", "8.8.8.0/24"))}, "8.8.8.8:1", ruleSetActionAllow},
		{"not_in_list", []any{rule(mk("not_in", "1.1.1.0/24,8.8.8.0/24"))}, "9.9.9.9:1", ruleSetActionBlock},
		{"malformed_never_matches", []any{rule(mk("in_ip", "notanip"))}, "8.8.8.8:1", ruleSetActionAllow},
		{"ipv6_contains", []any{rule(mk("equals", "2001:db8::/32"))}, "2001:db8::1:1", ruleSetActionBlock},
	}
	for _, tc := range cases {
		rs, err := buildRuleSet(map[string]any{"enabled": true, "rules": tc.rules})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if act, _ := rs.evaluate(requestFor(tc.ip, "x.com", "", "GET"), nil); act != tc.want {
			t.Errorf("%s: act=%d want=%d", tc.name, act, tc.want)
		}
	}
}

func TestEvaluateAllowAndLogActions(t *testing.T) {
	mk := func(id, action string) map[string]any {
		am := map[string]any{"type": action}
		return map[string]any{"id": id, "action": am,
			"conditions": []any{[]any{map[string]any{"param": "host", "operator": "contains", "value": "match"}}}}
	}
	// allow rule runs first and short-circuits.
	rs, _ := buildRuleSet(map[string]any{"enabled": true,
		"rules": []any{mk("allow", "allow"), mk("deny", "deny")}})
	act, r := rs.evaluate(requestFor("8.8.8.8:1", "match", "", "GET"), nil)
	if act != ruleSetActionAllow || r.id != "allow" {
		t.Fatalf("allow short-circuit: %d %v", act, r)
	}
	// log action only.
	rs, _ = buildRuleSet(map[string]any{"enabled": true,
		"rules": []any{mk("log", "log")}})
	if act, _ := rs.evaluate(requestFor("8.8.8.8:1", "match", "", "GET"), nil); act != ruleSetActionAllow {
		t.Fatal("log action should allow")
	}
	// disabled rule ignored.
	rs, _ = buildRuleSet(map[string]any{"enabled": true,
		"rules": []any{mk("off", "deny")}})
	rs.rules[0].enabled = false
	if act, _ := rs.evaluate(requestFor("8.8.8.8:1", "match", "", "GET"), nil); act != ruleSetActionAllow {
		t.Fatal("disabled rule should allow")
	}
}

func TestWAFPluginLifecycle(t *testing.T) {
	dir := writeFixtureDir(t)
	locWAF := map[string]any{
		"enabled": true,
		"rules": []any{map[string]any{"id": "r1", "conditions": []any{[]any{
			map[string]any{"param": "host", "operator": "contains", "value": "deny"},
		}}}},
	}
	p := NewWAFPlugin()
	if err := p.Init(map[string]any{
		"enabled": true, "geolite_dir": dir,
		"locations": []any{
			"notamap",
			map[string]any{"target": "a", "location": "x"},
			map[string]any{"target": "a", "location": "y", "waf": "nope"},
			map[string]any{"target": "a", "location": "/bad", "waf": map[string]any{"enabled": true, "rules": "bad"}},
			map[string]any{"target": "a", "location": "/off", "waf": map[string]any{"enabled": false}},
			map[string]any{"target": "a", "location": "/", "waf": locWAF},
		},
	}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if p.rs["a|/"] == nil {
		t.Fatal("expected ruleset for a|/")
	}
	if p.rs["a|/bad"] != nil || p.rs["a|/off"] != nil {
		t.Fatal("bad/disabled locations should be skipped")
	}

	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if p.gs == nil {
		t.Fatal("expected geodb")
	}
	if err := p.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if p.gs != nil {
		t.Fatal("geodb should be nil after Stop")
	}

	// Disabled plugin: no Start work, no geodb.
	p2 := NewWAFPlugin()
	_ = p2.Init(map[string]any{"enabled": false})
	if err := p2.Start(ctx); err != nil {
		t.Fatalf("Start disabled: %v", err)
	}
	if p2.gs != nil {
		t.Fatal("disabled plugin opened geodb")
	}
	_ = p2.Stop(ctx)

	// Empty geolite dir (no files): geodir "" opens an empty geodb.
	p3 := NewWAFPlugin()
	_ = p3.Init(map[string]any{"enabled": true, "geolite_dir": ""})
	if err := p3.Start(ctx); err != nil {
		t.Fatalf("Start empty dir: %v", err)
	}
	if p3.gs == nil {
		t.Fatal("expected empty geodb")
	}
	_ = p3.Stop(ctx)

	// Missing Country DB -> Start fails.
	p4 := NewWAFPlugin()
	_ = p4.Init(map[string]any{"enabled": true, "geolite_dir": t.TempDir()})
	if err := p4.Start(ctx); err == nil {
		t.Fatal("expected error when Country DB missing")
	}
}

func TestBeforeProxy(t *testing.T) {
	rs, _ := buildRuleSet(map[string]any{
		"enabled": true,
		"rules": []any{map[string]any{
			"id": "x", "action": map[string]any{"type": "deny", "code": 418, "message": "blocked"},
			"conditions": []any{[]any{map[string]any{"param": "host", "operator": "contains", "value": "deny"}}},
		}},
	})
	p := NewWAFPlugin()
	_ = p.Init(map[string]any{"enabled": true})
	p.rs = map[string]*ruleSet{"a|/": rs}

	w := httptest.NewRecorder()
	req := requestFor("8.8.8.8:1", "deny.com", "", "GET")
	if err := p.BeforeProxy(w, req, "a", "/"); err != errWAFBlocked {
		t.Fatalf("err = %v, want errWAFBlocked", err)
	}
	if w.Header().Get("X-WAF-Block") != "true" {
		t.Fatal("missing X-WAF-Block header")
	}
	if w.Code != 418 {
		t.Fatalf("status = %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "blocked") {
		t.Fatalf("body = %q", w.Body.String())
	}

	// Unknown target|location.
	w2 := httptest.NewRecorder()
	if err := p.BeforeProxy(w2, req, "a", "/other"); err != nil {
		t.Fatalf("unknown location err = %v", err)
	}

	// Matched policy but request passes -> allow path.
	w4 := httptest.NewRecorder()
	passing := requestFor("8.8.8.8:1", "pass.com", "", "GET")
	if err := p.BeforeProxy(w4, passing, "a", "/"); err != nil {
		t.Fatalf("passing request err = %v", err)
	}

	// Default block status/message via direct writeBlock.
	w5 := httptest.NewRecorder()
	p.writeBlock(w5, req, 0, "")
	if w5.Code != http.StatusForbidden || w5.Body.String() != "Request blocked by WAF" {
		t.Fatalf("writeBlock defaults = %d %q", w5.Code, w5.Body.String())
	}

	// Disabled plugin passes everything.
	p.enabled = false
	w3 := httptest.NewRecorder()
	if err := p.BeforeProxy(w3, req, "a", "/"); err != nil {
		t.Fatalf("disabled plugin err = %v", err)
	}

	if err := p.AfterProxy(w3, req, "a", "/", nil); err != nil {
		t.Fatalf("AfterProxy err = %v", err)
	}
}
