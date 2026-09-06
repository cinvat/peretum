package waf

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/cinvat/peretum/plugins/base"
	"k8s.io/klog/v2"
)

// Action types mirroring the Lua actions/handler.
const (
	actionDeny  = "deny"
	actionAllow = "allow"
	actionLog   = "log"
)

var errWAFBlocked = fmt.Errorf("waf: request blocked")

// WAFPlugin is the per-location Web Application Firewall. Rules live inline
// under each target location; the GeoLite databases are shared globally.
type WAFPlugin struct {
	*base.BasePlugin
	enabled bool
	geodir  string
	rs      map[string]*ruleSet
	gs      *geodb
}

// NewWAFPlugin returns a fresh WAF plugin instance.
func NewWAFPlugin() *WAFPlugin {
	return &WAFPlugin{BasePlugin: base.NewBasePlugin("waf")}
}

// Init parses the plugin config, which carries a global geolite directory and
// the per-location WAF policies that this proxy serves.
func (p *WAFPlugin) Init(config map[string]any) error {
	p.BasePlugin.Init(config)
	p.enabled = base.GetBool(config, "enabled")
	p.geodir = base.GetString(config, "geolite_dir")
	p.rs = make(map[string]*ruleSet)

	locs, _ := config["locations"].([]any)
	for _, raw := range locs {
		loc, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		target := getString(loc, "target")
		location := getString(loc, "location")
		wafCfg, ok := loc["waf"].(map[string]any)
		if !ok {
			continue
		}
		rs, err := buildRuleSet(wafCfg)
		if err != nil {
			klog.Warningf("waf: skipping %s|%s: %v", target, location, err)
			continue
		}
		if rs != nil {
			p.rs[target+"|"+location] = rs
		}
	}
	return nil
}

// Start opens the shared GeoLite databases.
func (p *WAFPlugin) Start(ctx context.Context) error {
	if !p.enabled {
		return nil
	}
	gs, err := openGeodb(p.geodir)
	if err != nil {
		return err
	}
	p.gs = gs
	if p.geodir != "" && p.gs.country != nil {
		klog.Infof("waf: geolite country database loaded from %s", p.geodir)
	}
	return nil
}

// Stop closes the GeoLite readers.
func (p *WAFPlugin) Stop(ctx context.Context) error {
	if p.gs != nil {
		p.gs.close()
		p.gs = nil
	}
	return nil
}

// BeforeProxy enforces the per-location WAF policy for target/location.
func (p *WAFPlugin) BeforeProxy(w http.ResponseWriter, r *http.Request, target, location string) error {
	if !p.enabled {
		return nil
	}
	rs, ok := p.rs[target+"|"+location]
	if !ok {
		return nil
	}
	action, rule := rs.evaluate(r, p.gs)
	if action == ruleSetActionBlock {
		status, message := blockConfig(rule)
		p.writeBlock(w, r, status, message)
		return errWAFBlocked
	}
	return nil
}

// AfterProxy is required by the RequestHook interface; this plugin does not
// inspect responses.
func (p *WAFPlugin) AfterProxy(w http.ResponseWriter, r *http.Request, target, location string, resp *http.Response) error {
	return nil
}

// writeBlock emits a plain text block page; the frontend error_page plugin may
// render a branded page on top of the status code afterwards.
func (p *WAFPlugin) writeBlock(w http.ResponseWriter, r *http.Request, status int, message string) {
	if status == 0 {
		status = http.StatusForbidden
	}
	if message == "" {
		message = "Request blocked by WAF"
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-WAF-Block", "true")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, message)
	klog.Warningf("waf: blocked %s %s from %s: %s", r.Method, r.URL.RequestURI(), clientIP(r), message)
}

// clientIP resolves the client address using the same precedence as the Lua
// WAF context: X-Real-IP, then the first X-Forwarded-For hop, then the TCP
// remote address.
func clientIP(r *http.Request) string {
	if x := r.Header.Get("X-Real-IP"); x != "" {
		return strings.TrimSpace(x)
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i > 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

// requestCtx lazily extracts the request fields a policy references so each
// value (including the body) is read at most once.
type requestCtx struct {
	r        *http.Request
	gs       *geodb
	ip       string
	geoDone  bool
	country  string
	asn      uint32
	asnOrg   string
	city     string
	bodyDone bool
	body     string
}

func newRequestCtx(r *http.Request, gs *geodb) *requestCtx {
	return &requestCtx{r: r, gs: gs, ip: clientIP(r)}
}

func (c *requestCtx) ensureGeo() {
	if c.geoDone {
		return
	}
	c.geoDone = true
	if c.gs == nil {
		return
	}
	ip := net.ParseIP(strings.TrimSpace(c.ip))
	if ip == nil {
		return
	}
	c.country, c.asn, c.asnOrg, c.city = c.gs.lookup(ip)
}

func (c *requestCtx) ensureBody() string {
	if c.bodyDone {
		return c.body
	}
	c.bodyDone = true
	if c.r.Body == nil {
		return ""
	}
	b, err := io.ReadAll(c.r.Body)
	if err != nil {
		return ""
	}
	c.body = string(b)
	return c.body
}

// get resolves a condition parameter. Values mirror the Lua parameters table
// plus geo extensions (country, asn, asn_org, city).
func (c *requestCtx) get(param, name string) (string, bool) {
	switch param {
	case "host":
		return c.r.Host, c.r.Host != ""
	case "user_agent":
		v := c.r.Header.Get("User-Agent")
		return v, v != ""
	case "referer":
		v := c.r.Header.Get("Referer")
		return v, v != ""
	case "cookie":
		v := c.r.Header.Get("Cookie")
		return v, v != ""
	case "url":
		return c.r.URL.RequestURI(), true
	case "path":
		return c.r.URL.Path, true
	case "query":
		return c.r.URL.RawQuery, true
	case "method":
		return c.r.Method, true
	case "ip":
		return c.ip, c.ip != ""
	case "country":
		c.ensureGeo()
		return c.country, c.country != ""
	case "asn":
		c.ensureGeo()
		if c.asn == 0 {
			return "", false
		}
		return strconv.FormatUint(uint64(c.asn), 10), true
	case "asn_org":
		c.ensureGeo()
		return c.asnOrg, c.asnOrg != ""
	case "city":
		c.ensureGeo()
		return c.city, c.city != ""
	case "body":
		return c.ensureBody(), len(c.body) > 0
	case "arg":
		v := c.r.URL.Query().Get(name)
		return v, v != ""
	case "header":
		v := c.r.Header.Get(name)
		return v, v != ""
	default:
		return "", false
	}
}

// ruleSetAction is the enforcement decision.
type ruleSetAction int

const (
	ruleSetActionAllow ruleSetAction = iota
	ruleSetActionBlock
)

// ruleSet is a compiled per-location WAF policy.
type ruleSet struct {
	params    map[string]*paramIndex
	rules     []*compiledRule
	condCount int
}

// compiledRule is a rule with its OR-of-AND-groups of condition ids. The
// action is self-contained: deny rules carry their own status and message.
type compiledRule struct {
	id      string
	name    string
	enabled bool
	action  string
	code    int
	message string
	groups  [][]int
}

// paramIndex bundles all conditions that reference one parameter so a single
// scrape of the request field drives every operator. `contains` patterns are
// merged into one Aho-Corasick automaton so the field is scanned once in
// total regardless of how many signature rules exist.
type paramIndex struct {
	name             string
	containsPatterns []string
	containsConds    []int
	aho              *ahoMatcher
	equals           map[string]int
	prefixes         []prefixCond
	suffixes         []suffixCond
	regexes          []regexCond
	ins              []inCond
	numGt            []numCond
	numLt            []numCond
	ipNets           []ipCond
	exists           []int
	notExists        []int
}

type prefixCond struct {
	prefix string
	condID int
}
type suffixCond struct {
	suffix string
	condID int
}
type regexCond struct {
	re     *regexp.Regexp
	condID int
}
type inCond struct {
	values map[string]struct{}
	negate bool
	condID int
}
type numCond struct {
	threshold float64
	condID    int
}

type ipCond struct {
	nets   []*net.IPNet
	negate bool
	condID int
}

// isIPOperator returns true when the operator should be interpreted as a
// network-containment check against the effective client IP.
func isIPOperator(op string) bool {
	switch strings.ToLower(op) {
	case "equals", "in", "not_in", "in_ip", "not_in_ip":
		return true
	}
	return false
}

// addIPCond parses a comma-separated list of IP addresses or CIDR blocks and
// registers a network-containment condition.
func addIPCond(idx *paramIndex, value string, negate bool, condID int) {
	nets := parseIPNets(value)
	if len(nets) == 0 {
		return
	}
	idx.ipNets = append(idx.ipNets, ipCond{nets: nets, negate: negate, condID: condID})
}

// buildRuleSet compiles the per-location WAF configuration.
func buildRuleSet(cfg map[string]any) (*ruleSet, error) {
	if !getBool(cfg, "enabled") {
		return nil, nil
	}
	rs := &ruleSet{
		params: make(map[string]*paramIndex),
	}

	rules, ok := cfg["rules"].([]any)
	if !ok {
		if _, present := cfg["rules"]; present {
			return nil, fmt.Errorf("waf: rules must be a list")
		}
		return rs, nil
	}
	for _, rawRule := range rules {
		rm, ok := rawRule.(map[string]any)
		if !ok {
			continue
		}
		rule := &compiledRule{
			id:      getString(rm, "id"),
			name:    getString(rm, "name"),
			enabled: true,
			action:  actionDeny,
		}
		if e, ok := rm["enabled"].(bool); ok {
			rule.enabled = e
		}
		if am, ok := rm["action"].(map[string]any); ok {
			rule.action = getString(am, "type")
			if rule.action == "" {
				rule.action = actionDeny
			}
			rule.code = getInt(am, "code")
			rule.message = getString(am, "message")
		}

		groups, _ := rm["conditions"].([]any)
		for _, rawGroup := range groups {
			conds, ok := rawGroup.([]any)
			if !ok {
				continue
			}
			group := make([]int, 0, len(conds))
			for _, rawCond := range conds {
				cm, ok := rawCond.(map[string]any)
				if !ok {
					continue
				}
				param := getString(cm, "param")
				if param == "" {
					continue
				}
				op := getString(cm, "operator")
				value := getString(cm, "value")
				if op == "in" {
					op = "In"
				}
				idx, ok := rs.params[param]
				if !ok {
					idx = &paramIndex{equals: make(map[string]int)}
					rs.params[param] = idx
				}
				if name := getString(cm, "param_name"); name != "" {
					idx.name = name
				}
				group = append(group, rs.condCount)
				if param == "ip" && isIPOperator(op) {
					addIPCond(idx, value, strings.ToLower(op) == "not_in" || strings.ToLower(op) == "not_in_ip", rs.condCount)
				} else {
					compileCondition(idx, op, value, rs.condCount)
				}
				rs.condCount++
			}
			if len(group) > 0 {
				rule.groups = append(rule.groups, group)
			}
		}
		if len(rule.groups) > 0 {
			rs.rules = append(rs.rules, rule)
		}
	}
	finalizeIndexes(rs)
	return rs, nil
}

// compileCondition wires one condition into its parameter index.
func compileCondition(idx *paramIndex, op, value string, condID int) {
	switch strings.ToLower(op) {
	case "contains":
		idx.containsPatterns = append(idx.containsPatterns, strings.ToLower(value))
		idx.containsConds = append(idx.containsConds, condID)
	case "equals":
		idx.equals[value] = condID
	case "startswith":
		idx.prefixes = append(idx.prefixes, prefixCond{prefix: value, condID: condID})
	case "endswith":
		idx.suffixes = append(idx.suffixes, suffixCond{suffix: value, condID: condID})
	case "matches":
		if re, err := regexp.Compile(value); err == nil {
			idx.regexes = append(idx.regexes, regexCond{re: re, condID: condID})
		}
	case "in", "not_in":
		negate := strings.ToLower(op) == "not_in"
		vals := make(map[string]struct{})
		for _, item := range strings.Split(value, ",") {
			if v := strings.TrimSpace(item); v != "" {
				vals[v] = struct{}{}
			}
		}
		idx.ins = append(idx.ins, inCond{values: vals, negate: negate, condID: condID})
	case "gt":
		if th, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
			idx.numGt = append(idx.numGt, numCond{threshold: th, condID: condID})
		}
	case "lt":
		if th, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
			idx.numLt = append(idx.numLt, numCond{threshold: th, condID: condID})
		}
	case "exists":
		idx.exists = append(idx.exists, condID)
	case "not_exists":
		idx.notExists = append(idx.notExists, condID)
	}
}

// finalizeIndexes merges all contains patterns of a parameter into a single
// Aho-Corasick automaton. Duplicate pattern strings share one automaton entry
// so the field is scanned once.
func finalizeIndexes(rs *ruleSet) {
	for _, idx := range rs.params {
		if len(idx.containsPatterns) == 0 {
			continue
		}
		patternMap := make(map[string]int)
		var patterns []string
		var condMap [][]int
		for i, p := range idx.containsPatterns {
			pid, seen := patternMap[p]
			if !seen {
				pid = len(patterns)
				patternMap[p] = pid
				patterns = append(patterns, p)
				condMap = append(condMap, nil)
			}
			condMap[pid] = append(condMap[pid], idx.containsConds[i])
		}
		idx.aho = buildAhoMatcher(patterns, condMap)
	}
}

// evaluate runs the policy against one request and returns the enforcement
// decision plus the matched deny rule (for rule-driven blocks) or nil.
func (rs *ruleSet) evaluate(r *http.Request, gs *geodb) (ruleSetAction, *compiledRule) {
	ctx := newRequestCtx(r, gs)

	hits := make([]bool, rs.condCount)
	for param, idx := range rs.params {
		value, present := ctx.get(param, idx.name)
		applyIndex(idx, hits, value, present)
	}

	for _, rule := range rs.rules {
		if !rule.enabled || !ruleMatches(rule, hits) {
			continue
		}
		switch rule.action {
		case actionAllow:
			return ruleSetActionAllow, rule
		case actionLog:
			klog.Infof("waf: log rule %q matched for %s", rule.id, ctx.ip)
			continue
		case actionDeny:
			return ruleSetActionBlock, rule
		}
	}
	return ruleSetActionAllow, nil
}

// ruleMatches implements the OR-of-AND-groups semantics from the Lua engine.
func ruleMatches(rule *compiledRule, hits []bool) bool {
	for _, group := range rule.groups {
		all := true
		for _, cid := range group {
			if cid >= len(hits) || !hits[cid] {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// applyIndex marks every condition of a parameter that the observed value
// satisfies, scanning the value once across all conditions of that parameter.
func applyIndex(idx *paramIndex, hits []bool, value string, present bool) {
	for _, cid := range idx.exists {
		if present {
			hits[cid] = true
		}
	}
	for _, cid := range idx.notExists {
		if !present {
			hits[cid] = true
		}
	}
	if !present {
		return
	}

	if idx.aho != nil {
		lower := strings.ToLower(value)
		for _, cid := range idx.aho.match([]byte(lower)) {
			hits[cid] = true
		}
	}
	if cid, ok := idx.equals[value]; ok {
		hits[cid] = true
	}
	for _, c := range idx.prefixes {
		if strings.HasPrefix(value, c.prefix) {
			hits[c.condID] = true
		}
	}
	for _, c := range idx.suffixes {
		if strings.HasSuffix(value, c.suffix) {
			hits[c.condID] = true
		}
	}
	for _, c := range idx.regexes {
		if c.re.MatchString(value) {
			hits[c.condID] = true
		}
	}
	for _, c := range idx.ins {
		_, in := c.values[value]
		if in != c.negate {
			hits[c.condID] = true
		}
	}
	if ip := net.ParseIP(strings.TrimSpace(value)); ip != nil {
		for _, c := range idx.ipNets {
			inside := false
			for _, n := range c.nets {
				if n.Contains(ip) {
					inside = true
					break
				}
			}
			if inside != c.negate {
				hits[c.condID] = true
			}
		}
	}
	if num, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
		for _, c := range idx.numGt {
			if num > c.threshold {
				hits[c.condID] = true
			}
		}
		for _, c := range idx.numLt {
			if num < c.threshold {
				hits[c.condID] = true
			}
		}
	}
}

// blockConfig resolves the block status and message from the matched rule,
// defaulting to a plain 403 page when the rule omits them.
func blockConfig(rule *compiledRule) (int, string) {
	status := 403
	message := "Request blocked by WAF"
	if rule != nil {
		if rule.code != 0 {
			status = rule.code
		}
		if rule.message != "" {
			message = rule.message
		}
	}
	return status, message
}
