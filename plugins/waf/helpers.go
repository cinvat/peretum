package waf

import (
	"net"
	"strings"
)

func getString(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getBool(m map[string]any, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func getInt(m map[string]any, key string) int {
	switch v := m[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	case uint32:
		return int(v)
	}
	return 0
}

// parseIPNets splits a comma-separated rule value into IP addresses and CIDR
// blocks, skipping entries that fail to parse.
func parseIPNets(value string) []*net.IPNet {
	var nets []*net.IPNet
	for _, item := range strings.Split(value, ",") {
		if n := parseCIDR(item); n != nil {
			nets = append(nets, n)
		}
	}
	return nets
}

// parseCIDR accepts a plain IP (host net becomes a /32 or /128 host route) or
// a CIDR block.
func parseCIDR(s string) *net.IPNet {
	s = strings.TrimSpace(s)
	if !strings.Contains(s, "/") {
		ip := net.ParseIP(s)
		if ip == nil {
			return nil
		}
		bits := 32
		if ip.To4() == nil {
			bits = 128
		}
		return &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)}
	}
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		return nil
	}
	return n
}
