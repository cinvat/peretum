package router

import "testing"

func TestNormalizeHost(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"plain hostname", "api.example.com", "api.example.com"},
		{"strips port", "api.example.com:8080", "api.example.com"},
		{"lowercases", "API.Example.COM", "api.example.com"},
		{"trims surrounding space", "  api.example.com  ", "api.example.com"},
		{"strips trailing dot", "api.example.com.", "api.example.com"},
		{"port and trailing dot", "api.example.com.:8080", "api.example.com"},

		// A bare IPv6 literal has several colons and carries no port, so it
		// must survive normalization intact.
		{"bare ipv6", "2001:db8::1", "2001:db8::1"},
		{"bracketed ipv6", "[2001:db8::1]", "[2001:db8::1]"},
		{"bracketed ipv6 with port", "[2001:db8::1]:8443", "[2001:db8::1]"},
		{"bracketed ipv6 loopback with port", "[::1]:8443", "[::1]"},
		{"unterminated bracket", "[2001:db8::1", "[2001:db8::1"},

		{"empty", "", ""},
		{"only spaces", "   ", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeHost(tc.in); got != tc.want {
				t.Fatalf("NormalizeHost(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
