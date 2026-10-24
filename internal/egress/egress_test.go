package egress

import (
	"net/netip"
	"testing"
)

func TestMatch(t *testing.T) {
	for _, c := range []struct {
		patterns []string
		host     string
		want     bool
	}{
		{[]string{"*"}, "example.com", true},
		{[]string{"example.com"}, "example.com", true},
		{[]string{"example.com"}, "Example.COM.", true},
		{[]string{"example.com"}, "api.example.com", false},
		{[]string{"*.example.com"}, "api.example.com", true},
		{[]string{"*.example.com"}, "example.com", false},
		{[]string{"*.example.com"}, "badexample.com", false},
		{nil, "example.com", false},
	} {
		if got := Match(c.patterns, c.host); got != c.want {
			t.Errorf("Match(%v, %q) = %v", c.patterns, c.host, got)
		}
	}
}

func TestValidPattern(t *testing.T) {
	for p, want := range map[string]bool{
		"*": true, "example.com": true, "*.example.com": true, "localhost": true, "1.2.3.4": true,
		"": false, "*.": false, "a.*.com": false, "http://x.com": false, "x.com/path": false, "-x.com": false, "Example.com": false,
	} {
		if ValidPattern(p) != want {
			t.Errorf("ValidPattern(%q) != %v", p, want)
		}
	}
}

func TestAllowed(t *testing.T) {
	for ip, want := range map[string]bool{
		"1.1.1.1": true, "2606:4700:4700::1111": true,
		"127.0.0.1": false, "10.0.0.1": false, "192.168.1.1": false, "172.16.0.1": false, "169.254.169.254": false,
		"100.64.0.1": false, "::1": false, "fd00::1": false, "::ffff:10.0.0.1": false, "0.0.0.0": false,
	} {
		if Allowed(netip.MustParseAddr(ip)) != want {
			t.Errorf("Allowed(%s) != %v", ip, want)
		}
	}
}
