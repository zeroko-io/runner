package server

import (
	"net/http/httptest"
	"testing"
)

func TestClient(t *testing.T) {
	cases := []struct {
		remote, forwarded, want string
	}{
		{"203.0.113.7:4242", "", "203.0.113.7"},
		{"203.0.113.7:4242", "198.51.100.9", "203.0.113.7"}, // a client's own word
		{"172.18.0.5:4242", "198.51.100.9", "198.51.100.9"}, // a proxy's
		{"127.0.0.1:4242", "10.0.0.1, 198.51.100.9", "198.51.100.9"},
		{"[::1]:4242", "198.51.100.9", "198.51.100.9"},
		{"172.18.0.5:4242", "", "172.18.0.5"},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = c.remote
		if c.forwarded != "" {
			r.Header.Set("X-Forwarded-For", c.forwarded)
		}
		if got := client(r); got != c.want {
			t.Errorf("client(%s, %q) = %q, want %q", c.remote, c.forwarded, got, c.want)
		}
	}
}

func TestLimiter(t *testing.T) {
	l := newLimiter()
	for i := range 4 { // a burst of twice the rate
		if !l.allow("a", 2) {
			t.Fatalf("request %d refused", i)
		}
	}
	if l.allow("a", 2) {
		t.Error("a fifth request at once was let through")
	}
	if !l.allow("b", 2) {
		t.Error("another client was held to a's bucket")
	}
}
