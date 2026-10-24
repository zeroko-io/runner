// Package egress is the way out for services. Every request and socket a
// service opens to the outside reaches the runner here, through workerd's global
// outbound, with the token of its process. The runner checks the service's policy,
// connects to public addresses only, and reports each request.
package egress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

// TokenHeader carries the process token; workerd sets it on every request.
const TokenHeader = "x-zeroko-token"

// Service is what the proxy needs to know about the process making a request.
type Service struct {
	ID         string
	Deployment string
	Allow      []string // host patterns; empty: nothing
}

// Request is one outbound request or socket, reported when it ends.
type Request struct {
	Method   string
	URL      string // scheme, host and path; the query is left out
	Status   int    // 0: no response
	Duration time.Duration
	Blocked  bool
	Err      string
}

// Failed reports whether the request was blocked, failed or got a 5xx.
func (r *Request) Failed() bool { return r.Blocked || r.Err != "" || r.Status >= 500 }

// Proxy serves the egress socket.
type Proxy struct {
	lookup func(token string) (*Service, bool)
	report func(*Service, Request)
	dialer *net.Dialer
	rp     *httputil.ReverseProxy
}

var ErrBlocked = errors.New("not a public internet address")

// dial connects to a host as it is named, fully qualified: the machine's
// search domains never apply, so a name that doesn't exist can't resolve to
// some host under them (a search domain with a wildcard would catch
// every name).
func (p *Proxy) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err == nil && net.ParseIP(host) == nil && !strings.HasSuffix(host, ".") {
		addr = net.JoinHostPort(host+".", port)
	}
	return p.dialer.DialContext(ctx, network, addr)
}

func New(lookup func(token string) (*Service, bool), report func(*Service, Request)) *Proxy {
	p := &Proxy{
		lookup: lookup,
		report: report,
		dialer: &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second, Control: control, Resolver: &net.Resolver{PreferGo: true}},
	}
	p.rp = &httputil.ReverseProxy{
		Rewrite: func(*httputil.ProxyRequest) {}, // the request already holds its absolute URL
		Transport: &http.Transport{
			Proxy:                 nil,
			DialContext:           p.dial,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          256,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
			DisableCompression:    true, // pass bodies through as they are
		},
		FlushInterval: -1,
		ErrorLog:      log.New(io.Discard, "", 0),
		ModifyResponse: func(res *http.Response) error {
			if o, ok := res.Request.Context().Value(outcomeKey{}).(*outcome); ok {
				o.status = res.StatusCode
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			msg := describe(err)
			if r.Context().Err() != nil {
				msg = "canceled"
			}
			if o, ok := r.Context().Value(outcomeKey{}).(*outcome); ok {
				o.err = msg
			}
			refuse(w, http.StatusBadGateway, "error", fmt.Sprintf("%s %s failed: %s", r.Method, r.URL.Redacted(), msg))
		},
	}
	return p
}

type outcomeKey struct{}

type outcome struct {
	status int
	err    string
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	service, ok := p.lookup(r.Header.Get(TokenHeader))
	if !ok {
		http.Error(w, "unknown process", http.StatusForbidden)
		return
	}
	r.Header.Del(TokenHeader)
	if r.Method == http.MethodConnect {
		p.tunnel(w, r, service)
		return
	}
	start := time.Now()
	rec := Request{Method: r.Method, URL: r.URL.Scheme + "://" + r.URL.Host + r.URL.Path}
	defer func() {
		rec.Duration = time.Since(start)
		p.report(service, rec)
	}()
	if (r.URL.Scheme != "http" && r.URL.Scheme != "https") || r.URL.Host == "" {
		rec.Blocked, rec.Err = true, "only http and https URLs can be fetched"
		refuse(w, http.StatusBadRequest, "blocked", rec.Err)
		return
	}
	if err := check(service, r.URL.Hostname()); err != nil {
		rec.Blocked, rec.Err = true, err.Error()
		refuse(w, http.StatusForbidden, "blocked", rec.Err)
		return
	}
	o := &outcome{}
	p.rp.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), outcomeKey{}, o)))
	rec.Status, rec.Err = o.status, o.err
}

// tunnel serves a socket the service opens with connect(): workerd asks for it
// with CONNECT host:port.
func (p *Proxy) tunnel(w http.ResponseWriter, r *http.Request, service *Service) {
	start := time.Now()
	rec := Request{Method: "CONNECT", URL: r.Host}
	defer func() {
		rec.Duration = time.Since(start)
		p.report(service, rec)
	}()
	host, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		rec.Blocked, rec.Err = true, "invalid address "+r.Host
		refuse(w, http.StatusBadRequest, "blocked", rec.Err)
		return
	}
	if err := check(service, host); err != nil {
		rec.Blocked, rec.Err = true, err.Error()
		refuse(w, http.StatusForbidden, "blocked", rec.Err)
		return
	}
	upstream, err := p.dial(r.Context(), "tcp", r.Host)
	if err != nil {
		rec.Err = describe(err)
		refuse(w, http.StatusBadGateway, "error", rec.Err)
		return
	}
	defer upstream.Close()
	client, buf, err := http.NewResponseController(w).Hijack()
	if err != nil {
		rec.Err = err.Error()
		return
	}
	defer client.Close()
	if _, err := client.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n")); err != nil {
		rec.Err = err.Error()
		return
	}
	rec.Status = http.StatusOK
	var once sync.Once
	done := make(chan struct{})
	end := func() { once.Do(func() { close(done) }) }
	go func() {
		io.Copy(upstream, buf.Reader) // what the client sent after CONNECT, then the rest
		end()
	}()
	go func() {
		io.Copy(client, upstream)
		end()
	}()
	<-done
}

// check applies the service's policy to a host, and refuses addresses that are
// not public outright.
func check(service *Service, host string) error {
	if len(service.Allow) == 0 {
		return errors.New("outbound requests are disabled for this service")
	}
	if !Match(service.Allow, host) {
		return fmt.Errorf("the service's egress policy does not allow %s", host)
	}
	if ip, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil && !Allowed(ip) {
		return fmt.Errorf("%s is %w", host, ErrBlocked)
	}
	return nil
}

// refuse answers the service with an error of the proxy, marked so that the
// service can tell it from the remote server's.
func refuse(w http.ResponseWriter, status int, kind, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Zeroko-Egress", kind)
	w.WriteHeader(status)
	io.WriteString(w, "zeroko: "+msg+"\n")
}

func describe(err error) string {
	var dns *net.DNSError
	switch {
	case errors.As(err, &dns):
		if dns.IsNotFound {
			return "no such host " + dns.Name
		}
		return "could not resolve " + dns.Name
	case errors.Is(err, ErrBlocked):
		var op *net.OpError
		if errors.As(err, &op) && op.Addr != nil {
			host, _, _ := net.SplitHostPort(op.Addr.String())
			return host + " is " + ErrBlocked.Error()
		}
		return ErrBlocked.Error()
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, syscall.ETIMEDOUT):
		return "timed out"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused"
	}
	return err.Error()
}

var pattern = regexp.MustCompile(`^(\*|(\*\.)?[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*)$`)

// ValidPattern reports whether p is a host pattern: "*" (any host),
// "example.com" or "*.example.com" (its subdomains).
func ValidPattern(p string) bool { return len(p) <= 253 && pattern.MatchString(p) }

// Match reports whether host matches one of the patterns.
func Match(patterns []string, host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, p := range patterns {
		if p == "*" || p == host || (strings.HasPrefix(p, "*.") && strings.HasSuffix(host, p[1:])) {
			return true
		}
	}
	return false
}

var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // carrier-grade NAT
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"), // NAT64 can map to private IPv4
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("fec0::/10"),
}

// Allowed reports whether ip is a public unicast address.
func Allowed(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	for _, p := range blockedPrefixes {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

// control runs on every address the proxy dials, after DNS.
func control(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if ip, err := netip.ParseAddr(host); err != nil || !Allowed(ip) {
		return fmt.Errorf("%s: %w", host, ErrBlocked)
	}
	return nil
}
