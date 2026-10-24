package server

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/zeroko-io/runner/internal/platform"
	"github.com/zeroko-io/runner/internal/realtime"
)

// Hop-by-hop headers, which a proxy does not forward.
var hopHeaders = []string{"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection", "Te", "Trailer", "Transfer-Encoding", "Upgrade"}

func stripHop(h http.Header) {
	for _, f := range h.Values("Connection") {
		for _, name := range strings.Split(f, ",") {
			h.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range hopHeaders {
		h.Del(name)
	}
}

// serveService proxies a request to the service's process. A response from the
// service's realtime subscribe() turns the request into a realtime connection.
func (s *Server) serveService(w http.ResponseWriter, r *http.Request, name string) {
	ctx := r.Context()
	service, err := s.p.Service(ctx, name)
	if err != nil {
		if errors.Is(err, platform.ErrNotFound) {
			writeError(w, http.StatusNotFound, "service_not_found", "No service is named "+name+".")
		} else {
			s.fail(w, err)
		}
		return
	}
	if service.Stopped {
		writeError(w, http.StatusServiceUnavailable, "service_stopped", "The service is stopped.")
		return
	}
	if service.ActiveDeployment == "" {
		writeError(w, http.StatusNotFound, "service_not_deployed", "The service has no deployment.")
		return
	}
	// Every request that reaches the service is counted and logged once done.
	start, status, deployment := time.Now(), 0, service.ActiveDeployment
	defer func() {
		if status != 0 {
			s.p.Request(service.ID, deployment, r.Method, r.URL.Path, status, time.Since(start))
		}
	}()
	proc, err := s.p.RT.Get(ctx, service.ID)
	if err != nil {
		if ctx.Err() == nil {
			s.p.ServiceLog(service.ID, service.ActiveDeployment, "runtime", "error", "the service failed to start: "+err.Error())
			status = http.StatusServiceUnavailable
			writeError(w, status, "service_unavailable", "The service failed to start. Check its logs.")
		}
		return
	}
	deployment = proc.DeploymentID
	var once sync.Once
	end := proc.Begin()
	done := func() { once.Do(end) }
	defer done()

	out := r.Clone(ctx)
	out.RequestURI = ""
	out.URL.Scheme = "http"
	out.URL.Host = r.Host
	out.Body = http.MaxBytesReader(w, r.Body, s.p.Cfg.MaxBodyBytes())
	if r.ContentLength == 0 {
		out.Body = nil
	}
	stripHop(out.Header)
	for k := range out.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-zeroko-") {
			out.Header.Del(k)
		}
	}
	if caller, ok := ctx.Value(callerKey{}).(string); ok {
		out.Header.Set("X-Zeroko-From", caller) // from another service, through a binding
	} else if prior := r.Header.Get("X-Forwarded-For"); prior != "" {
		out.Header.Set("X-Forwarded-For", prior+", "+clientIP(r))
	} else {
		out.Header.Set("X-Forwarded-For", clientIP(r))
	}
	out.Header.Set("X-Forwarded-Proto", s.p.Cfg.PublicScheme)

	resp, err := proc.Transport().RoundTrip(out)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			status = http.StatusRequestEntityTooLarge
			writeError(w, status, "body_too_large", "The request body is too large.")
			return
		}
		status = http.StatusBadGateway
		writeError(w, status, "service_unreachable", "The service did not answer. Check its logs.")
		return
	}
	defer resp.Body.Close()

	if sub := resp.Header.Get("x-zeroko-realtime"); sub != "" {
		subscription, ok := realtime.ParseSubscription(sub)
		if _, internal := ctx.Value(callerKey{}).(string); internal {
			status = http.StatusBadGateway
			writeError(w, status, "invalid_subscription", "Realtime connections come from clients, not from other services.")
			return
		}
		if !ok {
			status = http.StatusBadGateway
			writeError(w, status, "invalid_subscription", "The service's subscription is invalid.")
			return
		}
		done() // a realtime connection does not keep the process busy
		status = http.StatusSwitchingProtocols
		if !realtime.IsWebSocket(r) {
			status = http.StatusOK
		}
		s.p.Request(service.ID, deployment, r.Method, r.URL.Path, status, time.Since(start))
		status = 0 // logged above: the connection may stay open for hours
		s.p.Hub.Serve(w, r, service.ID, subscription)
		return
	}

	stripHop(resp.Header)
	h := w.Header()
	for k, v := range resp.Header {
		h[k] = v
	}
	status = resp.StatusCode
	w.WriteHeader(status)
	copyFlushing(w, resp.Body)
}

// copyFlushing copies a response body, flushing after every read so that
// streamed responses reach the client as they are produced.
func copyFlushing(w http.ResponseWriter, body io.Reader) {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32<<10)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if err != nil {
			return
		}
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
