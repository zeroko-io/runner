// Package server is the runner's HTTP side: the gateway that serves each
// service at its host, the API for the app that manages the runner, and the
// control API the bindings in service processes call.
package server

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/zeroko-io/runner/internal/platform"
	"github.com/zeroko-io/runner/internal/store"
)

type Server struct {
	p    *platform.Platform
	log  *slog.Logger
	root [32]byte // SHA-256 of the root token

	api     http.Handler
	control http.Handler
}

// New makes the runner's handlers; rootToken is the token of the app that
// manages the runner.
func New(p *platform.Platform, rootToken string, log *slog.Logger) *Server {
	s := &Server{p: p, log: log, root: sha256.Sum256([]byte(rootToken))}
	s.api = s.apiRoutes()
	s.control = s.controlRoutes()
	return s
}

// Gateway serves services: <name><separator><domain> is the service of that
// name.
func (s *Server) Gateway() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := strings.ToLower(r.Host)
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		name := s.p.Cfg.ServiceOf(host)
		if name == "" {
			writeError(w, http.StatusNotFound, "unknown_host", "No service is served at this host.")
			return
		}
		s.serveService(w, r, name)
	})
}

// API serves the API, its MCP server at /mcp, and /healthz, at any host.
func (s *Server) API() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/healthz":
			w.Write([]byte("ok\n"))
		case strings.HasPrefix(r.URL.Path, "/api/"):
			s.api.ServeHTTP(w, r)
		case r.URL.Path == "/mcp":
			s.authed(s.mcp)(w, r)
		default:
			writeError(w, http.StatusNotFound, "not_found", "The API is at /api.")
		}
	})
}

// Control is the handler for the control socket.
func (s *Server) Control() http.Handler { return s.control }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

// fail writes err as the right API error.
func (s *Server) fail(w http.ResponseWriter, err error) {
	var inv platform.Invalid
	switch {
	case errors.As(err, &inv):
		writeError(w, http.StatusBadRequest, "invalid", inv.Error())
	case errors.Is(err, platform.ErrNotFound), errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "Not found.")
	case errors.Is(err, platform.ErrStopped):
		writeError(w, http.StatusConflict, "service_stopped", "The service is stopped.")
	default:
		s.log.Error("request failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal", "Internal error.")
	}
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
