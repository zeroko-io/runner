package server

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const tokenPrefix = "zk_"

func hashSecret(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func newSecret() string { return strings.ToLower(rand.Text() + rand.Text()) }

// authed wraps an API handler: it takes a bearer token, the root token from
// ZEROKO_TOKEN (the app that manages the runner) or one made through the API
// (agents). Calls that change something go to the runner's log.
func (s *Server) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || token == "" {
			writeError(w, http.StatusUnauthorized, "unauthorized", "Use Authorization: Bearer <token>.")
			return
		}
		var by string
		if subtle.ConstantTimeCompare(hashBytes(token), s.root[:]) == 1 {
			by = "root"
		} else {
			name, valid, err := "", false, error(nil)
			if strings.HasPrefix(token, tokenPrefix) {
				name, valid, err = s.p.Store.UseToken(r.Context(), hashSecret(token))
			}
			if err != nil {
				s.fail(w, err)
				return
			}
			if !valid {
				s.log.Warn("api token refused", "ip", clientIP(r))
				writeError(w, http.StatusUnauthorized, "unauthorized", "Unknown or revoked token.")
				return
			}
			by = "token " + name
		}
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			next(w, r)
			return
		}
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next(sw, r)
		level := slog.LevelInfo
		if sw.status >= 500 {
			level = slog.LevelError
		} else if sw.status >= 400 {
			level = slog.LevelWarn
		}
		s.log.Log(r.Context(), level, "api", "method", r.Method, "path", r.URL.Path, "status", sw.status,
			"ms", time.Since(start).Milliseconds(), "by", by, "ip", clientIP(r))
	}
}

func hashBytes(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}
