package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/zeroko-io/runner/internal/data"
	"github.com/zeroko-io/runner/internal/deploy"
	"github.com/zeroko-io/runner/internal/platform"
	"github.com/zeroko-io/runner/internal/runtime"
)

type procKey struct{}

// callerKey holds the name of the service a request through a service
// binding comes from.
type callerKey struct{}

// hopsKey is how many service bindings a call went through.
type hopsKey struct{}

// controlRoutes serve the bindings inside service processes. The process token
// identifies the service; the binding name comes from the process's config, so
// a service reaches only its own data.
func (s *Server) controlRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /db/{binding}/batch", s.ctlBatch)
	mux.HandleFunc("POST /db/{binding}/lock", s.ctlLock)
	mux.HandleFunc("POST /db/{binding}/unlock", s.ctlUnlock)
	mux.HandleFunc("GET /files/{binding}/list", s.ctlListFiles)
	mux.HandleFunc("GET /files/{binding}", s.ctlGetFile)
	mux.HandleFunc("PUT /files/{binding}", s.ctlPutFile)
	mux.HandleFunc("DELETE /files/{binding}", s.ctlDeleteFile)
	mux.HandleFunc("POST /jobs/send", s.ctlSendJob)
	mux.HandleFunc("POST /jobs/cancel", s.ctlCancelJob)
	mux.HandleFunc("POST /realtime/publish", s.ctlPublish)
	mux.HandleFunc("POST /realtime/send", s.ctlSend)
	mux.HandleFunc("POST /realtime/connections", s.ctlConnections)
	mux.HandleFunc("GET /assets", s.ctlAssets)
	mux.HandleFunc("/call/{service}/{path...}", s.ctlCall)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proc, ok := s.p.RT.ProcForToken(r.Header.Get("x-zeroko-token"))
		if !ok {
			ctlError(w, http.StatusForbidden, "unknown process")
			return
		}
		mux.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), procKey{}, proc)))
	})
}

func ctlProc(r *http.Request) *runtime.Proc { return r.Context().Value(procKey{}).(*runtime.Proc) }

func ctlService(r *http.Request) string { return ctlProc(r).ServiceID }

// ctlAssets serves a file of the calling process's deployment.
func (s *Server) ctlAssets(w http.ResponseWriter, r *http.Request) {
	proc := ctlProc(r)
	site, err := s.p.Site(r.Context(), proc.ServiceID, proc.DeploymentID)
	if err != nil {
		s.ctlFail(w, err)
		return
	}
	name, a, ok := site.Find(r.URL.Query().Get("path"))
	if !ok {
		ctlError(w, http.StatusNotFound, "not found")
		return
	}
	f, err := site.Open(name)
	if err != nil {
		s.ctlFail(w, err)
		return
	}
	defer f.Close()
	h := w.Header()
	h.Set("Content-Type", a.Type)
	h.Set("ETag", a.ETag)
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, "", time.Time{}, f)
}

// ctlCall sends a request to the service a binding of the caller names, as
// the gateway would, with X-Zeroko-From set to the caller.
func (s *Server) ctlCall(w http.ResponseWriter, r *http.Request) {
	ctx, proc, target := r.Context(), ctlProc(r), r.PathValue("service")
	d, err := s.p.Store.Deployment(ctx, proc.ServiceID, proc.DeploymentID)
	if err != nil {
		s.ctlFail(w, err)
		return
	}
	if !slices.Contains(slices.Collect(maps.Values(s.p.Manifest(d).Bindings)), deploy.ServicePrefix+target) {
		ctlError(w, http.StatusForbidden, "no binding to "+target)
		return
	}
	caller, err := s.p.Store.ServiceByID(ctx, proc.ServiceID)
	if err != nil {
		s.ctlFail(w, err)
		return
	}
	// How many bindings the request went through to get here, as the
	// caller's main.js passes it on: past maxHops, the services are calling
	// each other in a cycle.
	hops, _ := strconv.Atoi(r.Header.Get("X-Zeroko-Hops"))
	hops++
	if hops > maxHops {
		ctlError(w, http.StatusLoopDetected, fmt.Sprintf("%d calls from service to service: the services call each other in a cycle", hops))
		return
	}
	ctx = context.WithValue(ctx, hopsKey{}, hops)
	out := r.Clone(context.WithValue(ctx, callerKey{}, caller.Name))
	out.URL.Path, out.URL.RawPath = "/"+r.PathValue("path"), ""
	out.Host = s.p.Cfg.ServiceHost(target)
	s.serveService(w, out, target)
}

func ctlError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// ctlFail reports errors the service caused as 400 with their message, and
// hides the platform's.
func (s *Server) ctlFail(w http.ResponseWriter, err error) {
	var sqlErr data.SQLError
	var inv data.Invalid
	var pinv platform.Invalid
	switch {
	case errors.As(err, &sqlErr), errors.As(err, &inv), errors.As(err, &pinv):
		ctlError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, data.ErrNotFound):
		ctlError(w, http.StatusNotFound, "not found")
	default:
		s.log.Error("control request failed", "err", err)
		ctlError(w, http.StatusInternalServerError, "internal error")
	}
}

func decodeBody(w http.ResponseWriter, r *http.Request, limit int64, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit)).Decode(v); err != nil {
		ctlError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func (s *Server) ctlBatch(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Statements []data.Statement `json:"statements"`
	}
	if !decodeBody(w, r, 64<<20, &in) {
		return
	}
	res, err := s.p.Data.Batch(r.Context(), ctlService(r), r.PathValue("binding"), in.Statements)
	if err != nil {
		s.ctlFail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) ctlLock(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Key string `json:"key"`
	}
	if !decodeBody(w, r, 4096, &in) {
		return
	}
	id, err := s.p.Lock(r.Context(), ctlService(r), r.PathValue("binding"), in.Key)
	if err != nil {
		s.ctlFail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id})
}

func (s *Server) ctlUnlock(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Key string `json:"key"`
		ID  string `json:"id"`
	}
	if !decodeBody(w, r, 4096, &in) {
		return
	}
	s.p.Unlock(ctlService(r), r.PathValue("binding"), in.Key, in.ID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) ctlGetFile(w http.ResponseWriter, r *http.Request) {
	service, binding, key := ctlService(r), r.PathValue("binding"), r.URL.Query().Get("key")
	if r.URL.Query().Get("meta") != "" {
		f, err := s.p.Data.StatFile(r.Context(), service, binding, key)
		if err != nil {
			s.ctlFail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, f)
		return
	}
	serveFile(w, r, s, service, binding, key)
}

// serveFile writes a file with its type and ETag, answering ranges and
// conditional requests.
func serveFile(w http.ResponseWriter, r *http.Request, s *Server, service, binding, key string) {
	f, content, err := s.p.Data.OpenFile(r.Context(), service, binding, key)
	if err != nil {
		s.ctlFail(w, err)
		return
	}
	defer content.Close()
	w.Header().Set("Content-Type", f.Type)
	w.Header().Set("ETag", f.ETag)
	http.ServeContent(w, r, "", time.UnixMilli(f.Uploaded), content)
}

func (s *Server) ctlPutFile(w http.ResponseWriter, r *http.Request) {
	f, err := s.p.Data.PutFile(r.Context(), ctlService(r), r.PathValue("binding"), r.URL.Query().Get("key"), r.Header.Get("Content-Type"), r.Body)
	if err != nil {
		s.ctlFail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, f)
}

func (s *Server) ctlDeleteFile(w http.ResponseWriter, r *http.Request) {
	if err := s.p.Data.DeleteFile(r.Context(), ctlService(r), r.PathValue("binding"), r.URL.Query().Get("key")); err != nil {
		s.ctlFail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) ctlListFiles(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	l, err := s.p.Data.ListFiles(r.Context(), ctlService(r), r.PathValue("binding"), q.Get("prefix"), q.Get("cursor"), limit)
	if err != nil {
		s.ctlFail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, l)
}

func (s *Server) ctlSendJob(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Payload  json.RawMessage `json:"payload"`
		Delay    float64         `json:"delay"`
		Attempts int             `json:"attempts"`
		Key      string          `json:"key"`
	}
	if !decodeBody(w, r, 1<<20, &in) {
		return
	}
	j, queued, err := s.p.SendJob(r.Context(), ctlService(r), in.Payload, time.Duration(in.Delay*float64(time.Second)), in.Attempts, in.Key)
	if err != nil {
		s.ctlFail(w, err)
		return
	}
	out := map[string]any{"id": j.ID}
	if !queued {
		out["duplicate"] = true
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) ctlCancelJob(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ID  int64  `json:"id"`
		Key string `json:"key"`
	}
	if !decodeBody(w, r, 4096, &in) {
		return
	}
	ok, err := s.p.Store.CancelJob(r.Context(), ctlService(r), in.ID, in.Key)
	if err != nil {
		s.ctlFail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ok)
}

func (s *Server) ctlPublish(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Channel string `json:"channel"`
		Data    string `json:"data"`
	}
	if !decodeBody(w, r, 2<<20, &in) {
		return
	}
	if in.Channel == "" {
		ctlError(w, http.StatusBadRequest, "publish() needs a channel")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"delivered": s.p.Hub.Publish(ctlService(r), in.Channel, in.Data)})
}

func (s *Server) ctlSend(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Connection string `json:"connection"`
		Data       string `json:"data"`
	}
	if !decodeBody(w, r, 2<<20, &in) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"sent": s.p.Hub.Send(ctlService(r), in.Connection, in.Data)})
}

func (s *Server) ctlConnections(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Channel string `json:"channel"`
	}
	if !decodeBody(w, r, 4096, &in) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"connections": s.p.Hub.Members(ctlService(r), in.Channel)})
}
