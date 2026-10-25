package server

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/zeroko-io/runner/internal/data"
	"github.com/zeroko-io/runner/internal/deploy"
	"github.com/zeroko-io/runner/internal/platform"
	"github.com/zeroko-io/runner/internal/runtime"
	"github.com/zeroko-io/runner/internal/store"
)

func (s *Server) apiRoutes() http.Handler {
	mux := http.NewServeMux()
	a := func(pattern string, h http.HandlerFunc) { mux.HandleFunc(pattern, s.authed(h)) }
	service := func(pattern string, h func(http.ResponseWriter, *http.Request, *store.Service)) {
		a(pattern, func(w http.ResponseWriter, r *http.Request) {
			service, err := s.p.Service(r.Context(), r.PathValue("service"))
			if err != nil {
				s.fail(w, err)
				return
			}
			h(w, r, service)
		})
	}

	a("GET /api/services", s.listServices)
	a("POST /api/services", s.createService)
	service("GET /api/services/{service}", s.getService)
	service("PATCH /api/services/{service}", s.patchService)
	service("DELETE /api/services/{service}", s.deleteService)
	service("POST /api/services/{service}/stop", func(w http.ResponseWriter, r *http.Request, service *store.Service) {
		s.setStopped(w, r, service, true)
	})
	service("POST /api/services/{service}/start", func(w http.ResponseWriter, r *http.Request, service *store.Service) {
		s.setStopped(w, r, service, false)
	})

	a("POST /api/services/{service}/deployments", s.deploy)
	service("GET /api/services/{service}/deployments", s.listDeployments)
	service("POST /api/services/{service}/deployments/{id}/activate", s.activate)
	service("GET /api/services/{service}/deployments/{id}/source", s.source)
	service("GET /api/services/{service}/deployments/{id}/bundle", s.bundle)
	service("GET /api/services/{service}/metrics", s.metrics)
	service("POST /api/services/{service}/crons/run", s.runCron)

	service("GET /api/services/{service}/storage", s.storage)
	service("DELETE /api/services/{service}/storage", s.deleteStorage)
	service("DELETE /api/services/{service}/storage/{type}/{binding}", s.deleteStore)
	service("GET /api/services/{service}/export", s.exportData)
	service("POST /api/services/{service}/import", s.importData)

	service("GET /api/services/{service}/secrets", s.listSecrets)
	service("PUT /api/services/{service}/secrets/{name}", s.putSecret)
	service("DELETE /api/services/{service}/secrets/{name}", s.deleteSecret)

	service("GET /api/services/{service}/vars", s.listVars)
	service("PUT /api/services/{service}/vars/{name}", s.putVar)
	service("DELETE /api/services/{service}/vars/{name}", s.deleteVar)

	service("GET /api/services/{service}/logs", s.logs)

	service("GET /api/services/{service}/db/{binding}/tables", s.dbTables)
	service("POST /api/services/{service}/db/{binding}/query", s.dbQuery)

	service("GET /api/services/{service}/files/{binding}", s.listFiles)
	service("GET /api/services/{service}/files/{binding}/file", s.getFile)
	service("PUT /api/services/{service}/files/{binding}/file", s.putFile)
	service("DELETE /api/services/{service}/files/{binding}/file", s.deleteFile)

	service("GET /api/services/{service}/jobs", s.listJobs)
	service("POST /api/services/{service}/jobs", s.sendJob)
	service("POST /api/services/{service}/jobs/{id}/retry", s.retryJob)
	service("DELETE /api/services/{service}/jobs/{id}", s.deleteJob)

	service("POST /api/services/{service}/realtime/publish", s.publish)

	a("GET /api/tokens", s.listTokens)
	a("POST /api/tokens", s.createToken)
	a("DELETE /api/tokens/{id}", s.deleteToken)

	a("GET /api/settings", s.settings)
	a("PATCH /api/settings", s.setSettings)
	a("GET /api/system", s.system)
	a("GET /api/system/logs", s.systemLogs)
	a("GET /api/system/backup", s.backup)
	a("POST /api/system/restore", s.restore)

	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "No such API route.")
	})
	return mux
}

type serviceSummary struct {
	*store.Service
	URL     string            `json:"url"`
	Status  string            `json:"status"`            // running, stopped or not_deployed
	Version int               `json:"version,omitempty"` // the active deployment's number
	Latest  *store.Deployment `json:"latest,omitempty"`
	Traffic *store.Traffic    `json:"traffic,omitempty"` // over ?period=, when asked
}

// summary describes a service. running means it serves its code, whether
// its process is up or starts on the next request.
func (s *Server) summary(a *store.Service) serviceSummary {
	out := serviceSummary{Service: a, URL: s.p.Cfg.ServiceURL(a.Name), Status: "running"}
	ctx := context.Background()
	if ds, err := s.p.Store.Deployments(ctx, a.ID, 1); err == nil && len(ds) > 0 {
		out.Latest = ds[0]
	}
	if a.ActiveDeployment != "" {
		if d, err := s.p.Store.Deployment(ctx, a.ID, a.ActiveDeployment); err == nil {
			out.Version = d.Number
		}
	}
	switch {
	case a.Stopped:
		out.Status = "stopped"
	case a.ActiveDeployment == "":
		out.Status = "not_deployed"
	}
	return out
}

func (s *Server) listServices(w http.ResponseWriter, r *http.Request) {
	services, err := s.p.Store.Services(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	// ?period=24h adds each service's traffic, in one query for all.
	var traffic map[string]store.Traffic
	if period := r.URL.Query().Get("period"); period != "" {
		if traffic, err = s.p.Traffic(r.Context(), period); err != nil {
			s.fail(w, err)
			return
		}
	}
	out := make([]serviceSummary, len(services))
	for i, a := range services {
		out[i] = s.summary(a)
		if traffic != nil {
			t := traffic[a.ID]
			out[i].Traffic = &t
		}
	}
	writeJSON(w, http.StatusOK, out)
}

type bindingView struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Bytes int64  `json:"bytes,omitempty"`
	Count int64  `json:"count,omitempty"`
}

func (s *Server) getService(w http.ResponseWriter, r *http.Request, service *store.Service) {
	ctx := r.Context()
	out := struct {
		serviceSummary
		Deployment  *store.Deployment `json:"deployment,omitempty"`
		Manifest    *deploy.Manifest  `json:"manifest,omitempty"`
		Bindings    []bindingView     `json:"bindings"`
		Jobs        store.JobCounts   `json:"jobs"`
		Connections int               `json:"connections"`
		Bytes       int64             `json:"bytes"`      // all its data
		DiskQuota   int64             `json:"disk_quota"` // bytes
		Process     *runtime.ProcInfo `json:"process,omitempty"`
		Vars        []store.Var       `json:"vars"` // set on the service, over the manifest's
	}{serviceSummary: s.summary(service), Bindings: []bindingView{}, Connections: s.p.Hub.Connections(service.ID),
		Bytes: s.p.Data.Usage(service.ID), DiskQuota: s.p.DiskQuota(service)}
	if proc := s.p.RT.Running()[service.ID]; proc != nil {
		info := proc.Info()
		out.Process = &info
	}
	if service.ActiveDeployment != "" {
		d, err := s.p.Store.Deployment(ctx, service.ID, service.ActiveDeployment)
		if err != nil {
			s.fail(w, err)
			return
		}
		out.Deployment, out.Manifest = d, s.p.Manifest(d)
		for _, name := range slices.Sorted(maps.Keys(out.Manifest.Bindings)) {
			b := bindingView{Name: name, Type: out.Manifest.Bindings[name]}
			switch b.Type {
			case deploy.DB:
				b.Bytes = s.p.Data.DBSize(service.ID, name)
			case deploy.Files:
				b.Count, b.Bytes, _ = s.p.Data.FilesUsage(ctx, service.ID, name)
			}
			out.Bindings = append(out.Bindings, b)
		}
	}
	var err error
	if out.Jobs, err = s.p.Store.JobCounts(ctx, service.ID); err != nil {
		s.fail(w, err)
		return
	}
	if out.Vars, err = s.p.Store.Vars(ctx, service.ID); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) deleteService(w http.ResponseWriter, r *http.Request, service *store.Service) {
	if err := s.p.DeleteService(r.Context(), service); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) setStopped(w http.ResponseWriter, r *http.Request, service *store.Service, stopped bool) {
	if err := s.p.SetStopped(r.Context(), service, stopped); err != nil {
		s.fail(w, err)
		return
	}
	service.Stopped = stopped
	writeJSON(w, http.StatusOK, s.summary(service))
}

func (s *Server) deploy(w http.ResponseWriter, r *http.Request) {
	var req deploy.Request
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, deploy.MaxBundleBytes*2))
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "The body must be a JSON deployment: {\"files\": {...}, \"main\": ..., \"bindings\": {...}}.")
		return
	}
	d, err := s.p.Deploy(r.Context(), r.PathValue("service"), &req)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := map[string]any{"deployment": d, "url": s.p.Cfg.ServiceURL(r.PathValue("service"))}
	if d.Status != "ready" {
		writeJSON(w, http.StatusUnprocessableEntity, out)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (s *Server) listDeployments(w http.ResponseWriter, r *http.Request, service *store.Service) {
	ds, err := s.p.Store.Deployments(r.Context(), service.ID, 50)
	if err != nil {
		s.fail(w, err)
		return
	}
	type view struct {
		*store.Deployment
		Active   bool             `json:"active"`
		Manifest *deploy.Manifest `json:"manifest"`
	}
	out := make([]view, len(ds))
	for i, d := range ds {
		out[i] = view{Deployment: d, Active: d.ID == service.ActiveDeployment, Manifest: s.p.Manifest(d)}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) activate(w http.ResponseWriter, r *http.Request, service *store.Service) {
	d, err := s.p.Rollback(r.Context(), service, r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) listSecrets(w http.ResponseWriter, r *http.Request, service *store.Service) {
	secrets, err := s.p.Store.Secrets(r.Context(), service.ID)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, secrets)
}

func (s *Server) putSecret(w http.ResponseWriter, r *http.Request, service *store.Service) {
	var in struct {
		Value *string `json:"value"`
	}
	if !decodeBody(w, r, platform.MaxSecretBytes*2, &in) {
		return
	}
	if in.Value == nil {
		writeError(w, http.StatusBadRequest, "invalid", "The body is {\"value\": \"...\"}.")
		return
	}
	if err := s.p.SetSecret(r.Context(), service, r.PathValue("name"), *in.Value); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deleteSecret(w http.ResponseWriter, r *http.Request, service *store.Service) {
	if err := s.p.DeleteSecret(r.Context(), service, r.PathValue("name")); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listVars(w http.ResponseWriter, r *http.Request, service *store.Service) {
	vars, err := s.p.Store.Vars(r.Context(), service.ID)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, vars)
}

func (s *Server) putVar(w http.ResponseWriter, r *http.Request, service *store.Service) {
	var in struct {
		Value *string `json:"value"`
	}
	if !decodeBody(w, r, deploy.MaxVarBytes*2, &in) {
		return
	}
	if in.Value == nil {
		writeError(w, http.StatusBadRequest, "invalid", "The body is {\"value\": \"...\"}.")
		return
	}
	if err := s.p.SetVar(r.Context(), service, r.PathValue("name"), *in.Value); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deleteVar(w http.ResponseWriter, r *http.Request, service *store.Service) {
	if err := s.p.DeleteVar(r.Context(), service, r.PathValue("name")); err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request, service *store.Service) {
	q := r.URL.Query()
	after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	wait, _ := strconv.Atoi(q.Get("wait"))
	logs, err := s.p.Logs(r.Context(), service.ID, after, limit, time.Duration(min(max(wait, 0), 60))*time.Second)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, logs)
}

// bindingOf checks that the active version of service has the binding named in
// the path, of the given type.
func (s *Server) bindingOf(w http.ResponseWriter, r *http.Request, service *store.Service, typ string) (string, bool) {
	name := r.PathValue("binding")
	m := s.p.ActiveManifest(r.Context(), service)
	if m == nil || m.Bindings[name] != typ {
		writeError(w, http.StatusNotFound, "not_found", "The active version has no "+typ+" binding named "+name+".")
		return "", false
	}
	return name, true
}

func (s *Server) dataFail(w http.ResponseWriter, err error) {
	var sqlErr data.SQLError
	var inv data.Invalid
	switch {
	case errors.As(err, &sqlErr), errors.As(err, &inv):
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
	case errors.Is(err, data.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "Not found.")
	default:
		s.fail(w, err)
	}
}

func (s *Server) dbTables(w http.ResponseWriter, r *http.Request, service *store.Service) {
	name, ok := s.bindingOf(w, r, service, deploy.DB)
	if !ok {
		return
	}
	tables, err := s.p.Data.Tables(r.Context(), service.ID, name)
	if err != nil {
		s.dataFail(w, err)
		return
	}
	if tables == nil {
		tables = []data.Table{}
	}
	writeJSON(w, http.StatusOK, tables)
}

func (s *Server) dbQuery(w http.ResponseWriter, r *http.Request, service *store.Service) {
	name, ok := s.bindingOf(w, r, service, deploy.DB)
	if !ok {
		return
	}
	var in struct {
		SQL        string            `json:"sql"`
		Params     []json.RawMessage `json:"params"`
		Statements []data.Statement  `json:"statements"`
	}
	if !decodeBody(w, r, 64<<20, &in) {
		return
	}
	stmts := in.Statements
	if in.SQL != "" {
		stmts = append([]data.Statement{{SQL: in.SQL, Params: in.Params}}, stmts...)
	}
	res, err := s.p.Data.Batch(r.Context(), service.ID, name, stmts)
	if err != nil {
		s.dataFail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) listFiles(w http.ResponseWriter, r *http.Request, service *store.Service) {
	name, ok := s.bindingOf(w, r, service, deploy.Files)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	l, err := s.p.Data.ListFiles(r.Context(), service.ID, name, q.Get("prefix"), q.Get("cursor"), limit)
	if err != nil {
		s.dataFail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, l)
}

func (s *Server) getFile(w http.ResponseWriter, r *http.Request, service *store.Service) {
	name, ok := s.bindingOf(w, r, service, deploy.Files)
	if !ok {
		return
	}
	f, content, err := s.p.Data.OpenFile(r.Context(), service.ID, name, r.URL.Query().Get("key"))
	if err != nil {
		s.dataFail(w, err)
		return
	}
	defer content.Close()
	w.Header().Set("Content-Type", f.Type)
	w.Header().Set("ETag", f.ETag)
	w.Header().Set("Content-Security-Policy", "sandbox")
	if r.URL.Query().Get("download") != "" {
		w.Header().Set("Content-Disposition", "attachment")
	}
	http.ServeContent(w, r, "", time.UnixMilli(f.Uploaded), content)
}

func (s *Server) putFile(w http.ResponseWriter, r *http.Request, service *store.Service) {
	name, ok := s.bindingOf(w, r, service, deploy.Files)
	if !ok {
		return
	}
	f, err := s.p.Data.PutFile(r.Context(), service.ID, name, r.URL.Query().Get("key"), r.Header.Get("Content-Type"), http.MaxBytesReader(w, r.Body, data.MaxFileBytes))
	if err != nil {
		s.dataFail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, f)
}

func (s *Server) deleteFile(w http.ResponseWriter, r *http.Request, service *store.Service) {
	name, ok := s.bindingOf(w, r, service, deploy.Files)
	if !ok {
		return
	}
	if err := s.p.Data.DeleteFile(r.Context(), service.ID, name, r.URL.Query().Get("key")); err != nil {
		s.dataFail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request, service *store.Service) {
	status := r.URL.Query().Get("status")
	if status != "" && status != "queued" && status != "running" && status != "dead" {
		writeError(w, http.StatusBadRequest, "invalid", "status is queued, running or dead.")
		return
	}
	jobs, err := s.p.Store.Jobs(r.Context(), service.ID, status, 200)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) sendJob(w http.ResponseWriter, r *http.Request, service *store.Service) {
	var in struct {
		Payload  json.RawMessage `json:"payload"`
		Delay    float64         `json:"delay"`
		Attempts int             `json:"attempts"`
		Key      string          `json:"key"`
	}
	if !decodeBody(w, r, 1<<20, &in) {
		return
	}
	j, queued, err := s.p.SendJob(r.Context(), service.ID, in.Payload, time.Duration(in.Delay*float64(time.Second)), in.Attempts, in.Key)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !queued {
		writeJSON(w, http.StatusOK, j) // the pending job with that key
		return
	}
	writeJSON(w, http.StatusCreated, j)
}

func jobID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "Not found.")
		return 0, false
	}
	return id, true
}

func (s *Server) retryJob(w http.ResponseWriter, r *http.Request, service *store.Service) {
	id, ok := jobID(w, r)
	if !ok {
		return
	}
	found, err := s.p.Store.RequeueDead(r.Context(), service.ID, id)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "not_found", "No dead job with that id.")
		return
	}
	s.p.WakeJobs()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request, service *store.Service) {
	id, ok := jobID(w, r)
	if !ok {
		return
	}
	found, err := s.p.Store.DeleteJob(r.Context(), service.ID, id)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "not_found", "Not found.")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request, service *store.Service) {
	var in struct {
		Channel string          `json:"channel"`
		Data    json.RawMessage `json:"data"`
	}
	if !decodeBody(w, r, 2<<20, &in) {
		return
	}
	if in.Channel == "" {
		writeError(w, http.StatusBadRequest, "invalid", "channel is required.")
		return
	}
	msg := string(in.Data)
	var str string
	if json.Unmarshal(in.Data, &str) == nil {
		msg = str
	}
	writeJSON(w, http.StatusOK, map[string]int{"delivered": s.p.Hub.Publish(service.ID, in.Channel, msg)})
}

func (s *Server) listTokens(w http.ResponseWriter, r *http.Request) {
	tokens, err := s.p.Store.Tokens(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tokens)
}

func (s *Server) createToken(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name          string `json:"name"`
		ExpiresInDays int    `json:"expires_in_days"` // 0: never
	}
	if !decodeBody(w, r, 4096, &in) {
		return
	}
	if in.Name == "" || len(in.Name) > 100 {
		writeError(w, http.StatusBadRequest, "invalid", "name is 1 to 100 characters.")
		return
	}
	if in.ExpiresInDays < 0 || in.ExpiresInDays > 3650 {
		writeError(w, http.StatusBadRequest, "invalid", "expires_in_days is 0 (never) to 3650.")
		return
	}
	var expires int64
	if in.ExpiresInDays > 0 {
		expires = time.Now().AddDate(0, 0, in.ExpiresInDays).UnixMilli()
	}
	secret := tokenPrefix + newSecret()
	t, err := s.p.Store.AddToken(r.Context(), in.Name, hashSecret(secret), secret[:len(tokenPrefix)+6], expires)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"secret": secret, "token": t})
}

func (s *Server) deleteToken(w http.ResponseWriter, r *http.Request) {
	ok, err := s.p.Store.DeleteToken(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "Not found.")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
