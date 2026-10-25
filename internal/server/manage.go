package server

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/zeroko-io/runner/internal/store"
)

// createService creates a service; without a name, the runner picks one.
func (s *Server) createService(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	if r.ContentLength != 0 && !decodeBody(w, r, 4096, &in) {
		return
	}
	service, err := s.p.CreateService(r.Context(), strings.ToLower(strings.TrimSpace(in.Name)))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, s.summary(service))
}

func (s *Server) source(w http.ResponseWriter, r *http.Request, service *store.Service) {
	src, err := s.p.Source(r.Context(), service, r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, src)
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request, service *store.Service) {
	period := r.URL.Query().Get("period")
	if period == "" {
		period = "24h"
	}
	m, err := s.p.ServiceMetrics(r.Context(), service.ID, period)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (s *Server) runCron(w http.ResponseWriter, r *http.Request, service *store.Service) {
	var in struct {
		Schedule string `json:"schedule"`
	}
	if !decodeBody(w, r, 4096, &in) {
		return
	}
	j, err := s.p.RunCron(r.Context(), service, in.Schedule)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, j)
}

// storage lists the service's stores, and whether the active version binds
// each.
func (s *Server) storage(w http.ResponseWriter, r *http.Request, service *store.Service) {
	stores, err := s.p.Data.Stores(r.Context(), service.ID)
	if err != nil {
		s.fail(w, err)
		return
	}
	bindings := map[string]string{}
	if m := s.p.ActiveManifest(r.Context(), service); m != nil {
		bindings = m.Bindings
	}
	type view struct {
		Type    string `json:"type"`
		Binding string `json:"binding"`
		Bytes   int64  `json:"bytes"`
		Count   int64  `json:"count,omitempty"`
		Bound   bool   `json:"bound"`
	}
	out := make([]view, len(stores))
	for i, st := range stores {
		out[i] = view{st.Type, st.Binding, st.Bytes, st.Count, bindings[st.Binding] == st.Type}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) deleteStore(w http.ResponseWriter, r *http.Request, service *store.Service) {
	if err := s.p.Data.DeleteStore(service.ID, r.PathValue("type"), r.PathValue("binding")); err != nil {
		s.dataFail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deleteStorage(w http.ResponseWriter, r *http.Request, service *store.Service) {
	stores, err := s.p.Data.Stores(r.Context(), service.ID)
	if err != nil {
		s.fail(w, err)
		return
	}
	for _, st := range stores {
		if err := s.p.Data.DeleteStore(service.ID, st.Type, st.Binding); err != nil {
			s.fail(w, err)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) exportData(w http.ResponseWriter, r *http.Request, service *store.Service) {
	name := service.Name + "-" + time.Now().UTC().Format("20060102-150405") + ".tar.gz"
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	if err := s.p.Data.Export(r.Context(), service.ID, w); err != nil {
		// The headers are sent: a failed export ends the response early,
		// which the client sees as a broken download.
		s.log.Error("export", "service", service.Name, "err", err)
		panic(http.ErrAbortHandler)
	}
}

func (s *Server) importData(w http.ResponseWriter, r *http.Request, service *store.Service) {
	stores, err := s.p.ImportData(r.Context(), service, http.MaxBytesReader(w, r.Body, 50<<30))
	if err != nil {
		s.dataFail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string][]string{"stores": stores})
}

func (s *Server) system(w http.ResponseWriter, r *http.Request) {
	info := s.p.SystemInfo()
	services, err := s.p.Store.Services(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	names := map[string]string{}
	for _, a := range services {
		names[a.ID] = a.Name
	}
	procs := make([]map[string]any, len(info.Processes))
	for i, p := range info.Processes {
		procs[i] = map[string]any{"service": names[p.ServiceID], "pid": p.PID, "started": p.Started, "busy": p.Busy, "memory": p.Memory, "cpu": p.CPU}
	}
	var dataBytes int64
	for _, a := range services {
		dataBytes += s.p.Data.Usage(a.ID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version": info.Version, "workerd": info.Workerd, "started": info.Started, "services": len(services), "processes": procs,
		"disk": info.Disk, "data_bytes": dataBytes,
	})
}

func (s *Server) systemLogs(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	writeJSON(w, http.StatusOK, s.p.Lines.After(after, 1000))
}

// patchService renames a service, and changes what it may reach and store.
func (s *Server) patchService(w http.ResponseWriter, r *http.Request, service *store.Service) {
	var in struct {
		Name   *string   `json:"name"`
		Egress *[]string `json:"egress"`
		DiskMB *int      `json:"disk_mb"`
	}
	if !decodeBody(w, r, 64<<10, &in) {
		return
	}
	if in.Name != nil {
		if err := s.p.RenameService(r.Context(), service, strings.ToLower(strings.TrimSpace(*in.Name))); err != nil {
			s.fail(w, err)
			return
		}
	}
	egress, diskMB := service.Egress, service.DiskMB
	if in.Egress != nil {
		egress = *in.Egress
	}
	if in.DiskMB != nil {
		diskMB = *in.DiskMB
	}
	if err := s.p.SetLimits(r.Context(), service, egress, diskMB); err != nil {
		s.fail(w, err)
		return
	}
	s.getService(w, r, service)
}

// bundle downloads a deployment as the body that deploys it again.
func (s *Server) bundle(w http.ResponseWriter, r *http.Request, service *store.Service) {
	d, b, err := s.p.Bundle(r.Context(), service, r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	w.Header().Set("Content-Disposition", `attachment; filename="`+service.Name+"-v"+strconv.Itoa(d.Number)+`.json"`)
	writeJSON(w, http.StatusOK, b)
}

func (s *Server) backup(w http.ResponseWriter, r *http.Request) {
	name := "runner-" + time.Now().UTC().Format("20060102-150405") + ".tar.gz"
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	if err := s.p.Backup(r.Context(), w); err != nil {
		s.log.Error("backup failed", "err", err)
		panic(http.ErrAbortHandler)
	}
	s.log.Info("backup downloaded")
}

// restore stages a backup and restarts the runner, which puts it in place.
func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	if err := s.p.StageRestore(http.MaxBytesReader(w, r.Body, 1<<40)); err != nil {
		s.fail(w, err)
		return
	}
	s.log.Info("restoring a backup: restarting")
	writeJSON(w, http.StatusAccepted, map[string]bool{"restarting": true})
	s.p.RequestRestart()
}

func (s *Server) settings(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.p.Cfg)
}

// setSettings changes the settings the body names and restarts the runner
// with them, in place: service processes start again on their next request,
// and realtime connections close.
func (s *Server) setSettings(w http.ResponseWriter, r *http.Request) {
	next, err := s.p.Store.Settings(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	if !decodeBody(w, r, 64<<10, next) {
		return
	}
	if err := next.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	if err := s.p.Store.PutSettings(r.Context(), next); err != nil {
		s.fail(w, err)
		return
	}
	s.log.Info("settings changed: restarting")
	writeJSON(w, http.StatusAccepted, next)
	s.p.RequestRestart()
}
