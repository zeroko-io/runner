// Package platform is the runner's product logic: services, deployments, rollbacks,
// secrets, logs, jobs and crons. It runs services through package runtime and
// keeps their data through package data.
package platform

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/zeroko-io/runner/internal/config"
	"github.com/zeroko-io/runner/internal/data"
	"github.com/zeroko-io/runner/internal/deploy"
	"github.com/zeroko-io/runner/internal/egress"
	"github.com/zeroko-io/runner/internal/realtime"
	"github.com/zeroko-io/runner/internal/runtime"
	"github.com/zeroko-io/runner/internal/store"
)

const (
	keepDeployments = 10
	keepLogs        = 5000
	logRetention    = 7 * 24 * time.Hour
	MaxSecrets      = 100
	MaxSecretBytes  = 64 << 10
)

var (
	ErrNotFound = errors.New("not found")
	ErrStopped  = errors.New("the service is stopped")
)

// Invalid is an error in a request, shown to the caller as is.
type Invalid string

func (e Invalid) Error() string { return string(e) }

type Platform struct {
	Cfg   *config.Settings
	Store *store.Store
	Data  *data.Data
	RT    *runtime.Runtime
	Hub   *realtime.Hub
	Log   *slog.Logger

	Version string
	Lines   *Lines // the runner's own log

	sealer     *sealer
	bundlesDir string
	locks      sync.Map // service name -> *sync.Mutex: one deployment, rollback or deletion at a time

	logMu    sync.Mutex
	logWait  map[string]chan struct{} // closed when a service gets new logs
	logCh    chan pendingLogs
	logsQuit chan struct{} // closed by Close: write what is queued and stop
	logsDone chan struct{}
	metrics  metrics
	jobsWake chan struct{}
	started  time.Time
	stopped  context.Context // done when the runner shuts down

	cronMu sync.Mutex
	crons  map[string][]cronEntry // serviceID -> crons of the active version

	quotas      sync.Map // serviceID -> disk quota in bytes
	sites       sync.Map // deployment ID -> *Site
	keyLocks    locks
	restart     chan struct{}
	restartOnce sync.Once
}

type cronEntry struct {
	expr     string
	schedule *deploy.Schedule
	payload  json.RawMessage
}

func New(cfg *config.Settings, st *store.Store, log *slog.Logger) (*Platform, error) {
	p := &Platform{
		Cfg:        cfg,
		Store:      st,
		Log:        log,
		bundlesDir: filepath.Join(config.DataDir, "bundles"),
		logWait:    map[string]chan struct{}{},
		logCh:      make(chan pendingLogs, 4096),
		logsQuit:   make(chan struct{}),
		logsDone:   make(chan struct{}),
		metrics:    metrics{m: map[metricKey]*store.Metric{}},
		jobsWake:   make(chan struct{}, 1),
		crons:      map[string][]cronEntry{},
		started:    time.Now(),
		stopped:    context.Background(),
		restart:    make(chan struct{}),
	}
	go p.writeLogs()
	var err error
	if p.sealer, err = loadSealer(filepath.Join(config.DataDir, "master.key")); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(p.bundlesDir, 0o700); err != nil {
		return nil, err
	}
	if p.Data, err = data.New(filepath.Join(config.DataDir, "services")); err != nil {
		return nil, err
	}
	p.Hub = realtime.NewHub(p.deliverEvent)
	p.Data.Quota = p.quota
	p.RT, err = runtime.New(runtime.Options{
		WorkerdBin:   config.Workerd,
		Dir:          filepath.Join(config.DataDir, "run"),
		ControlSock:  p.ControlSock(),
		Egress:       egress.New(p.EgressService, p.Outbound),
		HeapMB:       cfg.HeapMB,
		CPUSeconds:   cfg.CPUSeconds,
		CPUPerMinute: cfg.CPUPerMinute,
		IdleSeconds:  cfg.IdleSeconds,
		MaxProcs:     cfg.MaxProcesses,
		Load:         p.load,
		Logs:         p.addLogs,
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

// ControlSock is where the bindings in service processes reach the runner.
func (p *Platform) ControlSock() string { return filepath.Join(config.DataDir, "run-control.sock") }

// Start loads the crons of every service and runs the job and cron workers
// until ctx ends.
func (p *Platform) Start(ctx context.Context) error {
	p.stopped = ctx
	services, err := p.Store.Services(ctx)
	if err != nil {
		return err
	}
	for _, a := range services {
		if a.ActiveDeployment == "" {
			continue
		}
		d, err := p.Store.Deployment(ctx, a.ID, a.ActiveDeployment)
		if err != nil {
			return err
		}
		var m deploy.Manifest
		if err := json.Unmarshal([]byte(d.Manifest), &m); err != nil {
			return err
		}
		p.setCrons(a.ID, &m)
	}
	if err := p.Store.ResetRunning(ctx); err != nil {
		return err
	}
	go p.runJobs(ctx)
	go p.runCrons(ctx)
	go p.runMetrics(ctx)
	go p.maintain(ctx)
	return nil
}

func (p *Platform) Close() {
	p.RT.Shutdown()
	p.flushMetrics(context.Background())
	close(p.logsQuit)
	<-p.logsDone
	p.Data.Close()
}

// Service returns a service by name.
func (p *Platform) Service(ctx context.Context, name string) (*store.Service, error) {
	a, err := p.Store.Service(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}
	return a, err
}

// lock serializes the changes to one service's versions; call the returned
// function to unlock.
func (p *Platform) lock(name string) func() {
	mu, _ := p.locks.LoadOrStore(name, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	return mu.(*sync.Mutex).Unlock
}

// Deploy builds a new version of a service, creating the service if needed,
// starts it and makes it the active one. A version that fails to start is
// recorded as failed and leaves the running one untouched.
func (p *Platform) Deploy(ctx context.Context, name string, req *deploy.Request) (*store.Deployment, error) {
	if !deploy.ServiceName.MatchString(name) {
		return nil, Invalid("service names are 1 to 40 lowercase letters, digits and hyphens, starting and ending with a letter or digit")
	}
	m, modules, err := deploy.Parse(req)
	if err != nil {
		return nil, Invalid(err.Error())
	}
	for b, typ := range m.Bindings {
		if typ == deploy.ServicePrefix+name {
			return nil, Invalid(b + ": a service cannot call itself through a binding")
		}
	}
	defer p.lock(name)()

	service, err := p.Store.Service(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		service, err = p.Store.CreateService(ctx, name)
	}
	if err != nil {
		return nil, err
	}
	secrets, err := p.Store.Secrets(ctx, service.ID)
	if err != nil {
		return nil, err
	}
	for _, s := range secrets {
		if _, ok := m.Bindings[s.Name]; ok {
			return nil, Invalid(fmt.Sprintf("%s is both a binding and a secret", s.Name))
		}
		if _, ok := m.Vars[s.Name]; ok {
			return nil, Invalid(fmt.Sprintf("%s is both a var and a secret", s.Name))
		}
	}
	vars, err := p.Store.Vars(ctx, service.ID)
	if err != nil {
		return nil, err
	}
	for _, v := range vars {
		if _, ok := m.Bindings[v.Name]; ok {
			return nil, Invalid(fmt.Sprintf("%s is both a binding and a var of the service", v.Name))
		}
	}
	sha, err := p.writeBundle(modules)
	if err != nil {
		return nil, err
	}
	manifest, _ := json.Marshal(m)
	d := &store.Deployment{ID: store.NewID(), ServiceID: service.ID, Manifest: string(manifest), Bundle: sha}

	env, err := p.env(ctx, service.ID, m)
	if err != nil {
		return nil, err
	}
	d.Number, err = p.Store.NextDeployment(ctx, service.ID)
	if err != nil {
		return nil, err
	}
	proc, err := p.RT.Start(&runtime.Spec{ServiceID: service.ID, DeploymentID: d.ID, Version: d.Number, Manifest: m, Modules: modules, Env: env, Egress: p.egress(service)})
	if err != nil {
		d.Status, d.Error = "failed", err.Error()
		if aerr := p.Store.AddDeployment(ctx, d); aerr != nil {
			return nil, aerr
		}
		p.ServiceLog(service.ID, d.ID, "runtime", "error", fmt.Sprintf("v%d failed to deploy: %s", d.Number, d.Error))
		return d, nil
	}
	d.Status = "ready"
	if err := p.Store.AddDeployment(ctx, d); err != nil {
		proc.Shutdown()
		return nil, err
	}
	p.activated(ctx, service, proc, m)
	return d, nil
}

// Rollback makes an earlier ready deployment the active one.
func (p *Platform) Rollback(ctx context.Context, service *store.Service, deploymentID string) (*store.Deployment, error) {
	defer p.lock(service.Name)()
	d, err := p.Store.Deployment(ctx, service.ID, deploymentID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if d.Status != "ready" {
		return nil, Invalid("only a ready deployment can be activated")
	}
	m := p.Manifest(d)
	spec, err := p.spec(ctx, service, d)
	if err != nil {
		return nil, err
	}
	proc, err := p.RT.Start(spec)
	if err != nil {
		p.ServiceLog(service.ID, d.ID, "runtime", "error", fmt.Sprintf("v%d failed to start: %s", d.Number, err))
		return nil, Invalid("the deployment failed to start: " + err.Error())
	}
	if err := p.Store.Activate(ctx, service.ID, d.ID); err != nil {
		proc.Shutdown()
		return nil, err
	}
	p.activated(ctx, service, proc, m)
	return d, nil
}

// activated makes proc, the process of a new active version, the service's.
func (p *Platform) activated(ctx context.Context, service *store.Service, proc *runtime.Proc, m *deploy.Manifest) {
	if service.Stopped {
		proc.Shutdown()
		p.RT.Stop(service.ID, "the service is stopped")
	} else {
		p.RT.Activate(proc)
	}
	p.setCrons(service.ID, m)
	unused, err := p.Store.PruneDeployments(ctx, service.ID, keepDeployments)
	if err != nil {
		p.Log.Warn("prune deployments", "service", service.Name, "err", err)
	}
	for _, sha := range unused {
		p.dropBundle(sha)
	}
}

func (p *Platform) writeBundle(modules []deploy.Module) (string, error) {
	b, err := json.Marshal(modules)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	sha := hex.EncodeToString(h[:])
	path := filepath.Join(p.bundlesDir, sha+".json")
	if _, err := os.Stat(path); err == nil {
		return sha, nil
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return "", err
	}
	return sha, os.Rename(tmp, path)
}

// load builds the spec of a service's active version, for the runtime.
func (p *Platform) load(ctx context.Context, serviceID string) (*runtime.Spec, error) {
	service, err := p.Store.ServiceByID(ctx, serviceID)
	if err != nil {
		return nil, err
	}
	if service.Stopped {
		return nil, ErrStopped
	}
	if service.ActiveDeployment == "" {
		return nil, ErrNotFound
	}
	d, err := p.Store.Deployment(ctx, serviceID, service.ActiveDeployment)
	if err != nil {
		return nil, err
	}
	return p.spec(ctx, service, d)
}

func (p *Platform) spec(ctx context.Context, service *store.Service, d *store.Deployment) (*runtime.Spec, error) {
	var m deploy.Manifest
	if err := json.Unmarshal([]byte(d.Manifest), &m); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(filepath.Join(p.bundlesDir, d.Bundle+".json"))
	if err != nil {
		return nil, err
	}
	var modules []deploy.Module
	if err := json.Unmarshal(b, &modules); err != nil {
		return nil, err
	}
	env, err := p.env(ctx, service.ID, &m)
	if err != nil {
		return nil, err
	}
	return &runtime.Spec{ServiceID: service.ID, DeploymentID: d.ID, Version: d.Number, Manifest: &m, Modules: modules, Env: env, Egress: p.egress(service)}, nil
}

// env is the service's secrets and vars: those of this version, and over
// them those set on the service. A secret or var named like a binding of this
// version, or a secret named like a var, gives way to it.
func (p *Platform) env(ctx context.Context, serviceID string, m *deploy.Manifest) (map[string]string, error) {
	env := map[string]string{}
	secrets, err := p.Store.Secrets(ctx, serviceID)
	if err != nil {
		return nil, err
	}
	for _, s := range secrets {
		v, err := p.sealer.open(s.Value, serviceID)
		if err != nil {
			return nil, fmt.Errorf("secret %s: %w", s.Name, err)
		}
		env[s.Name] = string(v)
	}
	for k, v := range m.Vars {
		env[k] = v
	}
	vars, err := p.Store.Vars(ctx, serviceID)
	if err != nil {
		return nil, err
	}
	for _, v := range vars {
		env[v.Name] = v.Value
	}
	for k := range m.Bindings {
		delete(env, k)
	}
	return env, nil
}

// Manifest returns the manifest of a deployment.
func (p *Platform) Manifest(d *store.Deployment) *deploy.Manifest {
	var m deploy.Manifest
	json.Unmarshal([]byte(d.Manifest), &m)
	return &m
}

// ActiveManifest returns the manifest of a service's active version, or nil.
func (p *Platform) ActiveManifest(ctx context.Context, service *store.Service) *deploy.Manifest {
	if service.ActiveDeployment == "" {
		return nil
	}
	d, err := p.Store.Deployment(ctx, service.ID, service.ActiveDeployment)
	if err != nil {
		return nil
	}
	return p.Manifest(d)
}

func (p *Platform) SetSecret(ctx context.Context, service *store.Service, name, value string) error {
	if !deploy.ValidEnvName(name) {
		return Invalid("secret names are letters, digits and underscores, not starting with a digit")
	}
	if len(value) > MaxSecretBytes {
		return Invalid("secrets are at most 64 KB")
	}
	secrets, err := p.Store.Secrets(ctx, service.ID)
	if err != nil {
		return err
	}
	if len(secrets) >= MaxSecrets {
		exists := false
		for _, s := range secrets {
			exists = exists || s.Name == name
		}
		if !exists {
			return Invalid(fmt.Sprintf("at most %d secrets per service", MaxSecrets))
		}
	}
	if m := p.ActiveManifest(ctx, service); m != nil {
		if _, ok := m.Bindings[name]; ok {
			return Invalid(name + " is a binding of the active version")
		}
		if _, ok := m.Vars[name]; ok {
			return Invalid(name + " is a var of the active version")
		}
	}
	if vars, err := p.Store.Vars(ctx, service.ID); err != nil {
		return err
	} else if slices.ContainsFunc(vars, func(v store.Var) bool { return v.Name == name }) {
		return Invalid(name + " is a var of the service")
	}
	if err := p.Store.PutSecret(ctx, service.ID, name, p.sealer.seal([]byte(value), service.ID)); err != nil {
		return err
	}
	p.RT.Stop(service.ID, "secrets changed") // the next request starts it with the new value
	return nil
}

func (p *Platform) DeleteSecret(ctx context.Context, service *store.Service, name string) error {
	ok, err := p.Store.DeleteSecret(ctx, service.ID, name)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}
	p.RT.Stop(service.ID, "secrets changed")
	return nil
}

// SetVar sets a var on the service, over the one of its deployments with
// the same name, without a deployment.
func (p *Platform) SetVar(ctx context.Context, service *store.Service, name, value string) error {
	if !deploy.ValidEnvName(name) {
		return Invalid("var names are letters, digits and underscores, not starting with a digit")
	}
	if len(value) > deploy.MaxVarBytes {
		return Invalid(fmt.Sprintf("vars are at most %d KB", deploy.MaxVarBytes>>10))
	}
	vars, err := p.Store.Vars(ctx, service.ID)
	if err != nil {
		return err
	}
	if len(vars) >= deploy.MaxVars && !slices.ContainsFunc(vars, func(v store.Var) bool { return v.Name == name }) {
		return Invalid(fmt.Sprintf("at most %d vars per service", deploy.MaxVars))
	}
	if m := p.ActiveManifest(ctx, service); m != nil {
		if _, ok := m.Bindings[name]; ok {
			return Invalid(name + " is a binding of the active version")
		}
	}
	secrets, err := p.Store.Secrets(ctx, service.ID)
	if err != nil {
		return err
	}
	if slices.ContainsFunc(secrets, func(s store.Secret) bool { return s.Name == name }) {
		return Invalid(name + " is a secret")
	}
	if err := p.Store.PutVar(ctx, service.ID, name, value); err != nil {
		return err
	}
	p.RT.Stop(service.ID, "vars changed")
	return nil
}

func (p *Platform) DeleteVar(ctx context.Context, service *store.Service, name string) error {
	ok, err := p.Store.DeleteVar(ctx, service.ID, name)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotFound
	}
	p.RT.Stop(service.ID, "vars changed")
	return nil
}

// SetStopped stops or starts a service. A stopped service serves no requests
// and runs no jobs or crons; its data stays.
func (p *Platform) SetStopped(ctx context.Context, service *store.Service, stopped bool) error {
	if err := p.Store.SetStopped(ctx, service.ID, stopped); err != nil {
		return err
	}
	if stopped {
		p.RT.Stop(service.ID, "the service was stopped")
		p.Hub.CloseService(service.ID)
	} else {
		p.wakeJobs()
	}
	return nil
}

// DeleteService deletes a service with everything it has.
func (p *Platform) DeleteService(ctx context.Context, service *store.Service) error {
	defer p.lock(service.Name)()
	if err := p.Store.SetStopped(ctx, service.ID, true); err != nil {
		return err
	}
	// Its processes go at once, before its data.
	p.RT.Kill(service.ID)
	p.quotas.Delete(service.ID)
	p.Hub.CloseService(service.ID)
	p.setCrons(service.ID, nil)
	deployments, err := p.Store.Deployments(ctx, service.ID, 1000)
	if err != nil {
		return err
	}
	if err := p.Store.DeleteService(ctx, service.ID); err != nil {
		return err
	}
	for _, d := range deployments {
		if used, err := p.Store.BundleUsed(ctx, d.Bundle); err == nil && !used {
			p.dropBundle(d.Bundle)
		}
	}
	return p.Data.DeleteService(service.ID)
}

// Logs.

type pendingLogs struct {
	serviceID string
	logs      []store.Log
}

// addLogs queues entries for writeLogs. When the runner cannot store them as
// fast as they come, they are dropped.
func (p *Platform) addLogs(serviceID string, logs []store.Log) {
	p.countExceptions(serviceID, logs)
	select {
	case p.logCh <- pendingLogs{serviceID, logs}:
	default:
	}
}

// writeLogs stores queued entries in batches: one transaction per service every
// 100 ms at most, then wakes the readers waiting for them. The channel is
// never closed, so a late entry cannot panic; Close makes the writer store
// what is queued and stop.
func (p *Platform) writeLogs() {
	defer close(p.logsDone)
	for {
		batch := map[string][]store.Log{}
		add := func(l pendingLogs) int {
			batch[l.serviceID] = append(batch[l.serviceID], l.logs...)
			return len(l.logs)
		}
		select {
		case l := <-p.logCh:
			deadline := time.After(100 * time.Millisecond)
		collect:
			for n := add(l); n < 2000; {
				select {
				case l := <-p.logCh:
					n += add(l)
				case <-deadline:
					break collect
				}
			}
			p.storeLogs(batch)
		case <-p.logsQuit:
			for len(p.logCh) > 0 {
				add(<-p.logCh)
			}
			p.storeLogs(batch)
			return
		}
	}
}

func (p *Platform) storeLogs(batch map[string][]store.Log) {
	for serviceID, logs := range batch {
		if err := p.Store.AddLogs(context.Background(), serviceID, logs); err != nil {
			p.Log.Warn("store logs", "service", serviceID, "err", err)
		}
	}
	p.logMu.Lock()
	for serviceID := range batch {
		if ch := p.logWait[serviceID]; ch != nil {
			close(ch)
			delete(p.logWait, serviceID)
		}
	}
	p.logMu.Unlock()
}

// ServiceLog records one entry in a service's logs.
func (p *Platform) ServiceLog(serviceID, deploymentID, source, level, msg string) {
	p.addLogs(serviceID, []store.Log{{Deployment: deploymentID, TS: time.Now().UnixMilli(), Source: source, Level: level, Message: msg}})
}

// Logs returns a service's entries after the given id. With wait, it waits up
// to that long for new ones.
func (p *Platform) Logs(ctx context.Context, serviceID string, after int64, limit int, wait time.Duration) ([]store.Log, error) {
	deadline := time.Now().Add(wait)
	for {
		p.logMu.Lock()
		ch := p.logWait[serviceID]
		if ch == nil {
			ch = make(chan struct{})
			p.logWait[serviceID] = ch
		}
		p.logMu.Unlock()
		logs, err := p.Store.Logs(ctx, serviceID, after, limit)
		if err != nil || len(logs) > 0 || time.Now().After(deadline) {
			return logs, err
		}
		select {
		case <-ch:
		case <-time.After(time.Until(deadline)):
		case <-ctx.Done():
			return logs, nil
		case <-p.stopped.Done():
			return logs, nil
		}
	}
}

// maintain prunes logs every few minutes.
func (p *Platform) maintain(ctx context.Context) {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		if err := p.Store.PruneLogs(ctx, keepLogs, time.Now().Add(-logRetention).UnixMilli()); err != nil && ctx.Err() == nil {
			p.Log.Warn("prune logs", "err", err)
		}
		p.Store.PruneMetrics(ctx, time.Now().Add(-metricsRetention).Unix()/60)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
