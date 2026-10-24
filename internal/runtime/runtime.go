// Package runtime runs services: one workerd process per service, started on
// demand, replaced on every new version or configuration, and stopped when
// idle or when it burns the CPU for too long. A process holds no state;
// everything a service keeps goes through its bindings to the runner.
package runtime

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/zeroko-io/runner/internal/deploy"
	"github.com/zeroko-io/runner/internal/store"
)

// Spec is everything a process needs to run one version of a service.
type Spec struct {
	ServiceID    string
	DeploymentID string
	Version      int // the deployment's number
	Manifest     *deploy.Manifest
	Modules      []deploy.Module
	Env          map[string]string // vars and secrets
	Egress       []string          // hosts the service may reach
}

type Options struct {
	WorkerdBin  string
	Dir         string // processes' directories and sockets
	ControlSock string
	EgressSock  string
	HeapMB      int
	CPUSeconds  int
	IdleSeconds int
	MaxProcs    int
	// Load builds the spec of a service's active version.
	Load func(ctx context.Context, serviceID string) (*Spec, error)
	// Logs receives each process's log entries.
	Logs func(serviceID string, logs []store.Log)
}

// ErrBusy is returned when as many processes run as the runner allows and all of
// them are handling requests.
var ErrBusy = errors.New("all service processes are busy (max_processes); try again shortly")

type Runtime struct {
	opt Options

	mu     sync.Mutex
	procs  map[string]*Proc      // serviceID -> the process serving it
	starts map[string]*startCall // serviceID -> a start in progress
	tokens map[string]*Proc      // process token -> process
	all    map[*Proc]struct{}    // every process, serving or not
	gen    map[string]int64      // serviceID -> generation, bumped by Activate and Stop
}

type startCall struct {
	done chan struct{}
	p    *Proc
	err  error
}

func New(opt Options) (*Runtime, error) {
	if err := os.RemoveAll(opt.Dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(opt.Dir, 0o700); err != nil {
		return nil, err
	}
	r := &Runtime{opt: opt, procs: map[string]*Proc{}, starts: map[string]*startCall{}, tokens: map[string]*Proc{}, all: map[*Proc]struct{}{}, gen: map[string]int64{}}
	go r.watch()
	return r, nil
}

// ProcForToken returns the process a token belongs to.
func (r *Runtime) ProcForToken(token string) (*Proc, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.tokens[token]
	return p, ok
}

// Get returns the running process of a service, starting it if needed.
func (r *Runtime) Get(ctx context.Context, serviceID string) (*Proc, error) {
	r.mu.Lock()
	if p := r.procs[serviceID]; p != nil && p.alive() {
		r.mu.Unlock()
		return p, nil
	}
	if c := r.starts[serviceID]; c != nil {
		r.mu.Unlock()
		select {
		case <-c.done:
			return c.p, c.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c := &startCall{done: make(chan struct{})}
	r.starts[serviceID] = c
	gen := r.gen[serviceID]
	r.mu.Unlock()

	c.p, c.err = r.startActive(serviceID)

	r.mu.Lock()
	delete(r.starts, serviceID)
	if c.err == nil {
		if r.gen[serviceID] != gen {
			// Replaced or stopped while starting: this version is stale.
			go c.p.stop("replaced while starting")
			c.p, c.err = nil, errors.New("the service changed while starting; retry")
		} else {
			r.procs[serviceID] = c.p
		}
	}
	r.mu.Unlock()
	close(c.done)
	return c.p, c.err
}

func (r *Runtime) startActive(serviceID string) (*Proc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	spec, err := r.opt.Load(ctx, serviceID)
	if err != nil {
		return nil, err
	}
	return r.Start(spec)
}

// Start starts a process for spec and waits until it serves. A version whose
// modules fail to load returns the error workerd reported.
func (r *Runtime) Start(spec *Spec) (*Proc, error) {
	id := store.NewID()
	tokenBytes := make([]byte, 24)
	rand.Read(tokenBytes)
	p := &Proc{
		ID:           id,
		ServiceID:    spec.ServiceID,
		DeploymentID: spec.DeploymentID,
		Version:      spec.Version,
		Egress:       spec.Egress,
		rt:           r,
		dir:          filepath.Join(r.opt.Dir, id),
		token:        hex.EncodeToString(tokenBytes),
		exited:       make(chan struct{}),
		started:      time.Now(),
	}
	p.lastActive.Store(time.Now().UnixNano())
	if err := r.makeRoom(spec.ServiceID); err != nil {
		return nil, err
	}
	if err := writeConfig(p.dir, spec, p.token, &r.opt); err != nil {
		return nil, err
	}
	p.public = unixClient(filepath.Join(p.dir, "public.sock"))
	p.internal = unixClient(filepath.Join(p.dir, "internal.sock"))

	cmd := exec.Command(r.opt.WorkerdBin, "serve", "config.capnp")
	cmd.Dir = p.dir
	cmd.Env = []string{"TZ=UTC"}
	cmd.SysProcAttr = sysProcAttr()
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = cmd.Stdout
	r.mu.Lock()
	r.tokens[p.token] = p
	r.all[p] = struct{}{}
	r.mu.Unlock()
	if err := cmd.Start(); err != nil {
		r.forget(p)
		p.cleanup()
		return nil, fmt.Errorf("start workerd: %w", err)
	}
	p.cmd = cmd
	logsDone := make(chan struct{})
	go func() {
		p.readLogs(out)
		close(logsDone)
	}()
	go func() {
		<-logsDone
		err := cmd.Wait()
		p.exitErr = err
		close(p.exited)
		r.exited(p)
	}()

	// workerd opens its sockets before it validates the modules, so the
	// process is ready only once the service's code answers.
	deadline := time.Now().Add(15 * time.Second)
	for {
		if p.ready() {
			p.served.Store(true)
			p.note("info", fmt.Sprintf("v%d started in %d ms", p.Version, time.Since(p.started).Milliseconds()))
			return p, nil
		}
		select {
		case <-p.exited:
			<-logsDone
			return nil, errors.New(p.startupError())
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			p.stop("did not start")
			return nil, errors.New("the service did not start within 15 seconds")
		}
	}
}

// makeRoom stops the least recently used idle process of another service when
// as many processes run as the runner allows.
func (r *Runtime) makeRoom(serviceID string) error {
	if r.opt.MaxProcs <= 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for p := range r.all {
		if !p.stopping.Load() {
			n++
		}
	}
	if n < r.opt.MaxProcs {
		return nil
	}
	var victim *Proc
	for id, p := range r.procs {
		if id != serviceID && p.Busy() == 0 && p.alive() && (victim == nil || p.lastActive.Load() < victim.lastActive.Load()) {
			victim = p
		}
	}
	if victim == nil {
		return ErrBusy
	}
	delete(r.procs, victim.ServiceID)
	victim.stopping.Store(true)
	victim.stopReason.Store("making room for another service")
	go victim.stop("")
	return nil
}

func (p *Proc) ready() bool {
	c, err := net.DialTimeout("unix", filepath.Join(p.dir, "public.sock"), time.Second)
	if err != nil {
		return false
	}
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://service/ready", nil)
	res, err := p.internal.Do(req)
	if err != nil {
		return false
	}
	res.Body.Close()
	return res.StatusCode == http.StatusOK
}

// Activate makes p serve its service and stops the process it replaces once its
// requests finish.
func (r *Runtime) Activate(p *Proc) {
	r.mu.Lock()
	old := r.procs[p.ServiceID]
	r.procs[p.ServiceID] = p
	r.gen[p.ServiceID]++
	r.mu.Unlock()
	if old != nil && old != p {
		go old.drain(fmt.Sprintf("replaced by v%d", p.Version))
	}
}

// Stop stops a service's process once its requests finish, for the reason
// given. The next request starts it again with the service's active version and
// configuration.
func (r *Runtime) Stop(serviceID, reason string) {
	r.mu.Lock()
	old := r.procs[serviceID]
	delete(r.procs, serviceID)
	r.gen[serviceID]++
	r.mu.Unlock()
	if old != nil {
		go old.drain(reason)
	}
}

// Kill ends every process of a service at once, its requests in flight
// included, and waits a few seconds for them to exit.
func (r *Runtime) Kill(serviceID string) {
	r.mu.Lock()
	delete(r.procs, serviceID)
	r.gen[serviceID]++
	var procs []*Proc
	for p := range r.all {
		if p.ServiceID == serviceID {
			procs = append(procs, p)
		}
	}
	r.mu.Unlock()
	deadline := time.After(5 * time.Second)
	for _, p := range procs {
		p.stopping.Store(true)
		p.quiet.Store(true)
		p.kill()
		select {
		case <-p.exited:
		case <-deadline:
			return
		}
	}
}

// Running lists the service IDs with a live process.
func (r *Runtime) Running() map[string]*Proc {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]*Proc, len(r.procs))
	for id, p := range r.procs {
		if p.alive() {
			out[id] = p
		}
	}
	return out
}

// ProcInfo describes a running process.
type ProcInfo struct {
	ServiceID    string `json:"service_id"`
	DeploymentID string `json:"deployment_id"`
	PID          int    `json:"pid"`
	Started      int64  `json:"started"`
	Busy         int64  `json:"busy"`   // requests and jobs in flight
	Memory       int64  `json:"memory"` // its own: anonymous pages, not the shared binary
	CPU          int64  `json:"cpu"`    // percent of one core
}

// Info describes a running process.
func (p *Proc) Info() ProcInfo {
	return ProcInfo{ServiceID: p.ServiceID, DeploymentID: p.DeploymentID, PID: p.cmd.Process.Pid, Started: p.started.UnixMilli(),
		Busy: p.Busy(), Memory: p.memory(), CPU: p.cpuPercent.Load()}
}

// Info describes every running process.
func (r *Runtime) Info() []ProcInfo {
	out := []ProcInfo{}
	for _, p := range r.Running() {
		out = append(out, p.Info())
	}
	return out
}

// Shutdown kills every process: they hold no state, and the runner has finished
// the requests in flight by then.
func (r *Runtime) Shutdown() {
	r.mu.Lock()
	var procs []*Proc
	for p := range r.all {
		procs = append(procs, p)
	}
	r.procs = map[string]*Proc{}
	r.mu.Unlock()
	var wg sync.WaitGroup
	for _, p := range procs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.stopping.Store(true)
			p.quiet.Store(true)
			p.kill()
			<-p.exited
		}()
	}
	wg.Wait()
}

func (r *Runtime) forget(p *Proc) {
	r.mu.Lock()
	if r.procs[p.ServiceID] == p {
		delete(r.procs, p.ServiceID)
	}
	delete(r.tokens, p.token)
	delete(r.all, p)
	r.mu.Unlock()
}

func (r *Runtime) exited(p *Proc) {
	r.forget(p)
	switch {
	case p.quiet.Load():
	case p.stopping.Load():
		if p.served.Load() {
			p.note("info", fmt.Sprintf("v%d stopped: %s", p.Version, p.stopReason.Load()))
		}
	case p.served.Load():
		msg := fmt.Sprintf("v%d exited", p.Version)
		if p.exitErr != nil {
			msg += ": " + p.exitErr.Error()
		}
		if reason := p.killReason.Load(); reason != nil {
			msg = reason.(string)
		}
		p.note("error", msg)
	}
	p.cleanup()
}

// watch stops idle processes and those that burn the CPU for too long.
func (r *Runtime) watch() {
	t := time.NewTicker(time.Second)
	for range t.C {
		for _, p := range r.Running() {
			if p.Busy() == 0 && time.Since(time.Unix(0, p.lastActive.Load())) > time.Duration(r.opt.IdleSeconds)*time.Second {
				r.mu.Lock()
				if r.procs[p.ServiceID] == p {
					delete(r.procs, p.ServiceID)
				}
				r.mu.Unlock()
				go p.stop("idle")
				continue
			}
			if busy := p.sampleCPU(); busy >= r.opt.CPUSeconds {
				p.killReason.Store(fmt.Sprintf("v%d kept the CPU busy for %d seconds and was restarted; requests in flight failed", p.Version, busy))
				p.kill()
			}
		}
	}
}

// Proc is one workerd process running one version of a service.
type Proc struct {
	ID           string
	ServiceID    string
	DeploymentID string
	Version      int
	Egress       []string

	rt       *Runtime
	dir      string
	token    string
	cmd      *exec.Cmd
	public   *http.Client
	internal *http.Client
	started  time.Time

	inflight   atomic.Int64
	lastActive atomic.Int64
	stopping   atomic.Bool
	stopReason atomic.Value
	quiet      atomic.Bool // stopped with the runner: nothing to note
	served     atomic.Bool // became ready at least once
	killReason atomic.Value
	exited     chan struct{}
	exitErr    error

	cpuTicks   uint64
	cpuBusySec int
	cpuPercent atomic.Int64 // of one core, over the last second

	mu        sync.Mutex
	startLogs []string
	causes    map[string]string // workerd internal error id -> its cause
}

func unixClient(sock string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     30 * time.Second,
		DisableCompression:  true,
	}}
}

// note records an event of the process in its service's logs.
func (p *Proc) note(level, msg string) {
	p.rt.opt.Logs(p.ServiceID, []store.Log{{Deployment: p.DeploymentID, TS: time.Now().UnixMilli(), Source: "runtime", Level: level, Message: msg}})
}

func (p *Proc) alive() bool {
	select {
	case <-p.exited:
		return false
	default:
		return !p.stopping.Load()
	}
}

// Busy is the number of requests the process is handling.
func (p *Proc) Busy() int64 { return p.inflight.Load() }

// Begin marks the start of work for the process; call the returned function
// when it ends.
func (p *Proc) Begin() func() {
	p.inflight.Add(1)
	p.lastActive.Store(time.Now().UnixNano())
	return func() {
		p.inflight.Add(-1)
		p.lastActive.Store(time.Now().UnixNano())
	}
}

// Transport sends public requests to the service.
func (p *Proc) Transport() http.RoundTripper { return p.public.Transport }

// Invoke calls one of the service's internal handlers (job or message) with a
// JSON body. A handler that threw returns its message as an error.
func (p *Proc) Invoke(ctx context.Context, handler string, body any) error {
	defer p.Begin()()
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://service/"+handler, strings.NewReader(string(b)))
	if err != nil {
		return err
	}
	res, err := p.internal.Do(req)
	if err != nil {
		return fmt.Errorf("the service is unreachable: %w", err)
	}
	defer res.Body.Close()
	var out struct {
		Error string `json:"error"`
	}
	json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&out)
	if res.StatusCode != http.StatusOK {
		if out.Error == "" {
			out.Error = res.Status
		}
		return errors.New(p.explain(out.Error))
	}
	return nil
}

// drain stops the process once its requests finish, or after 30 seconds.
func (p *Proc) drain(reason string) {
	if p.stopping.CompareAndSwap(false, true) {
		p.stopReason.Store(reason)
	}
	deadline := time.Now().Add(30 * time.Second)
	for p.Busy() > 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	p.stop(reason)
}

func (p *Proc) stop(reason string) {
	if p.stopping.CompareAndSwap(false, true) {
		p.stopReason.Store(reason)
	}
	if p.cmd == nil || p.cmd.Process == nil {
		p.cleanup()
		return
	}
	p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.exited:
	case <-time.After(5 * time.Second):
		p.cmd.Process.Kill()
		<-p.exited
	}
}

// Shutdown stops a process that never became the service's active one.
func (p *Proc) Shutdown() {
	p.quiet.Store(true)
	p.stop("not activated")
}

func (p *Proc) kill() {
	if p.cmd != nil && p.cmd.Process != nil {
		p.cmd.Process.Kill()
	}
}

func (p *Proc) cleanup() {
	p.public.CloseIdleConnections()
	p.internal.CloseIdleConnections()
	os.RemoveAll(p.dir)
}

// logLine is one line of workerd's structured output. Lines with a source
// are workerd's own; the others are the service's console.
type logLine struct {
	Timestamp json.Number `json:"timestamp"`
	Level     string      `json:"level"`
	Source    string      `json:"source"`
	Message   string      `json:"message"`
}

const maxLogMessage = 8 << 10

// readLogs turns workerd's output into the service's log entries: the service's
// console, and workerd's own lines about the service.
func (p *Proc) readLogs(out io.Reader) {
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	batch := make(chan store.Log, 256)
	go func() {
		var buf []store.Log
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case l, ok := <-batch:
				if !ok {
					if len(buf) > 0 {
						p.rt.opt.Logs(p.ServiceID, buf)
					}
					return
				}
				buf = append(buf, l)
				if len(buf) >= 100 {
					p.rt.opt.Logs(p.ServiceID, buf)
					buf = nil
				}
			case <-t.C:
				if len(buf) > 0 {
					p.rt.opt.Logs(p.ServiceID, buf)
					buf = nil
				}
			}
		}
	}()
	for sc.Scan() {
		line := sc.Text()
		var l logLine
		// A line that is not JSON is workerd itself, crashing for example.
		entry := store.Log{Deployment: p.DeploymentID, TS: time.Now().UnixMilli(), Source: "runtime", Level: "error", Message: line}
		if json.Unmarshal([]byte(line), &l) == nil && l.Message != "" {
			if ts, err := l.Timestamp.Int64(); err == nil {
				entry.TS = ts
			}
			entry.Level, entry.Message = normalizeLevel(l.Level), p.explain(jsgNoise.ReplaceAllString(l.Message, ""))
			// workerd reports the service's uncaught exceptions itself; they
			// are the service's.
			if l.Source == "" || strings.HasPrefix(entry.Message, "Uncaught") {
				entry.Source = "console"
			}
		}
		entry.Message = strings.TrimPrefix(entry.Message, "service main: ")
		if len(entry.Message) > maxLogMessage {
			entry.Message = entry.Message[:maxLogMessage] + "…"
		}
		if entry.Level == "error" && !p.served.Load() {
			// Errors before the service serves are why it failed to start: they
			// become the error of the deployment or of the start.
			p.mu.Lock()
			if len(p.startLogs) < 20 && !strings.HasPrefix(entry.Message, "Socket ") {
				p.startLogs = append(p.startLogs, entry.Message)
			}
			p.mu.Unlock()
			continue
		}
		select {
		case batch <- entry:
		default: // the service logs faster than the runner stores; drop
		}
	}
	close(batch)
}

// workerd shows the service "internal error; reference = <id>" for errors it
// hides, and logs the cause with the same id. explain keeps the cause of
// each such line and puts it in place of the reference wherever the id
// appears.
var (
	internalCause = regexp.MustCompile(`(?s)^e = \S+ failed: (.*?)(?:\nstack:|; sentryErrorContext).*wdErrId = (\w+)`)
	internalRef   = regexp.MustCompile(`internal error; reference = (\w+)`)
)

func (p *Proc) explain(msg string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if m := internalCause.FindStringSubmatch(msg); m != nil {
		if p.causes == nil || len(p.causes) > 100 {
			p.causes = map[string]string{}
		}
		p.causes[m[2]] = m[1]
		return "internal error " + m[2] + ": " + m[1]
	}
	return internalRef.ReplaceAllStringFunc(msg, func(ref string) string {
		id := internalRef.FindStringSubmatch(ref)[1]
		if cause, ok := p.causes[id]; ok {
			return cause + " (internal error " + id + ")"
		}
		return ref
	})
}

// jsgNoise is workerd's internal prefix on errors thrown across RPC.
var jsgNoise = regexp.MustCompile(`workerd/[^ ]+: failed: (remote\.)?jsg\.`)

func normalizeLevel(l string) string {
	switch l {
	case "log", "info", "debug", "warn", "error":
		return l
	case "fatal":
		return "error"
	}
	return "info"
}

func (p *Proc) startupError() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.startLogs) == 0 {
		return "the service failed to start"
	}
	return strings.Join(p.startLogs, "\n")
}
