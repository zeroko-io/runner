package platform

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/zeroko-io/runner/internal/deploy"
	"github.com/zeroko-io/runner/internal/realtime"
	"github.com/zeroko-io/runner/internal/store"
)

const (
	defaultAttempts = 3
	maxAttempts     = 20
	maxDelay        = 30 * 24 * time.Hour
	maxPayload      = 256 << 10
	maxJobKey       = 256
	jobTimeout      = 15 * time.Minute
	jobsPerService  = 4
)

// SendJob queues a job for a service's job() handler and reports true. With
// a key, while a job with it is not done, it returns that one and false.
func (p *Platform) SendJob(ctx context.Context, serviceID string, payload json.RawMessage, delay time.Duration, attempts int, key string) (*store.Job, bool, error) {
	if len(payload) > maxPayload {
		return nil, false, Invalid("job payloads are at most 256 KB of JSON")
	}
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	if !json.Valid(payload) {
		return nil, false, Invalid("the payload is not valid JSON")
	}
	if delay < 0 || delay > maxDelay {
		return nil, false, Invalid("delay is 0 to 30 days")
	}
	if attempts == 0 {
		attempts = defaultAttempts
	}
	if attempts < 1 || attempts > maxAttempts {
		return nil, false, Invalid("attempts is 1 to 20")
	}
	if len(key) > maxJobKey {
		return nil, false, Invalid("job keys are at most 256 bytes")
	}
	j := &store.Job{ServiceID: serviceID, Payload: string(payload), Key: key, MaxAttempts: attempts, RunAt: time.Now().Add(delay).UnixMilli()}
	queued, err := p.Store.Enqueue(ctx, j)
	if err != nil {
		return nil, false, err
	}
	if queued && delay == 0 {
		p.wakeJobs()
	}
	return j, queued, nil
}

// WakeJobs makes the job worker look for due jobs now.
func (p *Platform) WakeJobs() { p.wakeJobs() }

func (p *Platform) wakeJobs() {
	select {
	case p.jobsWake <- struct{}{}:
	default:
	}
}

// jobEvent is what the service's job() handler receives.
type jobEvent struct {
	ID      int64           `json:"id"`
	Payload json.RawMessage `json:"payload"`
	Attempt int             `json:"attempt"`
	Cron    string          `json:"cron,omitempty"`
}

// runJobs delivers due jobs, up to jobsPerService at once per service.
func (p *Platform) runJobs(ctx context.Context) {
	var mu sync.Mutex
	running := map[string]int{}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-p.jobsWake:
		}
		services, err := p.Store.DueServices(ctx)
		if err != nil {
			if ctx.Err() == nil {
				p.Log.Warn("jobs", "err", err)
			}
			continue
		}
		for _, serviceID := range services {
			service, err := p.Store.ServiceByID(ctx, serviceID)
			if err != nil || service.Stopped || service.ActiveDeployment == "" {
				continue
			}
			for {
				mu.Lock()
				if running[serviceID] >= jobsPerService {
					mu.Unlock()
					break
				}
				j, err := p.Store.ClaimJob(ctx, serviceID)
				if err != nil {
					mu.Unlock()
					if !errors.Is(err, store.ErrNotFound) && ctx.Err() == nil {
						p.Log.Warn("claim job", "err", err)
					}
					break
				}
				running[serviceID]++
				mu.Unlock()
				go func() {
					p.runJob(ctx, j)
					mu.Lock()
					if running[serviceID]--; running[serviceID] == 0 {
						delete(running, serviceID)
					}
					mu.Unlock()
					p.wakeJobs()
				}()
			}
		}
	}
}

func (p *Platform) runJob(ctx context.Context, j *store.Job) {
	ctx, cancel := context.WithTimeout(ctx, jobTimeout)
	defer cancel()
	err := p.invoke(ctx, j.ServiceID, "job", jobEvent{ID: j.ID, Payload: json.RawMessage(j.Payload), Attempt: j.Attempts, Cron: j.Cron})
	if err == nil {
		if _, err := p.Store.DeleteJob(context.Background(), j.ServiceID, j.ID); err != nil {
			p.Log.Warn("delete job", "err", err)
		}
		return
	}
	backoff := 10 * time.Second << min(j.Attempts-1, 12)
	if backoff > time.Hour {
		backoff = time.Hour
	}
	if err := p.Store.RetryJob(context.Background(), j.ID, err.Error(), time.Now().Add(backoff).UnixMilli()); err != nil {
		p.Log.Warn("retry job", "err", err)
	}
}

// invoke calls one of a service's internal handlers on its running process.
func (p *Platform) invoke(ctx context.Context, serviceID, handler string, body any) error {
	proc, err := p.RT.Get(ctx, serviceID)
	if err != nil {
		return err
	}
	return proc.Invoke(ctx, handler, body)
}

// deliverEvent runs a service's message() handler for a realtime event.
func (p *Platform) deliverEvent(ctx context.Context, serviceID string, e realtime.Event) error {
	if p.stopped.Err() != nil {
		return nil // shutting down: connections close, their services are not told
	}
	return p.invoke(ctx, serviceID, "message", e)
}

// Crons.

func (p *Platform) setCrons(serviceID string, m *deploy.Manifest) {
	var entries []cronEntry
	if m != nil {
		for _, c := range m.Crons {
			s, err := deploy.ParseCron(c.Schedule)
			if err != nil {
				continue
			}
			entries = append(entries, cronEntry{expr: c.Schedule, schedule: s, payload: c.Payload})
		}
	}
	p.cronMu.Lock()
	defer p.cronMu.Unlock()
	if len(entries) == 0 {
		delete(p.crons, serviceID)
	} else {
		p.crons[serviceID] = entries
	}
}

// runCrons queues a job for every cron due at the start of each minute. A
// cron job has one attempt: the next run is the retry.
func (p *Platform) runCrons(ctx context.Context) {
	for {
		next := time.Now().Truncate(time.Minute).Add(time.Minute)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next)):
		}
		p.cronMu.Lock()
		due := map[string][]cronEntry{}
		for serviceID, entries := range p.crons {
			for _, e := range entries {
				if e.schedule.Matches(next) {
					due[serviceID] = append(due[serviceID], e)
				}
			}
		}
		p.cronMu.Unlock()
		for serviceID, entries := range due {
			service, err := p.Store.ServiceByID(ctx, serviceID)
			if err != nil || service.Stopped {
				continue
			}
			for _, e := range entries {
				j := &store.Job{ServiceID: serviceID, Payload: string(e.payload), Cron: e.expr, MaxAttempts: 1, RunAt: next.UnixMilli()}
				if _, err := p.Store.Enqueue(ctx, j); err != nil && ctx.Err() == nil {
					p.Log.Warn("queue cron", "service", service.Name, "err", err)
				}
			}
		}
		p.wakeJobs()
	}
}
