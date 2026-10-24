package platform

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/zeroko-io/runner/internal/deploy"
	"github.com/zeroko-io/runner/internal/store"
)

// CreateService creates a service with no deployment. Without a name, it gets a
// free one such as service-x7k2.
func (p *Platform) CreateService(ctx context.Context, name string) (*store.Service, error) {
	if name == "" {
		for range 10 {
			name = "service-" + strings.ToLower(rand.Text()[:4])
			if _, err := p.Store.Service(ctx, name); errors.Is(err, store.ErrNotFound) {
				return p.Store.CreateService(ctx, name)
			}
		}
		return nil, errors.New("no free service name found")
	}
	if !deploy.ServiceName.MatchString(name) {
		return nil, Invalid("service names are 1 to 40 lowercase letters, digits and hyphens, starting and ending with a letter or digit")
	}
	if _, err := p.Store.Service(ctx, name); err == nil {
		return nil, Invalid("a service named " + name + " exists")
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	return p.Store.CreateService(ctx, name)
}

// Source is the code of a deployment: each file's text, or null for binary
// files.
type Source struct {
	Main  string             `json:"main"`
	Files map[string]*string `json:"files"`
}

func (p *Platform) Source(ctx context.Context, service *store.Service, deploymentID string) (*Source, error) {
	d, modules, err := p.modules(ctx, service, deploymentID)
	if err != nil {
		return nil, err
	}
	out := &Source{Main: p.Manifest(d).Main, Files: map[string]*string{}}
	for _, m := range modules {
		if !isText(m) {
			out.Files[m.Name] = nil
			continue
		}
		s := string(m.Content)
		out.Files[m.Name] = &s
	}
	return out, nil
}

// Bundle is a deployment as the request body that deploys it again.
type Bundle struct {
	deploy.Manifest
	Files map[string]any `json:"files"`
}

func (p *Platform) Bundle(ctx context.Context, service *store.Service, deploymentID string) (*store.Deployment, *Bundle, error) {
	d, modules, err := p.modules(ctx, service, deploymentID)
	if err != nil {
		return nil, nil, err
	}
	out := &Bundle{Manifest: *p.Manifest(d), Files: map[string]any{}}
	for _, m := range modules {
		if isText(m) {
			out.Files[m.Name] = string(m.Content)
		} else {
			out.Files[m.Name] = map[string]string{"base64": base64.StdEncoding.EncodeToString(m.Content)}
		}
	}
	return d, out, nil
}

func (p *Platform) modules(ctx context.Context, service *store.Service, deploymentID string) (*store.Deployment, []deploy.Module, error) {
	d, err := p.Store.Deployment(ctx, service.ID, deploymentID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	b, err := os.ReadFile(filepath.Join(p.bundlesDir, d.Bundle+".json"))
	if err != nil {
		return nil, nil, err
	}
	var modules []deploy.Module
	if err := json.Unmarshal(b, &modules); err != nil {
		return nil, nil, err
	}
	return d, modules, nil
}

func isText(m deploy.Module) bool {
	return m.Type != "data" && m.Type != "wasm" && utf8.Valid(m.Content)
}

// RunCron queues a job for one of the active version's crons now.
func (p *Platform) RunCron(ctx context.Context, service *store.Service, schedule string) (*store.Job, error) {
	if service.Stopped {
		return nil, ErrStopped
	}
	m := p.ActiveManifest(ctx, service)
	if m == nil {
		return nil, ErrNotFound
	}
	for _, c := range m.Crons {
		if c.Schedule == schedule {
			j := &store.Job{ServiceID: service.ID, Payload: string(c.Payload), Cron: c.Schedule, MaxAttempts: 1, RunAt: time.Now().UnixMilli()}
			if _, err := p.Store.Enqueue(ctx, j); err != nil {
				return nil, err
			}
			p.wakeJobs()
			return j, nil
		}
	}
	return nil, ErrNotFound
}

// ImportData replaces the service's stores found in an export archive. The
// service's process stops first; the next request starts it on the imported
// data.
func (p *Platform) ImportData(ctx context.Context, service *store.Service, r io.Reader) ([]string, error) {
	p.RT.Stop(service.ID, "data imported")
	return p.Data.Import(service.ID, r)
}
