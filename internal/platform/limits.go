package platform

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/zeroko-io/runner/internal/egress"
	"github.com/zeroko-io/runner/internal/store"
)

const (
	maxEgressHosts = 100
	maxDiskMB      = 1 << 20 // 1 TB
)

// SetLimits sets which hosts a service may reach and how much it may store.
// Hosts are patterns: "*" (any), "example.com", "*.example.com"; none
// disables outbound requests. diskMB 0 is the instance's default.
func (p *Platform) SetLimits(ctx context.Context, service *store.Service, hosts []string, diskMB, rateLimit int) error {
	allow := []string{}
	for _, h := range hosts {
		h = strings.ToLower(strings.TrimSpace(h))
		if !egress.ValidPattern(h) {
			return Invalid(fmt.Sprintf("invalid host %q: use example.com, *.example.com or *", h))
		}
		if !slices.Contains(allow, h) {
			allow = append(allow, h)
		}
	}
	if slices.Contains(allow, "*") {
		allow = []string{"*"}
	}
	if len(allow) > maxEgressHosts {
		return Invalid(fmt.Sprintf("at most %d hosts", maxEgressHosts))
	}
	if diskMB < 0 || diskMB > maxDiskMB {
		return Invalid("disk_mb is 0 (the default) to 1048576")
	}
	if rateLimit < -1 || rateLimit > 1000000 {
		return Invalid("rate_limit is -1 (none), 0 (the runner's) or 1 to 1000000 requests a second")
	}
	if err := p.Store.SetServiceLimits(ctx, service.ID, allow, diskMB, rateLimit); err != nil {
		return err
	}
	p.quotas.Delete(service.ID)
	if !slices.Equal(allow, service.Egress) {
		p.RT.Stop(service.ID, "egress changed")
	}
	service.Egress, service.DiskMB, service.RateLimit = allow, diskMB, rateLimit
	return nil
}

// egress is the hosts a service's processes may reach.
func (p *Platform) egress(service *store.Service) []string {
	if !p.Cfg.Egress {
		return nil
	}
	return service.Egress
}

// DiskQuota is a service's disk quota in bytes.
func (p *Platform) DiskQuota(service *store.Service) int64 {
	mb := service.DiskMB
	if mb == 0 {
		mb = p.Cfg.DiskMB
	}
	return int64(mb) << 20
}

// quota is DiskQuota by service ID, cached: it runs on every write.
func (p *Platform) quota(serviceID string) int64 {
	if q, ok := p.quotas.Load(serviceID); ok {
		return q.(int64)
	}
	service, err := p.Store.ServiceByID(context.Background(), serviceID)
	if err != nil {
		return int64(p.Cfg.DiskMB) << 20
	}
	q := p.DiskQuota(service)
	p.quotas.Store(serviceID, q)
	return q
}

// EgressService returns what the egress proxy needs about the process a token
// belongs to.
func (p *Platform) EgressService(token string) (*egress.Service, bool) {
	proc, ok := p.RT.ProcForToken(token)
	if !ok {
		return nil, false
	}
	return &egress.Service{ID: proc.ServiceID, Deployment: proc.DeploymentID, Allow: proc.Egress}, true
}

// Outbound records a request a service made, in its metrics and logs.
func (p *Platform) Outbound(a *egress.Service, r egress.Request) {
	p.metrics.mu.Lock()
	m := p.metrics.get(a.ID, time.Now())
	m.Outbound++
	if r.Failed() {
		m.OutboundErrors++
	}
	p.metrics.mu.Unlock()
	url := r.URL
	if len(url) > 300 {
		url = url[:300] + "…"
	}
	level, msg := "info", fmt.Sprintf("%s %s %d %s", r.Method, url, r.Status, duration(r.Duration))
	switch {
	case r.Blocked:
		level, msg = "warn", fmt.Sprintf("%s %s blocked: %s", r.Method, url, r.Err)
	case r.Err != "":
		level, msg = "error", fmt.Sprintf("%s %s failed after %s: %s", r.Method, url, duration(r.Duration), r.Err)
	case r.Status >= 500:
		level = "error"
	}
	p.ServiceLog(a.ID, a.Deployment, "fetch", level, msg)
}
