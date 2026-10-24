package platform

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/zeroko-io/runner/internal/store"
)

// DurationBuckets are the upper bounds, in milliseconds, of the request
// duration histogram; one more bucket counts longer requests.
var DurationBuckets = []int64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

const metricsRetention = 30 * 24 * time.Hour

type metricKey struct {
	service string
	minute  int64
}

// metrics counts traffic in memory; flush adds it to runner.db every 10
// seconds.
type metrics struct {
	mu sync.Mutex
	m  map[metricKey]*store.Metric
}

func (ms *metrics) get(service string, t time.Time) *store.Metric {
	k := metricKey{service, t.Unix() / 60}
	m := ms.m[k]
	if m == nil {
		m = &store.Metric{ServiceID: service, Minute: k.minute, Buckets: make([]int64, len(DurationBuckets)+1)}
		ms.m[k] = m
	}
	return m
}

// Request records one request to a service and logs it.
func (p *Platform) Request(serviceID, deploymentID, method, path string, status int, d time.Duration) {
	p.metrics.mu.Lock()
	m := p.metrics.get(serviceID, time.Now())
	m.Requests++
	if status >= 500 {
		m.Errors++
	}
	ms := d.Milliseconds()
	i := 0
	for i < len(DurationBuckets) && ms > DurationBuckets[i] {
		i++
	}
	m.Buckets[i]++
	p.metrics.mu.Unlock()
	if len(path) > 200 {
		path = path[:200] + "…"
	}
	level := "info"
	if status >= 500 {
		level = "error"
	}
	p.ServiceLog(serviceID, deploymentID, "request", level, fmt.Sprintf("%s %s %d %s", method, path, status, duration(d)))
}

func duration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%d ms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1f s", d.Seconds())
}

func (p *Platform) countExceptions(serviceID string, logs []store.Log) {
	n := 0
	for _, l := range logs {
		if l.Level == "error" && strings.HasPrefix(l.Message, "Uncaught") {
			n++
		}
	}
	if n == 0 {
		return
	}
	p.metrics.mu.Lock()
	p.metrics.get(serviceID, time.Now()).Exceptions += int64(n)
	p.metrics.mu.Unlock()
}

func (p *Platform) flushMetrics(ctx context.Context) {
	p.metrics.mu.Lock()
	batch := make([]store.Metric, 0, len(p.metrics.m))
	for _, m := range p.metrics.m {
		batch = append(batch, *m)
	}
	p.metrics.m = map[metricKey]*store.Metric{}
	p.metrics.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	if err := p.Store.AddMetrics(ctx, batch); err != nil {
		p.Log.Warn("store metrics", "err", err)
	}
}

func (p *Platform) runMetrics(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			p.flushMetrics(context.Background())
			return
		case <-t.C:
			p.flushMetrics(ctx)
		}
	}
}

type MetricsPoint struct {
	T              int64 `json:"t"` // start, Unix milliseconds
	Requests       int64 `json:"requests"`
	Errors         int64 `json:"errors"`
	Exceptions     int64 `json:"exceptions"`
	Outbound       int64 `json:"outbound"`
	OutboundErrors int64 `json:"outbound_errors"`
	P50            int64 `json:"p50"` // milliseconds, the bucket's upper bound; -1: longer than the last
	P95            int64 `json:"p95"`
	buckets        []int64
}

type Metrics struct {
	Period string         `json:"period"`
	Step   int64          `json:"step"` // seconds
	Points []MetricsPoint `json:"points"`
	Totals MetricsPoint   `json:"totals"`
}

var periods = map[string]struct{ span, step time.Duration }{
	"1h":  {time.Hour, time.Minute},
	"24h": {24 * time.Hour, 15 * time.Minute},
	"7d":  {7 * 24 * time.Hour, 2 * time.Hour},
	"30d": {30 * 24 * time.Hour, 12 * time.Hour},
}

// ServiceMetrics returns a service's traffic over a period (1h, 24h, 7d or
// 30d).
func (p *Platform) ServiceMetrics(ctx context.Context, serviceID, period string) (*Metrics, error) {
	pr, ok := periods[period]
	if !ok {
		return nil, Invalid("period is 1h, 24h, 7d or 30d")
	}
	p.flushMetrics(ctx)
	step := int64(pr.step / time.Minute)
	n := int64(pr.span / pr.step)
	last := time.Now().Unix() / 60 / step * step // the step that holds now
	first := last - (n-1)*step
	rows, err := p.Store.Metrics(ctx, serviceID, first)
	if err != nil {
		return nil, err
	}
	out := &Metrics{Period: period, Step: int64(pr.step / time.Second), Points: make([]MetricsPoint, n)}
	out.Totals.buckets = make([]int64, len(DurationBuckets)+1)
	for i := range out.Points {
		out.Points[i] = MetricsPoint{T: (first + int64(i)*step) * 60_000, buckets: make([]int64, len(DurationBuckets)+1)}
	}
	for _, r := range rows {
		i := (r.Minute - first) / step
		if i < 0 || i >= n {
			continue
		}
		for _, pt := range []*MetricsPoint{&out.Points[i], &out.Totals} {
			pt.Requests += r.Requests
			pt.Errors += r.Errors
			pt.Exceptions += r.Exceptions
			pt.Outbound += r.Outbound
			pt.OutboundErrors += r.OutboundErrors
			for j, b := range r.Buckets {
				if j < len(pt.buckets) {
					pt.buckets[j] += b
				}
			}
		}
	}
	for _, pt := range append([]*MetricsPoint{&out.Totals}, pointers(out.Points)...) {
		pt.P50, pt.P95 = quantile(pt.buckets, pt.Requests, 0.5), quantile(pt.buckets, pt.Requests, 0.95)
	}
	return out, nil
}

// Traffic sums every service's traffic over a period (1h, 24h, 7d or 30d),
// by service ID, over the same steps as ServiceMetrics.
func (p *Platform) Traffic(ctx context.Context, period string) (map[string]store.Traffic, error) {
	pr, ok := periods[period]
	if !ok {
		return nil, Invalid("period is 1h, 24h, 7d or 30d")
	}
	p.flushMetrics(ctx)
	step := int64(pr.step / time.Minute)
	n := int64(pr.span / pr.step)
	last := time.Now().Unix() / 60 / step * step
	return p.Store.TrafficSince(ctx, last-(n-1)*step)
}

func pointers(ps []MetricsPoint) []*MetricsPoint {
	out := make([]*MetricsPoint, len(ps))
	for i := range ps {
		out[i] = &ps[i]
	}
	return out
}

// quantile is the upper bound of the bucket that holds quantile q.
func quantile(buckets []int64, total int64, q float64) int64 {
	if total == 0 {
		return 0
	}
	target := int64(float64(total)*q + 0.5)
	if target < 1 {
		target = 1
	}
	var seen int64
	for i, b := range buckets {
		seen += b
		if seen >= target {
			if i < len(DurationBuckets) {
				return DurationBuckets[i]
			}
			return -1
		}
	}
	return -1
}
