// Package config holds the runner's settings: fixed by the image, or kept in
// runner.db and changed through the API.
package config

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Fixed by the image.
const (
	GatewayAddr = ":8080" // services' traffic, behind the public proxy
	APIAddr     = ":8081" // the API, for the app that manages the runner
	DataDir     = "/data"
	Workerd     = "/usr/bin/workerd"
)

// Settings are kept in runner.db and changed with PATCH /api/settings, which
// restarts the runner with them.
type Settings struct {
	// Services are served at <name><Separator><Domain>. Until it is set,
	// none is.
	Domain       string `json:"domain"`
	Separator    string `json:"separator"`     // "." (<name>.<domain>) or "--" (<name>--<domain>)
	PublicScheme string `json:"public_scheme"` // http or https, as clients see services

	MaxBodyMB    int  `json:"max_body_mb"`   // a request body to a service
	HeapMB       int  `json:"heap_mb"`       // V8 heap limit per service process
	CPUSeconds   int  `json:"cpu_seconds"`   // continuous CPU a service process may burn before it is restarted
	IdleSeconds  int  `json:"idle_seconds"`  // a service process with no work for this long is stopped
	MaxProcesses int  `json:"max_processes"` // service processes running at once
	DiskMB       int  `json:"disk_mb"`       // a service's disk quota unless it has its own
	Egress       bool `json:"egress"`        // services may reach the public internet
}

// Defaults are the settings of a new runner.
func Defaults() Settings {
	return Settings{
		Separator:    ".",
		PublicScheme: "https",
		MaxBodyMB:    100,
		HeapMB:       128,
		CPUSeconds:   30,
		IdleSeconds:  600,
		MaxProcesses: 50,
		DiskMB:       5120,
		Egress:       true,
	}
}

var hostname = regexp.MustCompile(`^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)*[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// Validate normalizes s and reports what is wrong with it.
func (s *Settings) Validate() error {
	s.Domain = strings.ToLower(strings.TrimSpace(s.Domain))
	if s.Domain != "" && (len(s.Domain) > 200 || !hostname.MatchString(s.Domain)) {
		return errors.New("domain is a host name, such as zeroko.example.com")
	}
	if s.Separator == "" {
		s.Separator = "."
	}
	if s.Separator != "." && s.Separator != "--" {
		return errors.New(`separator is "." or "--"`)
	}
	if s.PublicScheme != "http" && s.PublicScheme != "https" {
		return errors.New("public_scheme is http or https")
	}
	for _, v := range []struct {
		name      string
		n, lo, hi int
	}{
		{"max_body_mb", s.MaxBodyMB, 1, 10240},
		{"heap_mb", s.HeapMB, 16, 8192},
		{"cpu_seconds", s.CPUSeconds, 1, 3600},
		{"idle_seconds", s.IdleSeconds, 10, 86400},
		{"max_processes", s.MaxProcesses, 1, 10000},
		{"disk_mb", s.DiskMB, 1, 1 << 30},
	} {
		if v.n < v.lo || v.n > v.hi {
			return fmt.Errorf("%s is %d to %d", v.name, v.lo, v.hi)
		}
	}
	return nil
}

// MaxBodyBytes is the request body limit for services.
func (s *Settings) MaxBodyBytes() int64 { return int64(s.MaxBodyMB) << 20 }

// ServiceURL is the public URL of a service, or "" while no domain is set.
func (s *Settings) ServiceURL(service string) string {
	if s.Domain == "" {
		return ""
	}
	return s.PublicScheme + "://" + s.ServiceHost(service)
}

// ServiceHost is the host a service is served at: <name><Separator><Domain>.
func (s *Settings) ServiceHost(service string) string {
	return service + s.Separator + s.Domain
}

// ServiceOf reads the service's name from a host, or "" when the host is
// no service's.
func (s *Settings) ServiceOf(host string) string {
	name, ok := strings.CutSuffix(host, s.Separator+s.Domain)
	if s.Domain == "" || !ok || name == "" || strings.Contains(name, ".") {
		return ""
	}
	return name
}
