package platform

import (
	"bytes"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zeroko-io/runner/internal/config"
	"github.com/zeroko-io/runner/internal/runtime"
)

// Lines keeps the newest lines written to it: the runner's own log.
type Lines struct {
	mu      sync.Mutex
	lines   []Line
	next    int64
	max     int
	partial []byte
}

type Line struct {
	Seq     int64  `json:"seq"`
	TS      int64  `json:"ts"`
	Message string `json:"message"`
}

func NewLines(max int) *Lines { return &Lines{max: max, next: 1} }

func (l *Lines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.partial = append(l.partial, p...)
	for {
		i := bytes.IndexByte(l.partial, '\n')
		if i < 0 {
			break
		}
		l.lines = append(l.lines, Line{Seq: l.next, TS: time.Now().UnixMilli(), Message: string(l.partial[:i])})
		l.next++
		l.partial = l.partial[i+1:]
	}
	if over := len(l.lines) - l.max; over > 0 {
		l.lines = append(l.lines[:0], l.lines[over:]...)
	}
	return len(p), nil
}

// After returns up to limit lines after seq, oldest first.
func (l *Lines) After(seq int64, limit int) []Line {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := []Line{}
	for _, line := range l.lines {
		if line.Seq > seq {
			out = append(out, line)
			if len(out) == limit {
				break
			}
		}
	}
	return out
}

type System struct {
	Version   string             `json:"version"`
	Workerd   string             `json:"workerd"`
	Started   int64              `json:"started"`
	Processes []runtime.ProcInfo `json:"processes"`
	Disk      Disk               `json:"disk"` // of the data volume
}

type Disk struct {
	Total int64 `json:"total"`
	Free  int64 `json:"free"`
}

var workerdVersion = sync.OnceValue(func() string {
	out, err := exec.Command(config.Workerd, "--version").Output()
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(strings.TrimSpace(string(out)), "workerd ")
})

// SystemInfo describes the runner and its running service processes.
func (p *Platform) SystemInfo() System {
	s := System{Version: p.Version, Workerd: workerdVersion(), Started: p.started.UnixMilli(), Processes: p.RT.Info()}
	var fs syscall.Statfs_t
	if syscall.Statfs(config.DataDir, &fs) == nil {
		s.Disk = Disk{Total: int64(fs.Blocks) * fs.Bsize, Free: int64(fs.Bavail) * fs.Bsize}
	}
	return s
}
