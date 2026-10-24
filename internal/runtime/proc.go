package runtime

import (
	"bytes"
	"os"
	"strconv"
	"syscall"
)

const clockTicks = 100 // USER_HZ on Linux

func sysProcAttr() *syscall.SysProcAttr {
	// The process dies with the runner.
	return &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL, Setpgid: true}
}

// sampleCPU is called once a second. It returns for how many consecutive
// seconds the process has kept a core busy (90% or more): workerd runs a
// service on one thread, so a busy core means a request that never yields.
func (p *Proc) sampleCPU() int {
	if p.cmd == nil || p.cmd.Process == nil {
		return 0
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(p.cmd.Process.Pid) + "/stat")
	if err != nil {
		return 0
	}
	// Fields after the command name, which is in parentheses: utime is the
	// 12th and stime the 13th.
	i := bytes.LastIndexByte(b, ')')
	if i < 0 {
		return 0
	}
	f := bytes.Fields(b[i+1:])
	if len(f) < 13 {
		return 0
	}
	ut, _ := strconv.ParseUint(string(f[11]), 10, 64)
	st, _ := strconv.ParseUint(string(f[12]), 10, 64)
	ticks := ut + st
	if p.cpuTicks != 0 {
		p.cpuPercent.Store(int64((ticks - p.cpuTicks) * 100 / clockTicks))
	}
	if p.cpuTicks != 0 && ticks-p.cpuTicks >= clockTicks*9/10 {
		p.cpuBusySec++
	} else {
		p.cpuBusySec = 0
	}
	p.cpuTicks = ticks
	return p.cpuBusySec
}

// memory is the process's own memory in bytes: its anonymous pages (V8's
// heap and workerd's own state), from /proc/<pid>/smaps, which Linux and
// gVisor both have. Its resident size would count the pages of workerd's
// binary too, which every process shares, and gVisor counts them generously:
// three times this for an idle service. Without smaps, the resident size.
func (p *Proc) memory() int64 {
	pid := strconv.Itoa(p.cmd.Process.Pid)
	if b, err := os.ReadFile("/proc/" + pid + "/smaps"); err == nil {
		var kb int64
		for _, line := range bytes.Split(b, []byte("\n")) {
			if v, ok := bytes.CutPrefix(line, []byte("Anonymous:")); ok {
				if f := bytes.Fields(v); len(f) > 0 {
					n, _ := strconv.ParseInt(string(f[0]), 10, 64)
					kb += n
				}
			}
		}
		if kb > 0 {
			return kb << 10
		}
	}
	return p.statusKB("VmRSS:") << 10
}

// statusKB reads a field of /proc/<pid>/status, in kB.
func (p *Proc) statusKB(field string) int64 {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(p.cmd.Process.Pid) + "/status")
	if err != nil {
		return 0
	}
	for _, line := range bytes.Split(b, []byte("\n")) {
		if v, ok := bytes.CutPrefix(line, []byte(field)); ok {
			if f := bytes.Fields(v); len(f) > 0 {
				kb, _ := strconv.ParseInt(string(f[0]), 10, 64)
				return kb
			}
		}
	}
	return 0
}
