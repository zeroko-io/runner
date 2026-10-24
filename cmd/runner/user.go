package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/zeroko-io/runner/internal/config"
)

// asUser runs the runner as ZEROKO_UID and ZEROKO_GID (1000 by default) when
// it starts as root, giving them the data directory first. Started as any
// other user, the runner stays that user and leaves the directory as it is.
func asUser() error {
	if os.Getuid() != 0 {
		return nil
	}
	uid, err := id("ZEROKO_UID")
	if err != nil {
		return err
	}
	gid, err := id("ZEROKO_GID")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(config.DataDir, 0o700); err != nil {
		return err
	}
	if st, err := os.Stat(config.DataDir); err != nil {
		return err
	} else if s := st.Sys().(*syscall.Stat_t); int(s.Uid) != uid || int(s.Gid) != gid {
		// The directory itself last: a chown cut short is done again on the
		// next start.
		err := filepath.WalkDir(config.DataDir, func(path string, _ fs.DirEntry, err error) error {
			if err != nil || path == config.DataDir {
				return err
			}
			return os.Lchown(path, uid, gid)
		})
		if err != nil {
			return err
		}
		if err := os.Chown(config.DataDir, uid, gid); err != nil {
			return err
		}
	}
	if err := syscall.Setgroups(nil); err != nil {
		return err
	}
	if err := syscall.Setgid(gid); err != nil {
		return err
	}
	return syscall.Setuid(uid)
}

func id(key string) (int, error) {
	s := strings.TrimSpace(os.Getenv(key))
	if s == "" {
		return 1000, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}
