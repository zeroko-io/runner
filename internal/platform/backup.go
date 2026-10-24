package platform

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/zeroko-io/runner/internal/config"
	"github.com/zeroko-io/runner/internal/data"
	"github.com/zeroko-io/runner/internal/store"
)

// Backup writes a copy of the whole instance to w as a gzipped tar, while
// it runs: runner.db, the master key (secrets cannot be read without it),
// the deployed code and every service's data. Each database is a consistent
// snapshot of its own.
func (p *Platform) Backup(ctx context.Context, w io.Writer) error {
	tmp, err := os.MkdirTemp(config.DataDir, ".backup-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	snap := filepath.Join(tmp, "runner.db")
	if err := data.Snapshot(filepath.Join(config.DataDir, "runner.db"), snap); err != nil {
		return err
	}
	if err := data.AddFile(tw, "runner.db", snap); err != nil {
		return err
	}
	if err := data.AddFile(tw, "master.key", filepath.Join(config.DataDir, "master.key")); err != nil {
		return err
	}
	bundles, err := os.ReadDir(p.bundlesDir)
	if err != nil {
		return err
	}
	for _, b := range bundles {
		if !strings.HasSuffix(b.Name(), ".json") {
			continue
		}
		err := data.AddFile(tw, "bundles/"+b.Name(), filepath.Join(p.bundlesDir, b.Name()))
		if err != nil && !errors.Is(err, os.ErrNotExist) { // pruned since the listing
			return err
		}
	}
	services, err := p.Store.Services(ctx)
	if err != nil {
		return err
	}
	for _, a := range services {
		if err := p.Data.ExportTo(ctx, a.ID, tw, "services/"+a.ID+"/"); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

var backupEntry = regexp.MustCompile(`^(runner\.db|master\.key|bundles/[0-9a-f]{64}\.json|services/[a-z0-9]{1,32}/(db/[A-Za-z_][A-Za-z0-9_]{0,63}\.sqlite|files/[A-Za-z_][A-Za-z0-9_]{0,63}/(index\.sqlite|blobs/[a-z0-9]{1,64})))$`)

// restoreDir holds a backup waiting for the runner to start again.
func restoreDir() string { return filepath.Join(config.DataDir, "restore") }

// StageRestore unpacks and checks a backup next to the data it replaces.
// The runner puts it in place on its next start (ApplyRestore): call
// RequestRestart.
func (p *Platform) StageRestore(r io.Reader) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return Invalid("not a gzipped archive")
	}
	stage, err := os.MkdirTemp(config.DataDir, ".restore-")
	if err != nil {
		return err
	}
	staged := false
	defer func() {
		if !staged {
			os.RemoveAll(stage)
		}
	}()
	seen := map[string]bool{}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return Invalid("the archive is damaged: " + err.Error())
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		name := path.Clean(h.Name)
		if !backupEntry.MatchString(name) {
			return Invalid("not a runner backup: unexpected file " + name)
		}
		dst := filepath.Join(stage, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return err
		}
		f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		_, err = io.Copy(f, tr)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
		seen[name] = true
	}
	if !seen["runner.db"] || !seen["master.key"] {
		return Invalid("not a runner backup: runner.db or master.key is missing")
	}
	if err := store.Check(filepath.Join(stage, "runner.db")); err != nil {
		return Invalid("the backup's runner.db: " + err.Error())
	}
	if st, err := os.Stat(filepath.Join(stage, "master.key")); err != nil || st.Size() != 32 {
		return Invalid("the backup's master.key is not a key")
	}
	for _, dir := range []string{"bundles", "services"} {
		if err := os.MkdirAll(filepath.Join(stage, dir), 0o700); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(restoreDir()); err != nil {
		return err
	}
	if err := os.Rename(stage, restoreDir()); err != nil {
		return err
	}
	staged = true
	return nil
}

// ApplyRestore puts a staged backup in place of the instance's data. It runs
// at startup, before anything opens the data; one interrupted halfway goes
// on where it stopped on the next start.
func ApplyRestore() (bool, error) {
	stage := restoreDir()
	if _, err := os.Stat(stage); errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	for _, name := range []string{"runner.db", "master.key", "bundles", "services"} {
		src := filepath.Join(stage, name)
		if _, err := os.Stat(src); errors.Is(err, os.ErrNotExist) {
			continue // moved already
		}
		dst := filepath.Join(config.DataDir, name)
		if name == "runner.db" {
			os.Remove(dst + "-wal")
			os.Remove(dst + "-shm")
		}
		if err := os.RemoveAll(dst); err != nil {
			return true, err
		}
		if err := os.Rename(src, dst); err != nil {
			return true, err
		}
	}
	// Files extracted from the replaced bundles; the runner extracts them again.
	if err := os.RemoveAll(filepath.Join(config.DataDir, "assets")); err != nil {
		return true, err
	}
	return true, os.RemoveAll(stage)
}

// RequestRestart asks the runner to stop and start again in place.
func (p *Platform) RequestRestart() { p.restartOnce.Do(func() { close(p.restart) }) }

// Restarting is closed when a restart is requested.
func (p *Platform) Restarting() <-chan struct{} { return p.restart }
