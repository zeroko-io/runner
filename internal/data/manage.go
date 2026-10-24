package data

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Store is one binding's data on disk.
type Store struct {
	Type    string `json:"type"` // db or files
	Binding string `json:"binding"`
	Bytes   int64  `json:"bytes"`
	Count   int64  `json:"count,omitempty"` // files
}

var bindingName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

// Stores lists a service's stores, bound or not.
func (d *Data) Stores(ctx context.Context, serviceID string) ([]Store, error) {
	out := []Store{}
	dbs, _ := filepath.Glob(filepath.Join(d.serviceDir(serviceID), "db", "*.sqlite"))
	for _, p := range dbs {
		b := strings.TrimSuffix(filepath.Base(p), ".sqlite")
		out = append(out, Store{Type: "db", Binding: b, Bytes: d.DBSize(serviceID, b)})
	}
	dirs, _ := os.ReadDir(filepath.Join(d.serviceDir(serviceID), "files"))
	for _, e := range dirs {
		if !e.IsDir() || !bindingName.MatchString(e.Name()) {
			continue
		}
		s := Store{Type: "files", Binding: e.Name()}
		var err error
		if s.Count, s.Bytes, err = d.FilesUsage(ctx, serviceID, e.Name()); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Binding < out[j].Binding })
	return out, nil
}

// close closes the open handle of a store, if any.
func (d *Data) close(serviceID, typ, binding string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if typ == "db" {
		p := d.dbPath(serviceID, binding)
		if db := d.dbs[p]; db != nil {
			db.Close()
			delete(d.dbs, p)
		}
		return
	}
	dir := filepath.Join(d.serviceDir(serviceID), "files", binding)
	if fs := d.files[dir]; fs != nil {
		fs.close()
		delete(d.files, dir)
	}
}

func (d *Data) storePaths(serviceID, typ, binding string) []string {
	if typ == "db" {
		p := d.dbPath(serviceID, binding)
		return []string{p, p + "-wal", p + "-shm"}
	}
	return []string{filepath.Join(d.serviceDir(serviceID), "files", binding)}
}

// DeleteStore deletes one store's data. A binding that still uses it starts
// empty.
func (d *Data) DeleteStore(serviceID, typ, binding string) error {
	if (typ != "db" && typ != "files") || !bindingName.MatchString(binding) {
		return ErrNotFound
	}
	d.close(serviceID, typ, binding)
	for _, p := range d.storePaths(serviceID, typ, binding) {
		if err := os.RemoveAll(p); err != nil {
			return err
		}
	}
	return nil
}

// Snapshot writes a consistent copy of a SQLite file to dst.
func Snapshot(src, dst string) error {
	db, err := sql.Open("sqlite3", "file:"+src+"?mode=ro&_busy_timeout=10000")
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`VACUUM INTO ?`, dst)
	return err
}

// AddFile writes a file to an archive as name.
func AddFile(tw *tar.Writer, name, file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: st.Size(), ModTime: st.ModTime()}); err != nil {
		return err
	}
	_, err = io.Copy(tw, f)
	return err
}

// Export writes all of a service's stores to w as a gzipped tar: db/<binding>
// .sqlite, files/<binding>/index.sqlite and files/<binding>/blobs/….
func (d *Data) Export(ctx context.Context, serviceID string, w io.Writer) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	if err := d.ExportTo(ctx, serviceID, tw, ""); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

// ExportTo writes a service's stores to an archive, their names under prefix.
func (d *Data) ExportTo(ctx context.Context, serviceID string, tw *tar.Writer, prefix string) error {
	stores, err := d.Stores(ctx, serviceID)
	if err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(d.dir, ".export-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	for i, s := range stores {
		snap := filepath.Join(tmp, fmt.Sprintf("%d.sqlite", i))
		if s.Type == "db" {
			if err := Snapshot(d.dbPath(serviceID, s.Binding), snap); err != nil {
				return fmt.Errorf("%s: %w", s.Binding, err)
			}
			if err := AddFile(tw, prefix+"db/"+s.Binding+".sqlite", snap); err != nil {
				return err
			}
			continue
		}
		dir := filepath.Join(d.serviceDir(serviceID), "files", s.Binding)
		if err := Snapshot(filepath.Join(dir, "index.sqlite"), snap); err != nil {
			return fmt.Errorf("%s: %w", s.Binding, err)
		}
		blobs, err := snapshotBlobs(snap, filepath.Join(dir, "blobs"))
		if err != nil {
			return err
		}
		if err := AddFile(tw, prefix+"files/"+s.Binding+"/index.sqlite", snap); err != nil {
			return err
		}
		for _, b := range blobs {
			if err := AddFile(tw, prefix+"files/"+s.Binding+"/blobs/"+b, filepath.Join(dir, "blobs", b)); err != nil {
				return err
			}
		}
	}
	return nil
}

// snapshotBlobs lists the blobs an index snapshot references, dropping the
// rows whose blob was replaced or deleted since the snapshot.
func snapshotBlobs(index, dir string) ([]string, error) {
	db, err := sql.Open("sqlite3", "file:"+index)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.Query(`SELECT blob FROM files`)
	if err != nil {
		return nil, err
	}
	var present, gone []string
	for rows.Next() {
		var b string
		if err := rows.Scan(&b); err != nil {
			rows.Close()
			return nil, err
		}
		if _, err := os.Stat(filepath.Join(dir, b)); err == nil {
			present = append(present, b)
		} else {
			gone = append(gone, b)
		}
	}
	rows.Close()
	for _, b := range gone {
		if _, err := db.Exec(`DELETE FROM files WHERE blob = ?`, b); err != nil {
			return nil, err
		}
	}
	return present, nil
}

var (
	dbEntry    = regexp.MustCompile(`^db/([A-Za-z_][A-Za-z0-9_]{0,63})\.sqlite$`)
	indexEntry = regexp.MustCompile(`^files/([A-Za-z_][A-Za-z0-9_]{0,63})/index\.sqlite$`)
	blobEntry  = regexp.MustCompile(`^files/([A-Za-z_][A-Za-z0-9_]{0,63})/blobs/([a-z0-9]{1,64})$`)
)

// Import replaces each store found in an archive written by Export with the
// archive's copy; the service's other stores stay. It returns the stores
// imported, as type:binding.
func (d *Data) Import(serviceID string, r io.Reader) ([]string, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, Invalid("not a gzipped archive")
	}
	if err := os.MkdirAll(d.serviceDir(serviceID), 0o700); err != nil {
		return nil, err
	}
	stage, err := os.MkdirTemp(d.serviceDir(serviceID), ".import-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(stage)
	stores := map[[2]string]bool{}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, Invalid("the archive is damaged: " + err.Error())
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		name := path.Clean(h.Name)
		var key [2]string
		switch {
		case dbEntry.MatchString(name):
			key = [2]string{"db", dbEntry.FindStringSubmatch(name)[1]}
		case indexEntry.MatchString(name):
			key = [2]string{"files", indexEntry.FindStringSubmatch(name)[1]}
		case blobEntry.MatchString(name):
			key = [2]string{"files", blobEntry.FindStringSubmatch(name)[1]}
		default:
			return nil, Invalid("unexpected file in the archive: " + name)
		}
		stores[key] = true
		dst := filepath.Join(stage, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, err
		}
		_, err = io.Copy(f, tr)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return nil, err
		}
	}
	if len(stores) == 0 {
		return nil, Invalid("the archive has no stores")
	}
	var out []string
	for key := range stores {
		typ, binding := key[0], key[1]
		if typ == "files" {
			if _, err := os.Stat(filepath.Join(stage, "files", binding, "index.sqlite")); err != nil {
				return nil, Invalid("files/" + binding + " has no index.sqlite")
			}
		}
		if err := d.DeleteStore(serviceID, typ, binding); err != nil {
			return nil, err
		}
		src := filepath.Join(stage, "db", binding+".sqlite")
		dst := d.dbPath(serviceID, binding)
		if typ == "files" {
			src, dst = filepath.Join(stage, "files", binding), filepath.Join(d.serviceDir(serviceID), "files", binding)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return nil, err
		}
		if err := os.Rename(src, dst); err != nil {
			return nil, err
		}
		if typ == "files" {
			os.MkdirAll(filepath.Join(dst, "blobs"), 0o700)
		}
		out = append(out, typ+":"+binding)
	}
	sort.Strings(out)
	return out, nil
}
