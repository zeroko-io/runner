package data

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

const (
	MaxFileBytes = 5 << 30
	MaxKeyBytes  = 1024
	MaxListLimit = 1000
)

var ErrNotFound = errors.New("not found")

// File is a file's metadata.
type File struct {
	Key      string `json:"key"`
	Size     int64  `json:"size"`
	Type     string `json:"type"`
	ETag     string `json:"etag"`
	Uploaded int64  `json:"uploaded"`
	blob     string
}

// fileStore is one files binding: contents in blobs/, one file each, and
// their index in index.sqlite.
type fileStore struct {
	dir   string
	db    *sql.DB
	bytes atomic.Int64 // the files' total size
}

func (fs *fileStore) close() { fs.db.Close() }

func (d *Data) fileStore(serviceID, binding string) (*fileStore, error) {
	dir := filepath.Join(d.serviceDir(serviceID), "files", binding)
	d.mu.Lock()
	defer d.mu.Unlock()
	if fs := d.files[dir]; fs != nil {
		return fs, nil
	}
	if err := os.MkdirAll(filepath.Join(dir, "blobs"), 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(dir, "index.sqlite")+"?_journal_mode=WAL&_synchronous=FULL&_busy_timeout=10000")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS files (key TEXT PRIMARY KEY, blob TEXT NOT NULL, size INTEGER NOT NULL, type TEXT NOT NULL, etag TEXT NOT NULL, uploaded INTEGER NOT NULL)`); err != nil {
		db.Close()
		return nil, err
	}
	fs := &fileStore{dir: dir, db: db}
	var n int64
	if err := db.QueryRow(`SELECT coalesce(sum(size), 0) FROM files`).Scan(&n); err != nil {
		db.Close()
		return nil, err
	}
	fs.bytes.Store(n)
	d.files[dir] = fs
	return fs, nil
}

func validKey(key string) error {
	if key == "" || len(key) > MaxKeyBytes {
		return Invalid("file keys are 1 to 1024 bytes")
	}
	return nil
}

// Invalid is an error in what the service asked for.
type Invalid string

func (e Invalid) Error() string { return string(e) }

// PutFile stores body under key, replacing any file there.
func (d *Data) PutFile(ctx context.Context, serviceID, binding, key, typ string, body io.Reader) (*File, error) {
	if err := validKey(key); err != nil {
		return nil, err
	}
	fs, err := d.fileStore(serviceID, binding)
	if err != nil {
		return nil, err
	}
	// The file may take what the quota leaves, plus the room of the file it
	// replaces.
	limit, byQuota := int64(MaxFileBytes), false
	if q := d.quota(serviceID); q > 0 {
		var replaced int64
		fs.db.QueryRowContext(ctx, `SELECT size FROM files WHERE key = ?`, key).Scan(&replaced)
		if room := q - d.Usage(serviceID) + replaced; room < limit {
			limit, byQuota = max(room, 0), true
		}
	}
	tmp, err := os.CreateTemp(filepath.Join(fs.dir, "blobs"), ".upload-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name())
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(body, limit+1))
	if err == nil && n > limit {
		err = Invalid("files are at most 5 GB")
		if byQuota {
			err = d.quotaFull(serviceID)
		}
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	if typ == "" {
		typ = "application/octet-stream"
	}
	sum := hex.EncodeToString(h.Sum(nil))
	f := &File{Key: key, Size: n, Type: typ, ETag: `"` + sum[:32] + `"`, Uploaded: time.Now().UnixMilli(), blob: randomName()}
	if err := os.Rename(tmp.Name(), filepath.Join(fs.dir, "blobs", f.blob)); err != nil {
		return nil, err
	}
	var old string
	var oldSize int64
	tx, err := fs.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tx.QueryRowContext(ctx, `SELECT blob, size FROM files WHERE key = ?`, key).Scan(&old, &oldSize)
	if _, err := tx.ExecContext(ctx, `INSERT INTO files (key, blob, size, type, etag, uploaded) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (key) DO UPDATE SET blob = excluded.blob, size = excluded.size, type = excluded.type, etag = excluded.etag, uploaded = excluded.uploaded`,
		f.Key, f.blob, f.Size, f.Type, f.ETag, f.Uploaded); err != nil {
		os.Remove(filepath.Join(fs.dir, "blobs", f.blob))
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		os.Remove(filepath.Join(fs.dir, "blobs", f.blob))
		return nil, err
	}
	fs.bytes.Add(f.Size - oldSize)
	if old != "" {
		os.Remove(filepath.Join(fs.dir, "blobs", old))
	}
	return f, nil
}

// OpenFile returns a file's metadata and content. The caller closes it.
func (d *Data) OpenFile(ctx context.Context, serviceID, binding, key string) (*File, *os.File, error) {
	f, err := d.StatFile(ctx, serviceID, binding, key)
	if err != nil {
		return nil, nil, err
	}
	fs, _ := d.fileStore(serviceID, binding)
	r, err := os.Open(filepath.Join(fs.dir, "blobs", f.blob))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, ErrNotFound // replaced or deleted since the lookup
	}
	return f, r, err
}

func (d *Data) StatFile(ctx context.Context, serviceID, binding, key string) (*File, error) {
	if err := validKey(key); err != nil {
		return nil, err
	}
	fs, err := d.fileStore(serviceID, binding)
	if err != nil {
		return nil, err
	}
	f := &File{Key: key}
	err = fs.db.QueryRowContext(ctx, `SELECT blob, size, type, etag, uploaded FROM files WHERE key = ?`, key).Scan(&f.blob, &f.Size, &f.Type, &f.ETag, &f.Uploaded)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return f, err
}

func (d *Data) DeleteFile(ctx context.Context, serviceID, binding, key string) error {
	f, err := d.StatFile(ctx, serviceID, binding, key)
	if err != nil {
		return err
	}
	fs, _ := d.fileStore(serviceID, binding)
	r, err := fs.db.ExecContext(ctx, `DELETE FROM files WHERE key = ? AND blob = ?`, key, f.blob)
	if err != nil {
		return err
	}
	if n, _ := r.RowsAffected(); n > 0 {
		fs.bytes.Add(-f.Size)
	}
	os.Remove(filepath.Join(fs.dir, "blobs", f.blob))
	return nil
}

type FileList struct {
	Files  []File `json:"files"`
	Cursor string `json:"cursor,omitempty"` // set when there are more
}

// ListFiles lists files in key order: those starting with prefix, after the
// cursor.
func (d *Data) ListFiles(ctx context.Context, serviceID, binding, prefix, cursor string, limit int) (*FileList, error) {
	if limit <= 0 || limit > MaxListLimit {
		limit = MaxListLimit
	}
	fs, err := d.fileStore(serviceID, binding)
	if err != nil {
		return nil, err
	}
	after := cursor
	if after < prefix {
		after = ""
	}
	rows, err := fs.db.QueryContext(ctx, `SELECT key, size, type, etag, uploaded FROM files
		WHERE key > ? AND substr(key, 1, ?) = ? ORDER BY key LIMIT ?`, after, len(prefix), prefix, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := &FileList{Files: []File{}}
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.Key, &f.Size, &f.Type, &f.ETag, &f.Uploaded); err != nil {
			return nil, err
		}
		out.Files = append(out.Files, f)
	}
	if len(out.Files) > limit {
		out.Files = out.Files[:limit]
		out.Cursor = out.Files[limit-1].Key
	}
	return out, rows.Err()
}

// FilesUsage returns the number and total size of a binding's files.
func (d *Data) FilesUsage(ctx context.Context, serviceID, binding string) (count, size int64, err error) {
	fs, err := d.fileStore(serviceID, binding)
	if err != nil {
		return 0, 0, err
	}
	err = fs.db.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(size), 0) FROM files`).Scan(&count, &size)
	return
}

func randomName() string { return strings.ToLower(rand.Text()) }
