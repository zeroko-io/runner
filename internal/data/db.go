// Package data keeps services' data: one directory per service with a SQLite
// file per db binding and a store per files binding.
package data

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

const (
	MaxDBBytes      = 1 << 30
	MaxStatements   = 1000
	MaxResultRows   = 100_000
	MaxResultBytes  = 32 << 20
	StatementTimout = 30 * time.Second
)

// pragmas a service may run; anything else that changes how SQLite stores or
// syncs the database is the platform's.
var allowedPragmas = map[string]bool{
	"table_info": true, "table_xinfo": true, "table_list": true, "index_list": true, "index_info": true,
	"index_xinfo": true, "foreign_key_list": true, "foreign_keys": true, "foreign_key_check": true,
	"integrity_check": true, "quick_check": true, "user_version": true, "defer_foreign_keys": true,
	"page_count": true, "page_size": true, "freelist_count": true, "function_list": true,
	"pragma_list": true, "collation_list": true, "data_version": true, "optimize": true,
	"recursive_triggers": true, "case_sensitive_like": true,
}

// The service database driver: a size limit, no attached databases, and an
// authorizer that keeps the service inside its database.
func init() {
	sql.Register("zeroko-service-db", &sqlite3.SQLiteDriver{
		ConnectHook: func(c *sqlite3.SQLiteConn) error {
			st := &connState{}
			connStates.Store(c, st)
			if _, err := c.Exec(fmt.Sprintf("PRAGMA max_page_count = %d", MaxDBBytes/pageSize), nil); err != nil {
				return err
			}
			c.SetLimit(sqlite3.SQLITE_LIMIT_ATTACHED, 0)
			c.SetLimit(sqlite3.SQLITE_LIMIT_LENGTH, 64<<20)
			c.RegisterAuthorizer(func(action int, arg1, arg2, _ string) int {
				switch action {
				case sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH:
					return sqlite3.SQLITE_DENY
				case sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT:
					if st.own.Load() {
						return sqlite3.SQLITE_OK
					}
					return sqlite3.SQLITE_DENY
				case sqlite3.SQLITE_PRAGMA:
					if st.own.Load() || allowedPragmas[strings.ToLower(arg1)] {
						return sqlite3.SQLITE_OK
					}
					return sqlite3.SQLITE_DENY
				case sqlite3.SQLITE_FUNCTION:
					if strings.EqualFold(arg2, "load_extension") {
						return sqlite3.SQLITE_DENY
					}
				}
				return sqlite3.SQLITE_OK
			})
			return nil
		},
	})
}

// connState is per connection: whether the runner, not the service, runs the
// statement: a transaction (services get them only through batches) or a pragma
// that sets limits.
type connState struct{ own atomic.Bool }

var connStates sync.Map // *sqlite3.SQLiteConn -> *connState

// Data holds the open databases and files stores of every service.
type Data struct {
	dir string
	// Quota returns the most a service may store, in bytes; 0 is no limit.
	Quota func(serviceID string) int64

	mu    sync.Mutex
	dbs   map[string]*sql.DB    // path -> database
	files map[string]*fileStore // directory -> store
}

func New(dir string) (*Data, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Data{dir: dir, dbs: map[string]*sql.DB{}, files: map[string]*fileStore{}}, nil
}

func (d *Data) serviceDir(serviceID string) string { return filepath.Join(d.dir, serviceID) }

func (d *Data) dbPath(serviceID, binding string) string {
	return filepath.Join(d.serviceDir(serviceID), "db", binding+".sqlite")
}

// DB opens (creating it if needed) the database of a service's binding.
func (d *Data) DB(serviceID, binding string) (*sql.DB, error) {
	path := d.dbPath(serviceID, binding)
	d.mu.Lock()
	defer d.mu.Unlock()
	if db := d.dbs[path]; db != nil {
		return db, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("zeroko-service-db", "file:"+path+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=10000&_foreign_keys=1")
	if err != nil {
		return nil, err
	}
	// One connection per database: statements and batches never interleave.
	db.SetMaxOpenConns(1)
	db.SetConnMaxIdleTime(0)
	d.dbs[path] = db
	return db, nil
}

// Statement is one SQL statement with its parameters, as JSON values;
// {"$blob": "<base64>"} is a blob.
type Statement struct {
	SQL    string            `json:"sql"`
	Params []json.RawMessage `json:"params"`
}

type Result struct {
	Rows      []map[string]any `json:"rows"`
	Columns   []string         `json:"columns"`
	Changes   int64            `json:"changes"`
	LastRowID int64            `json:"lastRowId"`
}

// SQLError is an error in the service's SQL, as opposed to the platform's.
type SQLError string

func (e SQLError) Error() string { return string(e) }

// Batch runs statements in one transaction.
func (d *Data) Batch(ctx context.Context, serviceID, binding string, stmts []Statement) ([]Result, error) {
	if len(stmts) == 0 || len(stmts) > MaxStatements {
		return nil, SQLError(fmt.Sprintf("a batch has 1 to %d statements", MaxStatements))
	}
	db, err := d.DB(serviceID, binding)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, StatementTimout)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	limit, byQuota := d.dbLimit(serviceID, binding)
	// At least 1: 0 would leave the limit as it was.
	if err := ownExec(ctx, conn, fmt.Sprintf("PRAGMA max_page_count = %d", max(limit/pageSize, 1))); err != nil {
		return nil, err
	}
	single := len(stmts) == 1
	if !single {
		if err := ownExec(ctx, conn, "BEGIN IMMEDIATE"); err != nil {
			return nil, err
		}
	}
	out := make([]Result, 0, len(stmts))
	size := 0
	for i, s := range stmts {
		r, n, err := run(ctx, conn, s, MaxResultBytes-size)
		if errors.Is(err, errFull) && byQuota {
			err = d.quotaFull(serviceID)
		}
		if err != nil {
			if !single {
				ownExec(context.Background(), conn, "ROLLBACK")
				err = SQLError(fmt.Sprintf("statement %d: %v", i+1, err))
			}
			return nil, err
		}
		size += n
		out = append(out, r)
	}
	if !single {
		if err := ownExec(ctx, conn, "COMMIT"); err != nil {
			ownExec(context.Background(), conn, "ROLLBACK")
			return nil, SQLError(err.Error())
		}
	}
	return out, nil
}

// ownExec runs one of the runner's own statements, which the authorizer
// denies to the service: BEGIN, COMMIT, ROLLBACK and limits.
func ownExec(ctx context.Context, conn *sql.Conn, stmt string) error {
	var st *connState
	conn.Raw(func(dc any) error {
		if v, ok := connStates.Load(dc); ok {
			st = v.(*connState)
		}
		return nil
	})
	if st == nil {
		return errors.New("unknown connection")
	}
	st.own.Store(true)
	defer st.own.Store(false)
	_, err := conn.ExecContext(ctx, stmt)
	return err
}

func run(ctx context.Context, conn *sql.Conn, s Statement, budget int) (Result, int, error) {
	args := make([]any, len(s.Params))
	for i, p := range s.Params {
		v, err := decodeParam(p)
		if err != nil {
			return Result{}, 0, SQLError(fmt.Sprintf("parameter %d: %v", i+1, err))
		}
		args[i] = v
	}
	r := Result{Rows: []map[string]any{}, Columns: []string{}}
	if multiple(s.SQL) {
		// A script: Exec runs each statement in turn; it returns no rows.
		res, err := conn.ExecContext(ctx, s.SQL, args...)
		if err != nil {
			return r, 0, sqlErr(err)
		}
		r.Changes, _ = res.RowsAffected()
		r.LastRowID, _ = res.LastInsertId()
		return r, 0, nil
	}
	rows, err := conn.QueryContext(ctx, s.SQL, args...)
	if err != nil {
		return r, 0, sqlErr(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return r, 0, sqlErr(err)
	}
	r.Columns = cols
	size := 0
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if len(r.Rows) >= MaxResultRows {
			return r, 0, SQLError(fmt.Sprintf("the result has more than %d rows: add a LIMIT", MaxResultRows))
		}
		if err := rows.Scan(ptrs...); err != nil {
			return r, 0, sqlErr(err)
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			row[c], size = encodeValue(vals[i], size)
		}
		if size > budget {
			return r, 0, SQLError(fmt.Sprintf("the result is larger than %d MB: select fewer rows or columns", MaxResultBytes>>20))
		}
		r.Rows = append(r.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return r, 0, sqlErr(err)
	}
	rows.Close()
	// changes() and last_insert_rowid() refer to the statement just run.
	if err := conn.QueryRowContext(ctx, "SELECT changes(), last_insert_rowid()").Scan(&r.Changes, &r.LastRowID); err != nil {
		return r, 0, err
	}
	if len(cols) > 0 && isRead(s.SQL) {
		r.Changes = 0
	}
	return r, size, nil
}

// multiple reports whether q holds more than one statement: a semicolon,
// outside quotes and comments, followed by more than whitespace and comments.
func multiple(q string) bool {
	semicolon := false
	for i := 0; i < len(q); i++ {
		c := q[i]
		switch {
		case c == '\'' || c == '"' || c == '`' || c == '[':
			end := c
			if c == '[' {
				end = ']'
			}
			if semicolon {
				return true
			}
			for i++; i < len(q) && q[i] != end; i++ {
			}
		case c == '-' && i+1 < len(q) && q[i+1] == '-':
			for i += 2; i < len(q) && q[i] != '\n'; i++ {
			}
		case c == '/' && i+1 < len(q) && q[i+1] == '*':
			i += 2
			for i+1 < len(q) && !(q[i] == '*' && q[i+1] == '/') {
				i++
			}
			i++
		case c == ';':
			semicolon = true
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
		default:
			if semicolon {
				return true
			}
		}
	}
	return false
}

func isRead(q string) bool {
	q = strings.ToUpper(strings.TrimSpace(q))
	return strings.HasPrefix(q, "SELECT") || strings.HasPrefix(q, "WITH") || strings.HasPrefix(q, "PRAGMA") || strings.HasPrefix(q, "EXPLAIN")
}

func sqlErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return SQLError(fmt.Sprintf("the statement ran longer than %s", StatementTimout))
	}
	if strings.Contains(err.Error(), "database or disk is full") {
		return errFull
	}
	return SQLError(err.Error())
}

var errFull = SQLError(fmt.Sprintf("the database reached its size limit (%d MB)", MaxDBBytes>>20))

const pageSize = 4096 // SQLite's default, which the runner keeps

// dbLimit is how large a database may grow: up to MaxDBBytes, within what
// the service's quota leaves. byQuota reports whether the quota is the tighter.
func (d *Data) dbLimit(serviceID, binding string) (limit int64, byQuota bool) {
	q := d.quota(serviceID)
	if q <= 0 {
		return MaxDBBytes, false
	}
	if room := q - d.Usage(serviceID) + d.DBSize(serviceID, binding); room < MaxDBBytes {
		return max(room, 0), true
	}
	return MaxDBBytes, false
}

func (d *Data) quota(serviceID string) int64 {
	if d.Quota == nil {
		return 0
	}
	return d.Quota(serviceID)
}

func (d *Data) quotaFull(serviceID string) error {
	return Invalid(fmt.Sprintf("the service's disk quota (%d MB) is full", d.quota(serviceID)>>20))
}

// Usage is the size of all of a service's databases and files.
func (d *Data) Usage(serviceID string) int64 {
	var n int64
	dbs, _ := filepath.Glob(filepath.Join(d.serviceDir(serviceID), "db", "*.sqlite"))
	for _, p := range dbs {
		n += d.DBSize(serviceID, strings.TrimSuffix(filepath.Base(p), ".sqlite"))
	}
	dirs, _ := os.ReadDir(filepath.Join(d.serviceDir(serviceID), "files"))
	for _, e := range dirs {
		if e.IsDir() && bindingName.MatchString(e.Name()) {
			if fs, err := d.fileStore(serviceID, e.Name()); err == nil {
				n += fs.bytes.Load()
			}
		}
	}
	return n
}

func decodeParam(raw json.RawMessage) (any, error) {
	var v any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	switch x := v.(type) {
	case nil, string:
		return x, nil
	case bool:
		if x {
			return int64(1), nil
		}
		return int64(0), nil
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, nil
		}
		return x.Float64()
	case map[string]any:
		if s, ok := x["$blob"].(string); ok && len(x) == 1 {
			return base64.StdEncoding.DecodeString(s)
		}
	}
	return nil, errors.New("use a string, number, boolean, null or bytes")
}

func encodeValue(v any, size int) (any, int) {
	switch x := v.(type) {
	case []byte:
		return map[string]string{"$blob": base64.StdEncoding.EncodeToString(x)}, size + len(x)*4/3 + 12
	case string:
		return x, size + len(x) + 2
	case time.Time:
		s := x.UTC().Format(time.RFC3339Nano)
		return s, size + len(s) + 2
	}
	return v, size + 8
}

// DBSize returns the size of a binding's database in bytes.
func (d *Data) DBSize(serviceID, binding string) int64 {
	var n int64
	for _, suffix := range []string{"", "-wal"} {
		if st, err := os.Stat(d.dbPath(serviceID, binding) + suffix); err == nil {
			n += st.Size()
		}
	}
	return n
}

// Tables lists the tables of a binding's database with their row counts.
func (d *Data) Tables(ctx context.Context, serviceID, binding string) ([]Table, error) {
	db, err := d.DB(serviceID, binding)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT name, sql FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	var out []Table
	for rows.Next() {
		var t Table
		if err := rows.Scan(&t.Name, &t.SQL); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, t)
	}
	rows.Close()
	for i := range out {
		db.QueryRowContext(ctx, `SELECT count(*) FROM "`+strings.ReplaceAll(out[i].Name, `"`, `""`)+`"`).Scan(&out[i].Rows)
	}
	return out, nil
}

type Table struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
	Rows int64  `json:"rows"`
}

// DeleteService closes and deletes all of a service's data.
func (d *Data) DeleteService(serviceID string) error {
	prefix := d.serviceDir(serviceID) + string(filepath.Separator)
	d.mu.Lock()
	for path, db := range d.dbs {
		if strings.HasPrefix(path, prefix) {
			db.Close()
			delete(d.dbs, path)
		}
	}
	for key, fs := range d.files {
		if strings.HasPrefix(key, prefix) {
			fs.close()
			delete(d.files, key)
		}
	}
	d.mu.Unlock()
	return os.RemoveAll(d.serviceDir(serviceID))
}

// Close closes every open database.
func (d *Data) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, db := range d.dbs {
		db.Close()
	}
	for _, fs := range d.files {
		fs.close()
	}
}
