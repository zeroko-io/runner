// Package store is the runner's own database (runner.db): services,
// deployments, secrets, vars, logs, jobs, metrics and access tokens. Services' data lives elsewhere (see
// package data).
package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var ErrNotFound = errors.New("not found")

type Store struct {
	DB *sql.DB
	// Reads on every request (services, deployments, tokens, logs) on
	// connections of their own: WAL lets them run while DB writes, instead
	// of waiting for its one connection behind a batch of logs.
	read *sql.DB
}

// migrations are append-only: never edit one that has been released.
var migrations = []string{
	`CREATE TABLE services (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL UNIQUE,
		created_at INTEGER NOT NULL,
		active_deployment TEXT,
		stopped INTEGER NOT NULL DEFAULT 0,
		egress TEXT NOT NULL DEFAULT '["*"]',
		disk_mb INTEGER NOT NULL DEFAULT 0
	);
	CREATE TABLE deployments (
		id TEXT PRIMARY KEY,
		service_id TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
		number INTEGER NOT NULL,
		status TEXT NOT NULL,
		error TEXT NOT NULL DEFAULT '',
		manifest TEXT NOT NULL,
		bundle TEXT NOT NULL DEFAULT '',
		created_at INTEGER NOT NULL,
		UNIQUE (service_id, number)
	);
	CREATE TABLE secrets (
		service_id TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
		name TEXT NOT NULL,
		value BLOB NOT NULL,
		updated_at INTEGER NOT NULL,
		PRIMARY KEY (service_id, name)
	);
	CREATE TABLE vars (
		service_id TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
		name TEXT NOT NULL,
		value TEXT NOT NULL,
		updated_at INTEGER NOT NULL,
		PRIMARY KEY (service_id, name)
	);
	CREATE TABLE logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		service_id TEXT NOT NULL,
		deployment_id TEXT NOT NULL,
		ts INTEGER NOT NULL,
		source TEXT NOT NULL,
		level TEXT NOT NULL,
		message TEXT NOT NULL
	);
	CREATE INDEX logs_service ON logs (service_id, id);
	CREATE TABLE jobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		service_id TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
		payload TEXT NOT NULL,
		key TEXT NOT NULL DEFAULT '',
		cron TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL,
		attempts INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL,
		run_at INTEGER NOT NULL,
		error TEXT NOT NULL DEFAULT '',
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL
	);
	CREATE INDEX jobs_due ON jobs (status, run_at);
	CREATE INDEX jobs_service ON jobs (service_id, status, id);
	CREATE UNIQUE INDEX jobs_key ON jobs (service_id, key) WHERE key != '';
	CREATE TABLE metrics (
		service_id TEXT NOT NULL,
		minute INTEGER NOT NULL,
		requests INTEGER NOT NULL,
		errors INTEGER NOT NULL,
		exceptions INTEGER NOT NULL,
		outbound INTEGER NOT NULL,
		outbound_errors INTEGER NOT NULL,
		buckets TEXT NOT NULL,
		PRIMARY KEY (service_id, minute)
	);
	CREATE TABLE settings (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		value TEXT NOT NULL
	);
	CREATE TABLE tokens (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		prefix TEXT NOT NULL,
		hash TEXT NOT NULL UNIQUE,
		created_at INTEGER NOT NULL,
		last_used_at INTEGER NOT NULL DEFAULT 0,
		expires_at INTEGER NOT NULL DEFAULT 0
	);`,
	// A service's own rate limit, requests a second per client: 0 for the
	// runner's, -1 for none.
	`ALTER TABLE services ADD COLUMN rate_limit INTEGER NOT NULL DEFAULT 0`,
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", "file:"+path+"?_journal_mode=WAL&_synchronous=FULL&_busy_timeout=10000&_foreign_keys=1")
	if err != nil {
		return nil, err
	}
	// One connection: writes are serialized and never see SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	s := &Store{DB: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	if s.read, err = sql.Open("sqlite3", "file:"+path+"?mode=ro&_busy_timeout=10000"); err != nil {
		db.Close()
		return nil, err
	}
	s.read.SetMaxOpenConns(4)
	return s, nil
}

func (s *Store) Close() error {
	s.read.Close()
	return s.DB.Close()
}

func (s *Store) migrate() error {
	if _, err := s.DB.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return err
	}
	var v int
	if err := s.DB.QueryRow(`SELECT coalesce(max(version), 0) FROM schema_version`).Scan(&v); err != nil {
		return err
	}
	if v > len(migrations) {
		return fmt.Errorf("the database was created by a newer runner (schema %d, this version knows %d)", v, len(migrations))
	}
	for i := v; i < len(migrations); i++ {
		tx, err := s.DB.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(migrations[i]); err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := tx.Exec(`INSERT INTO schema_version (version) VALUES (?)`, i+1); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// NewID returns a random lowercase identifier.
func NewID() string {
	b := make([]byte, 10)
	rand.Read(b)
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b))
}

func now() int64 { return time.Now().UnixMilli() }

// Check reports whether the database at path is a runner.db this version
// can open.
func Check(path string) error {
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=1000")
	if err != nil {
		return err
	}
	defer db.Close()
	var v int
	if err := db.QueryRow(`SELECT coalesce(max(version), 0) FROM schema_version`).Scan(&v); err != nil {
		return errors.New("not a runner database")
	}
	if v > len(migrations) {
		return fmt.Errorf("written by a newer runner (schema %d, this version knows %d)", v, len(migrations))
	}
	return nil
}
