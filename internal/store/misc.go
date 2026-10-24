package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/zeroko-io/runner/internal/config"
)

// Secrets. Values are stored encrypted (see package secrets).

func (s *Store) PutSecret(ctx context.Context, serviceID, name string, value []byte) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO secrets (service_id, name, value, updated_at) VALUES (?, ?, ?, ?)
		ON CONFLICT (service_id, name) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`, serviceID, name, value, now())
	return err
}

func (s *Store) DeleteSecret(ctx context.Context, serviceID, name string) (bool, error) {
	r, err := s.DB.ExecContext(ctx, `DELETE FROM secrets WHERE service_id = ? AND name = ?`, serviceID, name)
	if err != nil {
		return false, err
	}
	n, _ := r.RowsAffected()
	return n > 0, nil
}

type Secret struct {
	Name      string `json:"name"`
	UpdatedAt int64  `json:"updated_at"`
	Value     []byte `json:"-"`
}

func (s *Store) Secrets(ctx context.Context, serviceID string) ([]Secret, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT name, updated_at, value FROM secrets WHERE service_id = ? ORDER BY name`, serviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Secret{}
	for rows.Next() {
		var x Secret
		if err := rows.Scan(&x.Name, &x.UpdatedAt, &x.Value); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

// Vars set on a service, in plain text.

type Var struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	UpdatedAt int64  `json:"updated_at"`
}

func (s *Store) PutVar(ctx context.Context, serviceID, name, value string) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO vars (service_id, name, value, updated_at) VALUES (?, ?, ?, ?)
		ON CONFLICT (service_id, name) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`, serviceID, name, value, now())
	return err
}

func (s *Store) DeleteVar(ctx context.Context, serviceID, name string) (bool, error) {
	r, err := s.DB.ExecContext(ctx, `DELETE FROM vars WHERE service_id = ? AND name = ?`, serviceID, name)
	if err != nil {
		return false, err
	}
	n, _ := r.RowsAffected()
	return n > 0, nil
}

func (s *Store) Vars(ctx context.Context, serviceID string) ([]Var, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT name, value, updated_at FROM vars WHERE service_id = ? ORDER BY name`, serviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Var{}
	for rows.Next() {
		var x Var
		if err := rows.Scan(&x.Name, &x.Value, &x.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

// Logs.

type Log struct {
	ID         int64  `json:"id"`
	Deployment string `json:"deployment"`
	TS         int64  `json:"ts"`
	Source     string `json:"source"` // request, console, fetch or runtime
	Level      string `json:"level"`
	Message    string `json:"message"`
}

func (s *Store) AddLogs(ctx context.Context, serviceID string, logs []Log) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, l := range logs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO logs (service_id, deployment_id, ts, source, level, message) VALUES (?, ?, ?, ?, ?, ?)`,
			serviceID, l.Deployment, l.TS, l.Source, l.Level, l.Message); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Logs returns up to limit entries after the given id, oldest first, or the
// newest limit entries when after is 0.
func (s *Store) Logs(ctx context.Context, serviceID string, after int64, limit int) ([]Log, error) {
	var rows *sql.Rows
	var err error
	if after > 0 {
		rows, err = s.DB.QueryContext(ctx, `SELECT id, deployment_id, ts, source, level, message FROM logs WHERE service_id = ? AND id > ? ORDER BY id LIMIT ?`, serviceID, after, limit)
	} else {
		rows, err = s.DB.QueryContext(ctx, `SELECT * FROM (SELECT id, deployment_id, ts, source, level, message FROM logs WHERE service_id = ? ORDER BY id DESC LIMIT ?) ORDER BY id`, serviceID, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Log{}
	for rows.Next() {
		var l Log
		if err := rows.Scan(&l.ID, &l.Deployment, &l.TS, &l.Source, &l.Level, &l.Message); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// PruneLogs keeps the newest keep entries of each service and none older than
// minTS.
func (s *Store) PruneLogs(ctx context.Context, keep int, minTS int64) error {
	_, err := s.DB.ExecContext(ctx, `DELETE FROM logs WHERE ts < ? OR id IN (
		SELECT id FROM (SELECT id, row_number() OVER (PARTITION BY service_id ORDER BY id DESC) AS n FROM logs) WHERE n > ?)`, minTS, keep)
	return err
}

// Settings: one row, the JSON of config.Settings.

// Settings returns the runner's settings: the defaults, and over them what
// was set.
func (s *Store) Settings(ctx context.Context) (*config.Settings, error) {
	out := config.Defaults()
	var v string
	err := s.DB.QueryRowContext(ctx, `SELECT value FROM settings WHERE id = 1`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return &out, nil
	}
	if err != nil {
		return nil, err
	}
	return &out, json.Unmarshal([]byte(v), &out)
}

func (s *Store) PutSettings(ctx context.Context, v *config.Settings) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO settings (id, value) VALUES (1, ?) ON CONFLICT (id) DO UPDATE SET value = excluded.value`, string(b))
	return err
}

// Tokens. Only hashes are stored.

type Token struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Prefix     string `json:"prefix"`
	CreatedAt  int64  `json:"created_at"`
	LastUsedAt int64  `json:"last_used_at"`
	ExpiresAt  int64  `json:"expires_at"` // 0: never
}

func (s *Store) AddToken(ctx context.Context, name, hash, prefix string, expiresAt int64) (*Token, error) {
	t := &Token{ID: NewID(), Name: name, Prefix: prefix, CreatedAt: now(), ExpiresAt: expiresAt}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO tokens (id, name, hash, prefix, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?)`, t.ID, t.Name, hash, t.Prefix, t.CreatedAt, t.ExpiresAt)
	return t, err
}

func (s *Store) Tokens(ctx context.Context) ([]Token, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT id, name, prefix, created_at, last_used_at, expires_at FROM tokens ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Token{}
	for rows.Next() {
		var t Token
		if err := rows.Scan(&t.ID, &t.Name, &t.Prefix, &t.CreatedAt, &t.LastUsedAt, &t.ExpiresAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) DeleteToken(ctx context.Context, id string) (bool, error) {
	r, err := s.DB.ExecContext(ctx, `DELETE FROM tokens WHERE id = ?`, id)
	if err != nil {
		return false, err
	}
	n, _ := r.RowsAffected()
	return n > 0, nil
}

// UseToken returns the name of the token with this hash, if it exists and
// has not expired, and records its use; ok is false otherwise.
func (s *Store) UseToken(ctx context.Context, hash string) (name string, ok bool, err error) {
	err = s.DB.QueryRowContext(ctx, `UPDATE tokens SET last_used_at = ? WHERE hash = ? AND (expires_at = 0 OR expires_at > ?) RETURNING name`, now(), hash, now()).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return name, err == nil, err
}
