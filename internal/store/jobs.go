package store

import (
	"context"
	"database/sql"
	"errors"
)

type Job struct {
	ID          int64  `json:"id"`
	ServiceID   string `json:"-"`
	Payload     string `json:"payload"`       // JSON
	Key         string `json:"key,omitempty"` // while a job with a key is not done, it is the only one
	Cron        string `json:"cron,omitempty"`
	Status      string `json:"status"` // queued, running or dead
	Attempts    int    `json:"attempts"`
	MaxAttempts int    `json:"max_attempts"`
	RunAt       int64  `json:"run_at"`
	Error       string `json:"error,omitempty"`
	CreatedAt   int64  `json:"created_at"`
}

const jobCols = `id, service_id, payload, key, cron, status, attempts, max_attempts, run_at, error, created_at`

func scanJob(row interface{ Scan(...any) error }) (*Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.ServiceID, &j.Payload, &j.Key, &j.Cron, &j.Status, &j.Attempts, &j.MaxAttempts, &j.RunAt, &j.Error, &j.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &j, err
}

// Enqueue queues j and reports true. When a job with j's key is queued,
// running or dead, it leaves j out, fills j with that job and reports false.
func (s *Store) Enqueue(ctx context.Context, j *Job) (bool, error) {
	t := now()
	if j.RunAt == 0 {
		j.RunAt = t
	}
	j.Status, j.CreatedAt = "queued", t
	err := s.DB.QueryRowContext(ctx, `INSERT INTO jobs (service_id, payload, key, cron, status, max_attempts, run_at, created_at, updated_at) VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?)
		ON CONFLICT (service_id, key) WHERE key != '' DO NOTHING RETURNING id`,
		j.ServiceID, j.Payload, j.Key, j.Cron, j.MaxAttempts, j.RunAt, t, t).Scan(&j.ID)
	if !errors.Is(err, sql.ErrNoRows) {
		return err == nil, err
	}
	old, err := scanJob(s.DB.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE service_id = ? AND key = ?`, j.ServiceID, j.Key))
	if err != nil {
		return false, err
	}
	*j = *old
	return false, nil
}

// CancelJob deletes a queued job by id, or by key when key is set.
func (s *Store) CancelJob(ctx context.Context, serviceID string, id int64, key string) (bool, error) {
	q, arg := `id = ?`, any(id)
	if key != "" {
		q, arg = `key = ?`, key
	}
	r, err := s.DB.ExecContext(ctx, `DELETE FROM jobs WHERE service_id = ? AND status = 'queued' AND `+q, serviceID, arg)
	if err != nil {
		return false, err
	}
	n, _ := r.RowsAffected()
	return n > 0, nil
}

// ClaimJob marks the oldest due job of a service as running and returns it.
func (s *Store) ClaimJob(ctx context.Context, serviceID string) (*Job, error) {
	t := now()
	return scanJob(s.DB.QueryRowContext(ctx, `UPDATE jobs SET status = 'running', attempts = attempts + 1, updated_at = ?
		WHERE id = (SELECT id FROM jobs WHERE service_id = ? AND status = 'queued' AND run_at <= ? ORDER BY run_at, id LIMIT 1)
		RETURNING `+jobCols, t, serviceID, t))
}

// DueServices lists the services with jobs ready to run.
func (s *Store) DueServices(ctx context.Context) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT DISTINCT service_id FROM jobs WHERE status = 'queued' AND run_at <= ?`, now())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) DeleteJob(ctx context.Context, serviceID string, id int64) (bool, error) {
	r, err := s.DB.ExecContext(ctx, `DELETE FROM jobs WHERE service_id = ? AND id = ?`, serviceID, id)
	if err != nil {
		return false, err
	}
	n, _ := r.RowsAffected()
	return n > 0, nil
}

// RetryJob queues a running job again at runAt, or marks it dead when it has
// no attempts left.
func (s *Store) RetryJob(ctx context.Context, id int64, errMsg string, runAt int64) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE jobs SET error = ?, updated_at = ?,
		status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
		run_at = CASE WHEN attempts >= max_attempts THEN run_at ELSE ? END
		WHERE id = ?`, errMsg, now(), runAt, id)
	return err
}

// RequeueDead queues a dead job again with a fresh set of attempts.
func (s *Store) RequeueDead(ctx context.Context, serviceID string, id int64) (bool, error) {
	r, err := s.DB.ExecContext(ctx, `UPDATE jobs SET status = 'queued', attempts = 0, run_at = ?, updated_at = ? WHERE service_id = ? AND id = ? AND status = 'dead'`, now(), now(), serviceID, id)
	if err != nil {
		return false, err
	}
	n, _ := r.RowsAffected()
	return n > 0, nil
}

// ResetRunning queues again the jobs that were running when the runner stopped.
func (s *Store) ResetRunning(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE jobs SET status = 'queued' WHERE status = 'running'`)
	return err
}

func (s *Store) Jobs(ctx context.Context, serviceID, status string, limit int) ([]*Job, error) {
	q := `SELECT ` + jobCols + ` FROM jobs WHERE service_id = ?`
	args := []any{serviceID}
	if status != "" {
		q += ` AND status = ?`
		args = append(args, status)
	}
	rows, err := s.DB.QueryContext(ctx, q+` ORDER BY id DESC LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

type JobCounts struct {
	Queued  int `json:"queued"`
	Running int `json:"running"`
	Dead    int `json:"dead"`
}

func (s *Store) JobCounts(ctx context.Context, serviceID string) (JobCounts, error) {
	var c JobCounts
	err := s.DB.QueryRowContext(ctx, `SELECT
		count(*) FILTER (WHERE status = 'queued'),
		count(*) FILTER (WHERE status = 'running'),
		count(*) FILTER (WHERE status = 'dead') FROM jobs WHERE service_id = ?`, serviceID).Scan(&c.Queued, &c.Running, &c.Dead)
	return c, err
}
