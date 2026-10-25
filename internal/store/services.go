package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type Service struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	CreatedAt        int64    `json:"created_at"`
	ActiveDeployment string   `json:"active_deployment,omitempty"`
	Stopped          bool     `json:"stopped"`
	Egress           []string `json:"egress"`  // hosts the service may reach; "*" is any
	DiskMB           int      `json:"disk_mb"` // its disk quota; 0: the instance's default
}

type Deployment struct {
	ID        string `json:"id"`
	ServiceID string `json:"-"`
	Number    int    `json:"number"`
	Status    string `json:"status"` // ready or failed
	Error     string `json:"error,omitempty"`
	Manifest  string `json:"-"`
	Bundle    string `json:"-"` // SHA-256 of the built module
	CreatedAt int64  `json:"created_at"`
}

const serviceCols = `id, name, created_at, coalesce(active_deployment, ''), stopped, egress, disk_mb`

func scanService(row interface{ Scan(...any) error }) (*Service, error) {
	var sv Service
	var egress string
	err := row.Scan(&sv.ID, &sv.Name, &sv.CreatedAt, &sv.ActiveDeployment, &sv.Stopped, &egress, &sv.DiskMB)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	json.Unmarshal([]byte(egress), &sv.Egress)
	if sv.Egress == nil {
		sv.Egress = []string{}
	}
	return &sv, err
}

// SetServiceLimits sets what a service may reach and store.
func (s *Store) SetServiceLimits(ctx context.Context, id string, egress []string, diskMB int) error {
	b, _ := json.Marshal(egress)
	_, err := s.DB.ExecContext(ctx, `UPDATE services SET egress = ?, disk_mb = ? WHERE id = ?`, string(b), diskMB, id)
	return err
}

// RenameService gives a service a new name.
func (s *Store) RenameService(ctx context.Context, id, name string) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE services SET name = ? WHERE id = ?`, name, id)
	return err
}

func (s *Store) CreateService(ctx context.Context, name string) (*Service, error) {
	a := &Service{ID: NewID(), Name: name, CreatedAt: now(), Egress: []string{"*"}}
	_, err := s.DB.ExecContext(ctx, `INSERT INTO services (id, name, created_at) VALUES (?, ?, ?)`, a.ID, a.Name, a.CreatedAt)
	return a, err
}

func (s *Store) Service(ctx context.Context, name string) (*Service, error) {
	return scanService(s.DB.QueryRowContext(ctx, `SELECT `+serviceCols+` FROM services WHERE name = ?`, name))
}

func (s *Store) ServiceByID(ctx context.Context, id string) (*Service, error) {
	return scanService(s.DB.QueryRowContext(ctx, `SELECT `+serviceCols+` FROM services WHERE id = ?`, id))
}

func (s *Store) Services(ctx context.Context) ([]*Service, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT `+serviceCols+` FROM services ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Service{}
	for rows.Next() {
		a, err := scanService(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) DeleteService(ctx context.Context, id string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, table := range []string{"logs", "metrics"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE service_id = ?`, id); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM services WHERE id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) SetStopped(ctx context.Context, id string, stopped bool) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE services SET stopped = ? WHERE id = ?`, stopped, id)
	return err
}

// AddDeployment records a deployment, numbered with NextDeployment, and, if
// it is ready, makes it the active one.
func (s *Store) AddDeployment(ctx context.Context, d *Deployment) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	d.CreatedAt = now()
	if _, err := tx.ExecContext(ctx, `INSERT INTO deployments (id, service_id, number, status, error, manifest, bundle, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		d.ID, d.ServiceID, d.Number, d.Status, d.Error, d.Manifest, d.Bundle, d.CreatedAt); err != nil {
		return err
	}
	if d.Status == "ready" {
		if _, err := tx.ExecContext(ctx, `UPDATE services SET active_deployment = ? WHERE id = ?`, d.ID, d.ServiceID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Activate(ctx context.Context, serviceID, deploymentID string) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE services SET active_deployment = ? WHERE id = ?`, deploymentID, serviceID)
	return err
}

const deploymentCols = `id, service_id, number, status, error, manifest, bundle, created_at`

func scanDeployment(row interface{ Scan(...any) error }) (*Deployment, error) {
	var d Deployment
	err := row.Scan(&d.ID, &d.ServiceID, &d.Number, &d.Status, &d.Error, &d.Manifest, &d.Bundle, &d.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &d, err
}

func (s *Store) Deployment(ctx context.Context, serviceID, id string) (*Deployment, error) {
	return scanDeployment(s.DB.QueryRowContext(ctx, `SELECT `+deploymentCols+` FROM deployments WHERE service_id = ? AND id = ?`, serviceID, id))
}

func (s *Store) Deployments(ctx context.Context, serviceID string, limit int) ([]*Deployment, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT `+deploymentCols+` FROM deployments WHERE service_id = ? ORDER BY number DESC LIMIT ?`, serviceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*Deployment{}
	for rows.Next() {
		d, err := scanDeployment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// PruneDeployments deletes all but the newest keep deployments of a service,
// never the active one, and returns the bundles no deployment references
// any more.
func (s *Store) PruneDeployments(ctx context.Context, serviceID string, keep int) ([]string, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id, bundle FROM deployments WHERE service_id = ?
		AND id != coalesce((SELECT active_deployment FROM services WHERE id = ?), '')
		ORDER BY number DESC LIMIT -1 OFFSET ?`, serviceID, serviceID, keep)
	if err != nil {
		return nil, err
	}
	var ids, bundles []string
	for rows.Next() {
		var id, b string
		if err := rows.Scan(&id, &b); err != nil {
			rows.Close()
			return nil, err
		}
		ids, bundles = append(ids, id), append(bundles, b)
	}
	rows.Close()
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `DELETE FROM deployments WHERE id = ?`, id); err != nil {
			return nil, err
		}
	}
	var unused []string
	for _, b := range bundles {
		if b == "" {
			continue
		}
		var n int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM deployments WHERE bundle = ?`, b).Scan(&n); err != nil {
			return nil, err
		}
		if n == 0 {
			unused = append(unused, b)
		}
	}
	return unused, tx.Commit()
}

// BundleUsed reports whether any deployment references a bundle.
func (s *Store) BundleUsed(ctx context.Context, sha string) (bool, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `SELECT count(*) FROM deployments WHERE bundle = ?`, sha).Scan(&n)
	return n > 0, err
}

// NextDeployment is the number of the service's next deployment.
func (s *Store) NextDeployment(ctx context.Context, serviceID string) (int, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `SELECT coalesce(max(number), 0) + 1 FROM deployments WHERE service_id = ?`, serviceID).Scan(&n)
	return n, err
}
