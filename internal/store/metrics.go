package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// Metric is one service's traffic in one minute. Buckets counts requests by
// duration (see platform.DurationBuckets).
type Metric struct {
	ServiceID      string
	Minute         int64 // Unix minute
	Requests       int64
	Errors         int64 // 5xx responses
	Exceptions     int64
	Outbound       int64 // requests the service made
	OutboundErrors int64 // of those, blocked, failed or 5xx
	Buckets        []int64
}

// AddMetrics adds counts to the stored minutes.
func (s *Store) AddMetrics(ctx context.Context, ms []Metric) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, m := range ms {
		var raw string
		err := tx.QueryRowContext(ctx, `SELECT buckets FROM metrics WHERE service_id = ? AND minute = ?`, m.ServiceID, m.Minute).Scan(&raw)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var old []int64
		json.Unmarshal([]byte(raw), &old)
		buckets := make([]int64, max(len(old), len(m.Buckets)))
		for i := range buckets {
			if i < len(old) {
				buckets[i] += old[i]
			}
			if i < len(m.Buckets) {
				buckets[i] += m.Buckets[i]
			}
		}
		b, _ := json.Marshal(buckets)
		if _, err := tx.ExecContext(ctx, `INSERT INTO metrics (service_id, minute, requests, errors, exceptions, outbound, outbound_errors, buckets) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (service_id, minute) DO UPDATE SET requests = requests + excluded.requests, errors = errors + excluded.errors,
			exceptions = exceptions + excluded.exceptions, outbound = outbound + excluded.outbound,
			outbound_errors = outbound_errors + excluded.outbound_errors, buckets = excluded.buckets`,
			m.ServiceID, m.Minute, m.Requests, m.Errors, m.Exceptions, m.Outbound, m.OutboundErrors, string(b)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Metrics returns a service's minutes from the given one on, in order.
func (s *Store) Metrics(ctx context.Context, serviceID string, from int64) ([]Metric, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT minute, requests, errors, exceptions, outbound, outbound_errors, buckets FROM metrics WHERE service_id = ? AND minute >= ? ORDER BY minute`, serviceID, from)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Metric
	for rows.Next() {
		m := Metric{ServiceID: serviceID}
		var raw string
		if err := rows.Scan(&m.Minute, &m.Requests, &m.Errors, &m.Exceptions, &m.Outbound, &m.OutboundErrors, &raw); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(raw), &m.Buckets)
		out = append(out, m)
	}
	return out, rows.Err()
}

// Traffic is a service's counts over a span, without durations.
type Traffic struct {
	Requests       int64 `json:"requests"`
	Errors         int64 `json:"errors"`
	Exceptions     int64 `json:"exceptions"`
	Outbound       int64 `json:"outbound"`
	OutboundErrors int64 `json:"outbound_errors"`
}

// TrafficSince sums every service's traffic from a minute on, by service
// ID: one query for a list of services.
func (s *Store) TrafficSince(ctx context.Context, from int64) (map[string]Traffic, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT service_id, sum(requests), sum(errors), sum(exceptions), sum(outbound), sum(outbound_errors)
		FROM metrics WHERE minute >= ? GROUP BY service_id`, from)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]Traffic{}
	for rows.Next() {
		var id string
		var t Traffic
		if err := rows.Scan(&id, &t.Requests, &t.Errors, &t.Exceptions, &t.Outbound, &t.OutboundErrors); err != nil {
			return nil, err
		}
		out[id] = t
	}
	return out, rows.Err()
}

func (s *Store) PruneMetrics(ctx context.Context, before int64) error {
	_, err := s.DB.ExecContext(ctx, `DELETE FROM metrics WHERE minute < ?`, before)
	return err
}
