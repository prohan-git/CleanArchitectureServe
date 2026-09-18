package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
)

type eventRepo struct{ q querier }

const eventColumns = `id, batch_id, job_id, type, from_state, to_state, actor, detail, at`

// Append 写入一条审计记录。这张表只追加、不更新、不删除。
func (r *eventRepo) Append(ctx context.Context, e *scheduling.JobEvent) error {
	const q = `INSERT INTO job_events (batch_id, job_id, type, from_state, to_state, actor, detail, at)
		VALUES (?,?,?,?,?,?,?,?)`
	_, err := r.q.ExecContext(ctx, q,
		e.BatchID, e.JobID, string(e.Type), e.FromState, e.ToState, e.Actor, e.Detail, toMillis(e.At))
	if err != nil {
		return fmt.Errorf("append event %s: %w", e.Type, err)
	}
	return nil
}

func (r *eventRepo) ListByBatch(ctx context.Context, batchID string, limit, offset int) ([]*scheduling.JobEvent, error) {
	const q = `SELECT ` + eventColumns + ` FROM job_events WHERE batch_id = ?
		ORDER BY id ASC LIMIT ? OFFSET ?`
	rows, err := r.q.QueryContext(ctx, q, batchID, normalizeLimit(limit), offset)
	if err != nil {
		return nil, fmt.Errorf("list events of batch %s: %w", batchID, err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func (r *eventRepo) ListByJob(ctx context.Context, jobID string) ([]*scheduling.JobEvent, error) {
	const q = `SELECT ` + eventColumns + ` FROM job_events WHERE job_id = ? ORDER BY id ASC`
	rows, err := r.q.QueryContext(ctx, q, jobID)
	if err != nil {
		return nil, fmt.Errorf("list events of job %s: %w", jobID, err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func scanEvents(rows *sql.Rows) ([]*scheduling.JobEvent, error) {
	var out []*scheduling.JobEvent
	for rows.Next() {
		var (
			e    scheduling.JobEvent
			typ  string
			atMS int64
		)
		if err := rows.Scan(&e.ID, &e.BatchID, &e.JobID, &typ, &e.FromState, &e.ToState, &e.Actor, &e.Detail, &atMS); err != nil {
			return nil, err
		}
		e.Type = scheduling.EventType(typ)
		e.At = fromMillis(atMS)
		out = append(out, &e)
	}
	return out, rows.Err()
}
