package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

type batchRepo struct{ q querier }

const batchColumns = `id, title, trigger_kind, trigger_subject, scope_kind, scope_ref, state, total, idempotency_key, created_at, updated_at`

func (r *batchRepo) Create(ctx context.Context, b *scheduling.Batch) error {
	const q = `INSERT INTO batches (` + batchColumns + `) VALUES (?,?,?,?,?,?,?,?,?,?,?)`
	_, err := r.q.ExecContext(ctx, q,
		b.ID, b.Title, string(b.Trigger.Kind), b.Trigger.Subject,
		b.Scope.Kind, b.Scope.Ref, string(b.State), b.Total, b.IdempotencyKey,
		toMillis(b.CreatedAt), toMillis(b.UpdatedAt))
	if err != nil {
		return fmt.Errorf("insert batch %s: %w", b.ID, err)
	}
	return nil
}

func (r *batchRepo) Get(ctx context.Context, id string) (*scheduling.Batch, error) {
	row := r.q.QueryRowContext(ctx, `SELECT `+batchColumns+` FROM batches WHERE id = ?`, id)
	b, err := scanBatch(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: batch %s", scheduling.ErrNotFound, id)
	}
	return b, err
}

// FindByIdempotencyKey 返回同 key 的已有批；没有则返回 (nil, nil)。
// 找不到不是错误——绝大多数提交都是新的。
func (r *batchRepo) FindByIdempotencyKey(ctx context.Context, key string) (*scheduling.Batch, error) {
	if key == "" {
		return nil, nil
	}
	row := r.q.QueryRowContext(ctx, `SELECT `+batchColumns+` FROM batches WHERE idempotency_key = ?`, key)
	b, err := scanBatch(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return b, err
}

func (r *batchRepo) Update(ctx context.Context, b *scheduling.Batch) error {
	const q = `UPDATE batches SET title=?, state=?, total=?, updated_at=? WHERE id=?`
	_, err := r.q.ExecContext(ctx, q, b.Title, string(b.State), b.Total, toMillis(b.UpdatedAt), b.ID)
	if err != nil {
		return fmt.Errorf("update batch %s: %w", b.ID, err)
	}
	return nil
}

func (r *batchRepo) List(ctx context.Context, f port.BatchFilter) ([]*scheduling.Batch, error) {
	var (
		where []string
		args  []any
	)
	if f.TriggerKind != "" {
		where = append(where, "trigger_kind = ?")
		args = append(args, string(f.TriggerKind))
	}
	if f.TriggerSubject != "" {
		where = append(where, "trigger_subject = ?")
		args = append(args, f.TriggerSubject)
	}
	if f.State != "" {
		where = append(where, "state = ?")
		args = append(args, string(f.State))
	}

	q := `SELECT ` + batchColumns + ` FROM batches`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"
	args = append(args, normalizeLimit(f.Limit), f.Offset)

	rows, err := r.q.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list batches: %w", err)
	}
	defer rows.Close()

	var out []*scheduling.Batch
	for rows.Next() {
		b, err := scanBatch(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// Progress 用一次 group by 聚合出批进度。
// 进度是派生数据，不落库：落库就要在每次任务状态变更时维护计数器，
// 那是一致性负担，而这个查询在 idx_jobs_batch 上是索引扫描，代价可忽略。
func (r *batchRepo) Progress(ctx context.Context, batchID string) (scheduling.Progress, error) {
	const q = `SELECT state, COUNT(*) FROM jobs WHERE batch_id = ? GROUP BY state`
	rows, err := r.q.QueryContext(ctx, q, batchID)
	if err != nil {
		return scheduling.Progress{}, fmt.Errorf("aggregate progress of batch %s: %w", batchID, err)
	}
	defer rows.Close()

	var p scheduling.Progress
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return scheduling.Progress{}, err
		}
		p.Total += n
		switch scheduling.JobState(state) {
		case scheduling.JobPending:
			p.Pending = n
		case scheduling.JobRunning:
			p.Running = n
		case scheduling.JobSucceeded:
			p.Succeeded = n
		case scheduling.JobDead:
			p.Dead = n
		case scheduling.JobCanceled:
			p.Canceled = n
		}
	}
	return p, rows.Err()
}

func scanBatch(s rowScanner) (*scheduling.Batch, error) {
	var (
		b                    scheduling.Batch
		triggerKind, state   string
		scopeKind, scopeRef  string
		triggerSubject       string
		createdMS, updatedMS int64
	)
	if err := s.Scan(&b.ID, &b.Title, &triggerKind, &triggerSubject, &scopeKind, &scopeRef,
		&state, &b.Total, &b.IdempotencyKey, &createdMS, &updatedMS); err != nil {
		return nil, err
	}
	b.Trigger = scheduling.Trigger{Kind: scheduling.TriggerKind(triggerKind), Subject: triggerSubject}
	b.Scope = scheduling.Scope{Kind: scopeKind, Ref: scopeRef}
	b.State = scheduling.BatchState(state)
	b.CreatedAt = fromMillis(createdMS)
	b.UpdatedAt = fromMillis(updatedMS)
	return &b, nil
}
