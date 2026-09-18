package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
)

type jobRepo struct{ q querier }

const jobColumns = `id, batch_id, lane, kind, target_ref, payload, idempotency_key, state,
	priority, attempt, max_attempts, available_at, lease_owner, lease_expires_at,
	last_error, created_at, updated_at`

func (r *jobRepo) InsertMany(ctx context.Context, jobs []*scheduling.Job) (int, error) {
	// ON CONFLICT DO NOTHING 让重复提交静默跳过，而不是整批失败。
	// conflict target 必须重复索引的 WHERE 子句，否则匹配不上部分唯一索引
	// （SQLite 与 PostgreSQL 在这一点上规则一致）。
	// 幂等在这里落地：定时任务重复触发、用户连点两次，第二次不会产生重复执行。
	const q = `INSERT INTO jobs (` + jobColumns + `)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT (idempotency_key) WHERE idempotency_key <> '' DO NOTHING`

	inserted := 0
	for _, j := range jobs {
		res, err := r.q.ExecContext(ctx, q,
			j.ID, j.BatchID, j.Lane, j.Kind, j.TargetRef, string(j.Payload), j.IdempotencyKey, string(j.State),
			j.Priority, j.Attempt, j.MaxAttempts, toMillis(j.AvailableAt), j.LeaseOwner, toMillis(j.LeaseExpires),
			j.LastError, toMillis(j.CreatedAt), toMillis(j.UpdatedAt))
		if err != nil {
			return inserted, fmt.Errorf("insert job %s: %w", j.ID, err)
		}
		if n, err := res.RowsAffected(); err == nil {
			inserted += int(n)
		}
	}
	return inserted, nil
}

func (r *jobRepo) Get(ctx context.Context, id string) (*scheduling.Job, error) {
	row := r.q.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = ?`, id)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: job %s", scheduling.ErrNotFound, id)
	}
	return j, err
}

func (r *jobRepo) Update(ctx context.Context, j *scheduling.Job) error {
	const q = `UPDATE jobs SET
		state=?, priority=?, attempt=?, max_attempts=?, available_at=?,
		lease_owner=?, lease_expires_at=?, last_error=?, updated_at=?
		WHERE id = ?`
	_, err := r.q.ExecContext(ctx, q,
		string(j.State), j.Priority, j.Attempt, j.MaxAttempts, toMillis(j.AvailableAt),
		j.LeaseOwner, toMillis(j.LeaseExpires), j.LastError, toMillis(j.UpdatedAt), j.ID)
	if err != nil {
		return fmt.Errorf("update job %s: %w", j.ID, err)
	}
	return nil
}

// Claim 原子地领取一个任务，是整个调度器正确性的核心。
//
// 三件事在一条语句里完成，不存在"先查再改"的窗口：
//  1. 挑出该车道上优先级最高、已到可执行时间的 pending 任务；
//  2. 校验该车道在途数量未达 capacity——这是 CT 串行、GPU 独占的全局保证，
//     由数据库而非"记得给 worker 加 --concurrency=1"来兑现；
//  3. 打上租约并把 attempt 加一。
//
// 在途计数只数租约未过期的 running 任务。否则一个被 kill -9 的 worker
// 留下的僵尸任务会把串行车道永久堵死，直到 reaper 跑完为止。
//
// 换 PostgreSQL 时这里改成：
//
//	UPDATE jobs SET ... WHERE id = (SELECT id FROM jobs WHERE ... FOR UPDATE SKIP LOCKED LIMIT 1)
//
// 语义完全一致，只是把 SQLite 的"整库写锁串行化"换成行级跳锁。
func (r *jobRepo) Claim(ctx context.Context, lane string, capacity int, owner string, leaseFor time.Duration, now time.Time) (*scheduling.Job, error) {
	nowMS := toMillis(now)
	leaseUntil := toMillis(now.Add(leaseFor))

	const q = `UPDATE jobs SET
			state       = 'running',
			attempt     = attempt + 1,
			lease_owner = ?,
			lease_expires_at = ?,
			updated_at  = ?
		WHERE id = (
			SELECT id FROM jobs
			WHERE lane = ? AND state = 'pending' AND available_at <= ?
			ORDER BY priority DESC, available_at ASC, created_at ASC
			LIMIT 1
		)
		AND (
			SELECT COUNT(*) FROM jobs busy
			WHERE busy.lane = ? AND busy.state = 'running' AND busy.lease_expires_at > ?
		) < ?
		RETURNING ` + jobColumns

	row := r.q.QueryRowContext(ctx, q, owner, leaseUntil, nowMS, lane, nowMS, lane, nowMS, capacity)
	j, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		// 车道空闲无任务，或车道已满。都是正常情况，不是错误。
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim job on lane %s: %w", lane, err)
	}
	return j, nil
}

// ReclaimExpired 取出租约已过期的 running 任务，交由用例层按重试规则处理。
func (r *jobRepo) ReclaimExpired(ctx context.Context, now time.Time, limit int) ([]*scheduling.Job, error) {
	const q = `SELECT ` + jobColumns + ` FROM jobs
		WHERE state = 'running' AND lease_expires_at <= ?
		ORDER BY lease_expires_at ASC LIMIT ?`
	rows, err := r.q.QueryContext(ctx, q, toMillis(now), limit)
	if err != nil {
		return nil, fmt.Errorf("select expired jobs: %w", err)
	}
	defer rows.Close()
	return scanJobs(rows)
}

func (r *jobRepo) CancelByBatch(ctx context.Context, batchID string, now time.Time) (int, error) {
	// 只取消未进终态的任务；已成功/已死信的保留原状，取消不能抹掉已发生的事实。
	const q = `UPDATE jobs SET state='canceled', lease_owner='', lease_expires_at=0, updated_at=?
		WHERE batch_id = ? AND state IN ('pending','running')`
	res, err := r.q.ExecContext(ctx, q, toMillis(now), batchID)
	if err != nil {
		return 0, fmt.Errorf("cancel jobs of batch %s: %w", batchID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return int(n), nil
}

func (r *jobRepo) ListByBatch(ctx context.Context, batchID string, limit, offset int) ([]*scheduling.Job, error) {
	const q = `SELECT ` + jobColumns + ` FROM jobs WHERE batch_id = ?
		ORDER BY created_at ASC, id ASC LIMIT ? OFFSET ?`
	rows, err := r.q.QueryContext(ctx, q, batchID, normalizeLimit(limit), offset)
	if err != nil {
		return nil, fmt.Errorf("list jobs of batch %s: %w", batchID, err)
	}
	defer rows.Close()
	return scanJobs(rows)
}

type rowScanner interface{ Scan(dest ...any) error }

func scanJob(s rowScanner) (*scheduling.Job, error) {
	var (
		j                                   scheduling.Job
		payload, state                      string
		availableAt, leaseExp, created, upd int64
	)
	err := s.Scan(&j.ID, &j.BatchID, &j.Lane, &j.Kind, &j.TargetRef, &payload, &j.IdempotencyKey, &state,
		&j.Priority, &j.Attempt, &j.MaxAttempts, &availableAt, &j.LeaseOwner, &leaseExp,
		&j.LastError, &created, &upd)
	if err != nil {
		return nil, err
	}
	if payload != "" {
		j.Payload = []byte(payload)
	}
	j.State = scheduling.JobState(state)
	j.AvailableAt = fromMillis(availableAt)
	j.LeaseExpires = fromMillis(leaseExp)
	j.CreatedAt = fromMillis(created)
	j.UpdatedAt = fromMillis(upd)
	return &j, nil
}

func scanJobs(rows *sql.Rows) ([]*scheduling.Job, error) {
	var out []*scheduling.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func normalizeLimit(limit int) int {
	switch {
	case limit <= 0:
		return 50
	case limit > 500:
		return 500
	default:
		return limit
	}
}
