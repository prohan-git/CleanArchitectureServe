// Package sqlstore 用标准 database/sql 实现调度事实源。
//
// 这里刻意只写通用 SQL：时间为整数毫秒，无方言特有类型。
// 唯一的方言差异集中在 job_repo.go 的 Claim 方法上
// （SQLite 用 BEGIN IMMEDIATE + 条件 UPDATE，PostgreSQL 用 FOR UPDATE SKIP LOCKED），
// 换库时要动的就只有那一处。
package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// querier 抹平 *sql.DB 与 *sql.Tx 的差异，让仓储代码事务内外通用。
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Store 是 port.Store 的 SQL 实现。
type Store struct {
	db *sql.DB
}

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) Close() error { return s.db.Close() }

// DB 暴露底层连接，仅供迁移与健康检查使用。
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Repos() port.Repos { return &repos{q: s.db} }

func (s *Store) WithTx(ctx context.Context, fn func(port.Repos) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	// 用 defer + recover 保证 fn panic 时连接不会泄漏。
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := fn(&repos{q: tx}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	committed = true
	return nil
}

type repos struct{ q querier }

func (r *repos) Batches() port.BatchRepository { return &batchRepo{q: r.q} }
func (r *repos) Jobs() port.JobRepository      { return &jobRepo{q: r.q} }
func (r *repos) Events() port.EventRepository  { return &eventRepo{q: r.q} }
