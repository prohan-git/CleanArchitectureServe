// Package port 定义应用层的出站端口（依赖倒置的接口）。
//
// 依赖方向：domain ← usecase ← adapter。
// usecase 只认识这里的接口，永远不认识 SQL、HTTP、Redis。
// 换存储 = 新写一个实现了这些接口的包，usecase 一行不改。
// 这就是"现在用 SQLite，将来换 PostgreSQL"能成立的全部技术前提。
package port

import (
	"context"
	"time"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
)

// Store 是调度事实源的抽象，也是事务边界的持有者。
type Store interface {
	// WithTx 在一个事务里执行 fn。fn 内的所有仓储操作要么全成功要么全回滚。
	// "创建批 + 批量入队 + 写审计事件"必须原子，否则会出现有批无任务的孤儿单。
	WithTx(ctx context.Context, fn func(Repos) error) error

	// Repos 返回非事务性的仓储集合，供只读查询使用。
	Repos() Repos

	Close() error
}

// Repos 聚合了一次事务内可用的全部仓储。
type Repos interface {
	Batches() BatchRepository
	Jobs() JobRepository
	Events() EventRepository
}

type BatchRepository interface {
	Create(ctx context.Context, b *scheduling.Batch) error
	Get(ctx context.Context, id string) (*scheduling.Batch, error)
	// FindByIdempotencyKey 命中已有批时返回它，未命中返回 (nil, nil)。
	FindByIdempotencyKey(ctx context.Context, key string) (*scheduling.Batch, error)
	Update(ctx context.Context, b *scheduling.Batch) error
	List(ctx context.Context, f BatchFilter) ([]*scheduling.Batch, error)
	Progress(ctx context.Context, batchID string) (scheduling.Progress, error)
}

// BatchFilter 支持"按触发方筛选"这类任务中心的核心查询。
type BatchFilter struct {
	TriggerKind    scheduling.TriggerKind // 空表示不过滤
	TriggerSubject string
	State          scheduling.BatchState
	Limit          int
	Offset         int
}

type JobRepository interface {
	// InsertMany 批量入队。遇到 IdempotencyKey 冲突的任务应当跳过而非报错，
	// 返回值是实际新插入的条数——这是幂等性在存储层的落点。
	InsertMany(ctx context.Context, jobs []*scheduling.Job) (inserted int, err error)

	Get(ctx context.Context, id string) (*scheduling.Job, error)
	Update(ctx context.Context, j *scheduling.Job) error

	// Claim 原子地领取一个任务：挑选 lane 上优先级最高且已到期可执行的 pending 任务，
	// 前提是该 lane 当前在途数量未达 capacity。
	//
	// 这个方法是整个调度器的正确性核心，必须由存储层用一条原子语句实现
	// （PostgreSQL: SELECT ... FOR UPDATE SKIP LOCKED；SQLite: BEGIN IMMEDIATE + 条件 UPDATE）。
	// 绝不能"先查再改"——那在多 worker 下必然重复执行同一个任务。
	// 无可领取任务时返回 (nil, nil)，这是正常情况而非错误。
	Claim(ctx context.Context, lane string, capacity int, owner string, leaseFor time.Duration, now time.Time) (*scheduling.Job, error)

	// ReclaimExpired 回收租约过期的任务（worker 崩溃留下的孤儿）。
	ReclaimExpired(ctx context.Context, now time.Time, limit int) ([]*scheduling.Job, error)

	// CancelByBatch 整单取消，返回受影响的任务数。
	CancelByBatch(ctx context.Context, batchID string, now time.Time) (int, error)

	ListByBatch(ctx context.Context, batchID string, limit, offset int) ([]*scheduling.Job, error)
}

type EventRepository interface {
	Append(ctx context.Context, e *scheduling.JobEvent) error
	ListByBatch(ctx context.Context, batchID string, limit, offset int) ([]*scheduling.JobEvent, error)
	ListByJob(ctx context.Context, jobID string) ([]*scheduling.JobEvent, error)
}
