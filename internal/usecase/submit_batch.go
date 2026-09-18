// Package usecase 实现应用层用例：编排领域对象与出站端口，不含任何传输或存储细节。
package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// SubmitBatchInput 是提交一张调度单的入参。
//
// 四种触发方共用这一个入口：终端用户传 {Kind:user, Scope:self}，
// 管理员传 {Kind:admin, Scope:class/library}，事件与定时任务同理。
// 没有"用户接口"和"管理员接口"两套代码。
type SubmitBatchInput struct {
	Title      string
	Trigger    scheduling.Trigger
	Scope      scheduling.Scope
	JobKind    string // 对每个目标执行的动作，如 "ai.analyze"
	Lane       string // 在哪条资源车道上执行
	Priority   int
	MaxRetries int
	NotBefore  time.Time
	// IdempotencyKey 是整单的幂等键，单个任务的幂等键由它与目标引用派生。
	IdempotencyKey string
}

type SubmitBatchOutput struct {
	BatchID  string
	Total    int
	Enqueued int // 实际新入队数；与 Total 的差额是被幂等键挡掉的重复任务
	// Duplicate 为 true 表示这次提交命中了已有的批，没有创建任何新东西。
	// 调用方据此可以区分"刚派发成功"和"这批早就派过了"。
	Duplicate bool
}

// SubmitBatch 创建调度单：展开范围 → 生成任务 → 连同审计事件一次性落库。
type SubmitBatch struct {
	store    port.Store
	expander port.TargetExpander
	topology *scheduling.Topology
	clock    port.Clock
	ids      port.IDGenerator
}

func NewSubmitBatch(store port.Store, expander port.TargetExpander, topo *scheduling.Topology, clock port.Clock, ids port.IDGenerator) *SubmitBatch {
	return &SubmitBatch{store: store, expander: expander, topology: topo, clock: clock, ids: ids}
}

func (uc *SubmitBatch) Execute(ctx context.Context, in SubmitBatchInput) (SubmitBatchOutput, error) {
	if in.JobKind == "" {
		return SubmitBatchOutput{}, fmt.Errorf("%w: job kind is empty", scheduling.ErrInvalidArgument)
	}
	lane, ok := uc.topology.Lookup(in.Lane)
	if !ok {
		return SubmitBatchOutput{}, fmt.Errorf("%w: unknown lane %q", scheduling.ErrInvalidArgument, in.Lane)
	}

	// 幂等前置检查：命中已有批就直接返回它，不再展开范围、不再建新批。
	// 没有这一步，重复提交虽然不会产生重复任务，却会留下一张 total>0 而任务数为 0
	// 的孤儿批——进度查询会显示 0/3，对用户就是"卡住了"。
	if in.IdempotencyKey != "" {
		existing, err := uc.store.Repos().Batches().FindByIdempotencyKey(ctx, in.IdempotencyKey)
		if err != nil {
			return SubmitBatchOutput{}, err
		}
		if existing != nil {
			return SubmitBatchOutput{BatchID: existing.ID, Total: existing.Total, Enqueued: 0, Duplicate: true}, nil
		}
	}

	now := uc.clock.Now()
	batch, err := scheduling.NewBatch(uc.ids.NewID(), in.Title, in.Trigger, in.Scope, now)
	if err != nil {
		return SubmitBatchOutput{}, err
	}
	batch.IdempotencyKey = in.IdempotencyKey

	// 范围展开发生在事务之外：它可能要读业务库、可能很慢，
	// 不该占着调度库的写事务。
	targets, err := uc.expander.Expand(ctx, in.Scope, in.Trigger)
	if err != nil {
		return SubmitBatchOutput{}, fmt.Errorf("expand scope: %w", err)
	}
	if len(targets) == 0 {
		return SubmitBatchOutput{}, fmt.Errorf("%w: scope %s/%s expanded to zero targets",
			scheduling.ErrInvalidArgument, in.Scope.Kind, in.Scope.Ref)
	}
	batch.Total = len(targets)

	jobs := make([]*scheduling.Job, 0, len(targets))
	for _, t := range targets {
		opts := scheduling.JobOptions{
			Priority:       in.Priority,
			MaxAttempts:    in.MaxRetries,
			NotBefore:      in.NotBefore,
			IdempotencyKey: deriveIdempotencyKey(in.IdempotencyKey, in.JobKind, t.Ref),
		}
		job, err := scheduling.NewJob(uc.ids.NewID(), batch.ID, lane.Name, in.JobKind, t.Ref, t.Payload, opts, now)
		if err != nil {
			return SubmitBatchOutput{}, err
		}
		jobs = append(jobs, job)
	}

	var enqueued int
	err = uc.store.WithTx(ctx, func(r port.Repos) error {
		if err := r.Batches().Create(ctx, batch); err != nil {
			return err
		}
		n, err := r.Jobs().InsertMany(ctx, jobs)
		if err != nil {
			return err
		}
		enqueued = n
		return r.Events().Append(ctx, &scheduling.JobEvent{
			BatchID: batch.ID,
			Type:    scheduling.EventBatchCreated,
			ToState: string(scheduling.BatchOpen),
			Actor:   in.Trigger.Subject,
			Detail: fmt.Sprintf("kind=%s lane=%s targets=%d enqueued=%d trigger=%s",
				in.JobKind, lane.Name, len(targets), n, in.Trigger.Kind),
			At: now,
		})
	})
	if err != nil {
		return SubmitBatchOutput{}, err
	}

	return SubmitBatchOutput{BatchID: batch.ID, Total: len(targets), Enqueued: enqueued}, nil
}

// deriveIdempotencyKey 由整单幂等键派生出单任务幂等键。
// 调用方没给整单键时返回空串，表示这次提交不做去重。
func deriveIdempotencyKey(batchKey, jobKind, targetRef string) string {
	if batchKey == "" {
		return ""
	}
	return batchKey + ":" + jobKind + ":" + targetRef
}
