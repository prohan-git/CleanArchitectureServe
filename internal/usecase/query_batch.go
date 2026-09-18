package usecase

import (
	"context"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// BatchView 是任务中心展示一张调度单所需的全部信息。
type BatchView struct {
	Batch    *scheduling.Batch
	Progress scheduling.Progress
}

// QueryBatch 提供任务中心的读侧用例。
type QueryBatch struct {
	store port.Store
}

func NewQueryBatch(store port.Store) *QueryBatch {
	return &QueryBatch{store: store}
}

// Get 返回单张调度单及其实时进度。
func (uc *QueryBatch) Get(ctx context.Context, batchID string) (BatchView, error) {
	r := uc.store.Repos()
	b, err := r.Batches().Get(ctx, batchID)
	if err != nil {
		return BatchView{}, err
	}
	p, err := r.Batches().Progress(ctx, batchID)
	if err != nil {
		return BatchView{}, err
	}
	return BatchView{Batch: b, Progress: p}, nil
}

// List 按触发方等条件列出调度单，供任务中心筛选。
func (uc *QueryBatch) List(ctx context.Context, f port.BatchFilter) ([]*scheduling.Batch, error) {
	return uc.store.Repos().Batches().List(ctx, f)
}

// Jobs 列出一张调度单下的任务明细。
func (uc *QueryBatch) Jobs(ctx context.Context, batchID string, limit, offset int) ([]*scheduling.Job, error) {
	return uc.store.Repos().Jobs().ListByBatch(ctx, batchID, limit, offset)
}

// Events 返回一张调度单的审计轨迹，即"留底"。
func (uc *QueryBatch) Events(ctx context.Context, batchID string, limit, offset int) ([]*scheduling.JobEvent, error) {
	return uc.store.Repos().Events().ListByBatch(ctx, batchID, limit, offset)
}
