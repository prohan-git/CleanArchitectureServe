package usecase

import (
	"context"
	"fmt"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// CancelBatch 整单取消。
//
// 已在 running 的任务同样置为 canceled：那次执行提交结果时会因租约校验失败被拒，
// 不会把状态改回 succeeded。这样"取消"对调用方是立即生效的，
// 不必等待正在跑的 GPU 任务结束。
type CancelBatch struct {
	store port.Store
	clock port.Clock
}

func NewCancelBatch(store port.Store, clock port.Clock) *CancelBatch {
	return &CancelBatch{store: store, clock: clock}
}

func (uc *CancelBatch) Execute(ctx context.Context, batchID, actor string) (canceled int, err error) {
	now := uc.clock.Now()
	err = uc.store.WithTx(ctx, func(r port.Repos) error {
		b, err := r.Batches().Get(ctx, batchID)
		if err != nil {
			return err
		}
		if err := b.Cancel(now); err != nil {
			return err
		}
		if err := r.Batches().Update(ctx, b); err != nil {
			return err
		}
		n, err := r.Jobs().CancelByBatch(ctx, batchID, now)
		if err != nil {
			return err
		}
		canceled = n
		return r.Events().Append(ctx, &scheduling.JobEvent{
			BatchID: batchID,
			Type:    scheduling.EventBatchCanceled,
			ToState: string(scheduling.BatchCanceled),
			Actor:   actor,
			Detail:  fmt.Sprintf("canceled %d job(s)", n),
			At:      now,
		})
	})
	return canceled, err
}
