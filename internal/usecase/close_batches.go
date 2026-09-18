package usecase

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// CloseCompletedBatches 把所有任务都已收尾的批标记为 done。
//
// 为什么不在最后一个任务成功时顺手改：那需要在每个任务收尾时聚合一次全批状态，
// 对一个五万任务的批就是白白重复五万次聚合。改由后台周期性批量处理，
// 代价是批的完成状态有一个扫描周期（默认 30 秒）的延迟——
// 而任务本身的进度是实时的，查 progress 永远是准的。
type CloseCompletedBatches struct {
	store port.Store
	clock port.Clock
	log   *slog.Logger
	limit int
}

func NewCloseCompletedBatches(store port.Store, clock port.Clock, log *slog.Logger, limit int) *CloseCompletedBatches {
	if limit <= 0 {
		limit = 100
	}
	return &CloseCompletedBatches{store: store, clock: clock, log: log, limit: limit}
}

// Execute 扫描一轮，返回被关闭的批数。
func (uc *CloseCompletedBatches) Execute(ctx context.Context) (int, error) {
	now := uc.clock.Now()
	var closed int

	err := uc.store.WithTx(ctx, func(r port.Repos) error {
		batches, err := r.Batches().ListCompletable(ctx, uc.limit)
		if err != nil {
			return err
		}
		for _, b := range batches {
			progress, err := r.Batches().Progress(ctx, b.ID)
			if err != nil {
				return err
			}
			if err := b.Complete(now); err != nil {
				uc.log.Warn("skip closing batch", "batch_id", b.ID, "error", err)
				continue
			}
			if err := r.Batches().Update(ctx, b); err != nil {
				return err
			}
			if err := r.Events().Append(ctx, &scheduling.JobEvent{
				BatchID:   b.ID,
				Type:      scheduling.EventBatchCompleted,
				FromState: string(scheduling.BatchOpen),
				ToState:   string(scheduling.BatchDone),
				Actor:     "housekeeper",
				Detail: fmt.Sprintf("succeeded=%d dead=%d canceled=%d",
					progress.Succeeded, progress.Dead, progress.Canceled),
				At: now,
			}); err != nil {
				return err
			}
			closed++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if closed > 0 {
		uc.log.Info("closed completed batches", "count", closed)
	}
	return closed, nil
}
