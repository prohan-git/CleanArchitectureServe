package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/prohan-git/CleanArchitectureServe/internal/usecase"
)

// Housekeeper 周期性执行两件收尾工作：
//
//  1. 回收租约过期的任务（worker 崩溃留下的孤儿）
//  2. 把所有任务都已收尾的批标记为 done
//
// 两件事都是"周期性对账"性质的：不做也不会丢数据，但不做的话
// 崩溃的任务会永久卡住、跑完的批会永远显示在进行中。
//
// 单机部署时和 worker 同进程即可；多实例时每个实例都跑也无妨——
// 两个操作都在事务里执行，重复扫描不会重复生效。
type Housekeeper struct {
	reap     *usecase.ReapExpired
	close    *usecase.CloseCompletedBatches
	interval time.Duration
	log      *slog.Logger
}

func NewHousekeeper(reap *usecase.ReapExpired, close *usecase.CloseCompletedBatches, interval time.Duration, log *slog.Logger) *Housekeeper {
	return &Housekeeper{reap: reap, close: close, interval: interval, log: log}
}

func (h *Housekeeper) Run(ctx context.Context) {
	t := time.NewTicker(h.interval)
	defer t.Stop()
	h.log.Info("housekeeper started", "interval", h.interval)
	defer h.log.Info("housekeeper stopped")

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := h.reap.Execute(ctx); err != nil && ctx.Err() == nil {
				h.log.Error("reap expired jobs failed", "error", err)
			}
			if _, err := h.close.Execute(ctx); err != nil && ctx.Err() == nil {
				h.log.Error("close completed batches failed", "error", err)
			}
		}
	}
}
