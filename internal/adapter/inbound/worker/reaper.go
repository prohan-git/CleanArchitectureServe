package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/prohan-git/CleanArchitectureServe/internal/usecase"
)

// Reaper 周期性回收租约过期的任务。
//
// 单机部署时它和 worker 同进程即可；多实例时每个实例都跑也无妨——
// ReclaimExpired 在事务里执行，重复扫描不会重复回收。
type Reaper struct {
	uc       *usecase.ReapExpired
	interval time.Duration
	log      *slog.Logger
}

func NewReaper(uc *usecase.ReapExpired, interval time.Duration, log *slog.Logger) *Reaper {
	return &Reaper{uc: uc, interval: interval, log: log}
}

func (r *Reaper) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	r.log.Info("reaper started", "interval", r.interval)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("reaper stopped")
			return
		case <-t.C:
			if _, err := r.uc.Execute(ctx); err != nil && ctx.Err() == nil {
				r.log.Error("reap failed", "error", err)
			}
		}
	}
}
