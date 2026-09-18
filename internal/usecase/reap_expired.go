package usecase

import (
	"context"
	"log/slog"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// ReapExpired 回收租约过期的任务。
//
// 没有它，一次 kill -9 就会让任务永久卡在 running：状态表说它在跑，
// 但世界上已经没有任何进程在跑它，而调用方永远等不到结果。
// 任何以数据库为事实源的调度器都必须有这一环，这是 worker 崩溃后的唯一出路。
type ReapExpired struct {
	store port.Store
	clock port.Clock
	log   *slog.Logger
	limit int
}

func NewReapExpired(store port.Store, clock port.Clock, log *slog.Logger, limit int) *ReapExpired {
	if limit <= 0 {
		limit = 100
	}
	return &ReapExpired{store: store, clock: clock, log: log, limit: limit}
}

// Execute 扫描一轮过期租约，返回被回收的任务数。
func (uc *ReapExpired) Execute(ctx context.Context) (int, error) {
	now := uc.clock.Now()
	var reaped int

	err := uc.store.WithTx(ctx, func(r port.Repos) error {
		stale, err := r.Jobs().ReclaimExpired(ctx, now, uc.limit)
		if err != nil {
			return err
		}
		for _, job := range stale {
			retry, err := job.Reclaim("lease expired; worker presumed dead", scheduling.BackoffFor(job.Attempt), now)
			if err != nil {
				uc.log.Warn("skip reclaim", "job_id", job.ID, "error", err)
				continue
			}
			if err := r.Jobs().Update(ctx, job); err != nil {
				return err
			}
			evType := scheduling.EventJobDead
			if retry {
				evType = scheduling.EventJobReclaimed
			}
			if err := r.Events().Append(ctx, &scheduling.JobEvent{
				BatchID: job.BatchID, JobID: job.ID,
				Type:      evType,
				FromState: string(scheduling.JobRunning),
				ToState:   string(job.State),
				Actor:     "reaper",
				Detail:    "lease expired",
				At:        now,
			}); err != nil {
				return err
			}
			reaped++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if reaped > 0 {
		uc.log.Warn("reclaimed expired jobs", "count", reaped)
	}
	return reaped, nil
}
