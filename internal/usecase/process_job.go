package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// ProcessJob 是 worker 的单次工作循环：领一个任务、执行、提交结果。
//
// 这就是"不需要 Celery"的全部内容。可靠投递由 Claim 的原子性保证，
// 重试由领域状态机保证，崩溃恢复由租约 + Reaper 保证。
// 引入 Redis 能额外买到的只有"立即唤醒"，而那用轮询间隔就能换。
type ProcessJob struct {
	store    port.Store
	registry port.ExecutorRegistry
	clock    port.Clock
	log      *slog.Logger

	owner    string        // 本 worker 实例的标识，写进租约
	leaseFor time.Duration // 租约时长，应显著大于任务的正常执行时间
}

func NewProcessJob(store port.Store, registry port.ExecutorRegistry, clock port.Clock, log *slog.Logger, owner string, leaseFor time.Duration) *ProcessJob {
	return &ProcessJob{store: store, registry: registry, clock: clock, log: log, owner: owner, leaseFor: leaseFor}
}

// Execute 尝试在 lane 上处理一个任务。
// 返回 false 表示当前无活可干（车道空或已满），调用方应退避后重试。
func (uc *ProcessJob) Execute(ctx context.Context, lane scheduling.Lane) (worked bool, err error) {
	now := uc.clock.Now()

	job, err := uc.claim(ctx, lane, now)
	if err != nil || job == nil {
		return false, err
	}

	log := uc.log.With("job_id", job.ID, "batch_id", job.BatchID, "lane", lane.Name,
		"kind", job.Kind, "attempt", job.Attempt)

	exec, ok := uc.registry.Lookup(job.Kind)
	if !ok {
		// 配置错误不该消耗重试次数式地反复跑，直接判定失败交由重试规则处理。
		uc.settleFailure(ctx, job, fmt.Sprintf("no executor registered for kind %q", job.Kind), log)
		return true, nil
	}

	// 执行超时挂在租约上：任务最多跑到租约到期，之后 Reaper 会接手回收。
	runCtx, cancel := context.WithDeadline(ctx, job.LeaseExpires)
	defer cancel()

	res, execErr := exec.Execute(runCtx, port.Task{
		JobID:     job.ID,
		BatchID:   job.BatchID,
		Kind:      job.Kind,
		TargetRef: job.TargetRef,
		Payload:   job.Payload,
		Attempt:   job.Attempt,
	})
	if execErr != nil {
		uc.settleFailure(ctx, job, execErr.Error(), log)
		return true, nil
	}

	uc.settleSuccess(ctx, job, res, log)
	return true, nil
}

func (uc *ProcessJob) claim(ctx context.Context, lane scheduling.Lane, now time.Time) (*scheduling.Job, error) {
	var job *scheduling.Job
	err := uc.store.WithTx(ctx, func(r port.Repos) error {
		j, err := r.Jobs().Claim(ctx, lane.Name, lane.Capacity, uc.owner, uc.leaseFor, now)
		if err != nil || j == nil {
			return err
		}
		job = j
		return r.Events().Append(ctx, &scheduling.JobEvent{
			BatchID:   j.BatchID,
			JobID:     j.ID,
			Type:      scheduling.EventJobClaimed,
			FromState: string(scheduling.JobPending),
			ToState:   string(scheduling.JobRunning),
			Actor:     uc.owner,
			Detail:    fmt.Sprintf("attempt=%d lease_until=%s", j.Attempt, j.LeaseExpires.Format(time.RFC3339)),
			At:        now,
		})
	})
	return job, err
}

func (uc *ProcessJob) settleSuccess(ctx context.Context, job *scheduling.Job, res port.Result, log *slog.Logger) {
	now := uc.clock.Now()
	err := uc.store.WithTx(ctx, func(r port.Repos) error {
		// 重新读取：任务可能在执行期间被取消，或租约已被 Reaper 收走。
		fresh, err := r.Jobs().Get(ctx, job.ID)
		if err != nil {
			return err
		}
		if err := fresh.Succeed(uc.owner, now); err != nil {
			return err
		}
		if err := r.Jobs().Update(ctx, fresh); err != nil {
			return err
		}
		return r.Events().Append(ctx, &scheduling.JobEvent{
			BatchID: fresh.BatchID, JobID: fresh.ID,
			Type:      scheduling.EventJobSucceeded,
			FromState: string(scheduling.JobRunning),
			ToState:   string(scheduling.JobSucceeded),
			Actor:     uc.owner, Detail: res.Summary, At: now,
		})
	})
	switch {
	case errors.Is(err, scheduling.ErrLeaseLost), errors.Is(err, scheduling.ErrIllegalTransition):
		// 任务在执行期间被取消或被回收，结果作废。这是预期内的竞态，不是故障。
		log.Info("discarding result, job no longer held", "reason", err)
	case err != nil:
		log.Error("failed to record success", "error", err)
	default:
		log.Info("job succeeded")
	}
}

func (uc *ProcessJob) settleFailure(ctx context.Context, job *scheduling.Job, cause string, log *slog.Logger) {
	now := uc.clock.Now()
	var willRetry bool
	err := uc.store.WithTx(ctx, func(r port.Repos) error {
		fresh, err := r.Jobs().Get(ctx, job.ID)
		if err != nil {
			return err
		}
		retry, err := fresh.Fail(uc.owner, cause, scheduling.BackoffFor(fresh.Attempt), now)
		if err != nil {
			return err
		}
		willRetry = retry
		if err := r.Jobs().Update(ctx, fresh); err != nil {
			return err
		}
		evType := scheduling.EventJobDead
		if retry {
			evType = scheduling.EventJobFailed
		}
		return r.Events().Append(ctx, &scheduling.JobEvent{
			BatchID: fresh.BatchID, JobID: fresh.ID,
			Type:      evType,
			FromState: string(scheduling.JobRunning),
			ToState:   string(fresh.State),
			Actor:     uc.owner, Detail: cause, At: now,
		})
	})
	switch {
	case errors.Is(err, scheduling.ErrLeaseLost), errors.Is(err, scheduling.ErrIllegalTransition):
		log.Info("discarding failure, job no longer held", "reason", err)
	case err != nil:
		log.Error("failed to record failure", "error", err, "cause", cause)
	default:
		log.Warn("job failed", "cause", cause, "will_retry", willRetry)
	}
}
