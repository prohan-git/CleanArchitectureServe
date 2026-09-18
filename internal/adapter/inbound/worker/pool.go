// Package worker 是由时间驱动的入站适配器：轮询车道并驱动 ProcessJob 用例。
//
// 它与 httpapi 处于同一层——都是"把外部刺激翻译成用例调用"。
// 区别只是 httpapi 被 HTTP 请求驱动，它被 ticker 驱动。
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase"
)

// Pool 为每条车道起一个 goroutine 循环取任务。
//
// 每条车道一个 goroutine，而不是一个全局 worker 池：这样一条车道被慢任务占满时
// 不会饿死其它车道。GPU 跑满两小时，缩略图照常出。
//
// 注意这些 goroutine 之间不共享任何状态，因此不需要任何锁——
// "哪个任务归谁跑"完全由数据库的原子 Claim 决定。这正是本设计要替换掉的
// "在主线程里开后台任务然后到处加锁"那套做法。
type Pool struct {
	process *usecase.ProcessJob
	lanes   []scheduling.Lane
	log     *slog.Logger

	// idleDelay 是车道无活可干时的等待时长。
	// 有任务时立刻取下一个，不等待——所以吞吐不受这个值影响，只有首个任务的延迟受影响。
	idleDelay time.Duration
}

func NewPool(process *usecase.ProcessJob, lanes []scheduling.Lane, idleDelay time.Duration, log *slog.Logger) *Pool {
	return &Pool{process: process, lanes: lanes, idleDelay: idleDelay, log: log}
}

// Run 阻塞运行直到 ctx 取消，返回时所有车道循环均已退出。
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, lane := range p.lanes {
		wg.Add(1)
		go func(l scheduling.Lane) {
			defer wg.Done()
			p.runLane(ctx, l)
		}(lane)
	}
	p.log.Info("worker pool started", "lanes", len(p.lanes))
	wg.Wait()
	p.log.Info("worker pool stopped")
}

func (p *Pool) runLane(ctx context.Context, lane scheduling.Lane) {
	log := p.log.With("lane", lane.Name, "kind", string(lane.Kind), "capacity", lane.Capacity)
	log.Info("lane loop started")

	// minRate 兑现限速车道：两次启动之间至少间隔 1/rate 秒。
	var minInterval time.Duration
	if lane.Kind == scheduling.LaneRateLimited && lane.RatePerS > 0 {
		minInterval = time.Duration(float64(time.Second) / lane.RatePerS)
	}

	backoff := p.idleDelay
	for {
		if ctx.Err() != nil {
			log.Info("lane loop stopped")
			return
		}

		started := time.Now()
		worked, err := p.process.Execute(ctx, lane)
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			log.Info("lane loop stopped")
			return
		case err != nil:
			// 存储层故障：退避重试，不要把日志刷爆。
			log.Error("lane iteration failed", "error", err, "retry_in", backoff)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = p.idleDelay

		if !worked {
			// 车道空闲或已满，等一会再看。
			if !sleepCtx(ctx, p.idleDelay) {
				return
			}
			continue
		}

		// 刚干完一件活，立刻看下一件；限速车道在此处补足最小间隔。
		if minInterval > 0 {
			if wait := minInterval - time.Since(started); wait > 0 {
				if !sleepCtx(ctx, wait) {
					return
				}
			}
		}
	}
}

// sleepCtx 等待 d，ctx 取消时提前返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func nextBackoff(d time.Duration) time.Duration {
	const max = 30 * time.Second
	d *= 2
	if d > max {
		return max
	}
	return d
}
