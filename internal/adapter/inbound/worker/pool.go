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

// Pool 为每条车道起 Capacity 个取号 goroutine。
//
// 为什么是 Capacity 个而不是一个：数据库里的容量校验是"上限保证"，
// 它只能拦住超发，不能制造并发。单个 goroutine 串行地 claim→执行→claim，
// 无论车道容量配多大，实际在途永远是 1。容量要兑现成吞吐，
// 必须在这里起对应数量的取号者。
//
// 两种限制因此各司其职：
//   - 数据库的 COUNT(*) < capacity —— 跨进程的硬上限，多起几个 worker 也不会超发
//   - 这里的 goroutine 数量       —— 单进程内实际能达到的并发
//
// 车道之间互不影响：GPU 车道被慢任务占满两小时，缩略图车道照常出活。
//
// 这些 goroutine 不共享任何可变状态，因此不需要锁——
// "哪个任务归谁跑"完全由数据库的原子 Claim 裁决。
type Pool struct {
	process *usecase.ProcessJob
	lanes   []scheduling.Lane
	log     *slog.Logger

	// idleDelay 是车道无活可干时的等待时长。
	// 有任务时立刻取下一个，不等待——所以吞吐不受这个值影响，只有首个任务的启动延迟受影响。
	idleDelay time.Duration
}

func NewPool(process *usecase.ProcessJob, lanes []scheduling.Lane, idleDelay time.Duration, log *slog.Logger) *Pool {
	return &Pool{process: process, lanes: lanes, idleDelay: idleDelay, log: log}
}

// Run 阻塞运行直到 ctx 取消，返回时所有车道循环均已退出。
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	slots := 0

	for _, lane := range p.lanes {
		// 限速车道由一个共享的令牌发生器控制启动速率；
		// 用 channel 而不是每个 goroutine 各自计时，否则总速率会变成 capacity 倍。
		tokens := p.startTokenSource(ctx, lane, &wg)

		for slot := 0; slot < lane.Capacity; slot++ {
			wg.Add(1)
			slots++
			go func(l scheduling.Lane, n int, tk <-chan struct{}) {
				defer wg.Done()
				p.runSlot(ctx, l, n, tk)
			}(lane, slot, tokens)
		}
	}

	p.log.Info("worker pool started", "lanes", len(p.lanes), "slots", slots)
	wg.Wait()
	p.log.Info("worker pool stopped")
}

// startTokenSource 为限速车道启动令牌发生器，其它车道返回 nil（表示不限速）。
func (p *Pool) startTokenSource(ctx context.Context, lane scheduling.Lane, wg *sync.WaitGroup) <-chan struct{} {
	if lane.Kind != scheduling.LaneRateLimited || lane.RatePerS <= 0 {
		return nil
	}
	interval := time.Duration(float64(time.Second) / lane.RatePerS)
	ch := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				select {
				case ch <- struct{}{}:
				case <-ctx.Done():
					return
				default:
					// 没有取号者在等，令牌作废，不积压突发额度。
				}
			}
		}
	}()
	return ch
}

// runSlot 是一个取号者的循环：取令牌（若限速）→ 领任务 → 执行 → 再来。
func (p *Pool) runSlot(ctx context.Context, lane scheduling.Lane, slot int, tokens <-chan struct{}) {
	log := p.log.With("lane", lane.Name, "lane_kind", string(lane.Kind), "slot", slot)
	log.Info("lane slot started")
	defer log.Info("lane slot stopped")

	backoff := p.idleDelay
	for {
		if ctx.Err() != nil {
			return
		}

		// 限速车道：先拿到令牌才允许发起下一个任务。
		if tokens != nil {
			select {
			case <-tokens:
			case <-ctx.Done():
				return
			}
		}

		worked, err := p.process.Execute(ctx, lane)
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
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
