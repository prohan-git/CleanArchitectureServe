package executor

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// NewDemo 返回一个模拟耗时任务的执行器，用于在没有真实推理服务时打通全链路。
// failRate 是模拟失败率（0~1），用来观察重试与死信是否按预期工作。
//
// 上生产前删掉它——留着示例执行器是事故的常见来源。
func NewDemo(work time.Duration, failRate float64) port.Executor {
	return Func(func(ctx context.Context, t port.Task) (port.Result, error) {
		select {
		case <-time.After(work):
		case <-ctx.Done():
			return port.Result{}, ctx.Err()
		}
		if rand.Float64() < failRate {
			return port.Result{}, fmt.Errorf("simulated failure on %s (attempt %d)", t.TargetRef, t.Attempt)
		}
		return port.Result{Summary: fmt.Sprintf("demo processed %s in %s", t.TargetRef, work)}, nil
	})
}
