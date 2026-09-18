package runtime

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// SignalContext 返回一个在收到 SIGINT/SIGTERM 时取消的 context。
//
// worker 必须优雅退出：正在跑的任务要么跑完并提交结果，要么让租约自然过期
// 由 Reaper 回收。直接 kill 不会丢任务（租约机制兜底），但会白白浪费一次执行。
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
