// Package observability 提供结构化日志。
package observability

import (
	"log/slog"
	"os"
)

// NewLogger 返回一个 JSON 结构化日志器。
// 调度系统的日志是排障主线索，必须可被机器检索，不能是拼接出来的人话。
func NewLogger(component string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h).With("component", component)
}
