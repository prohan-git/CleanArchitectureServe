// Package executor 实现具体的任务执行器，按 Job.Kind 路由。
//
// 执行器是调度器与业务之间的边界：调度器负责"什么时候、在哪条车道上、失败了怎么办"，
// 执行器负责"这件事具体怎么做"。执行器不感知重试次数、租约和批进度。
package executor

import (
	"context"

	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// Registry 是 kind -> Executor 的静态路由表，在启动时装配完成。
type Registry struct {
	execs map[string]port.Executor
}

func NewRegistry() *Registry {
	return &Registry{execs: make(map[string]port.Executor)}
}

// Register 注册一种任务类型的执行器。重复注册会覆盖，装配错误应在启动时暴露。
func (r *Registry) Register(kind string, e port.Executor) *Registry {
	r.execs[kind] = e
	return r
}

func (r *Registry) Lookup(kind string) (port.Executor, bool) {
	e, ok := r.execs[kind]
	return e, ok
}

// Kinds 返回已注册的任务类型，供启动日志与健康检查使用。
func (r *Registry) Kinds() []string {
	out := make([]string, 0, len(r.execs))
	for k := range r.execs {
		out = append(out, k)
	}
	return out
}

// Func 让简单执行器可以直接用函数实现，不必为此定义一个类型。
type Func func(ctx context.Context, t port.Task) (port.Result, error)

func (f Func) Execute(ctx context.Context, t port.Task) (port.Result, error) { return f(ctx, t) }
