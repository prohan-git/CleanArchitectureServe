package port

import (
	"context"
	"encoding/json"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
)

// Task 是交给 Executor 的一次执行请求。
//
// 注意它不包含 Job 的状态、重试次数、租约——执行器不需要知道这些。
// 这条边界是刻意划的：重试、状态、审计归调度器管，模型推理归执行器管。
// Python 推理服务在这条线的另一侧，它看到的就只有 Kind/TargetRef/Payload。
type Task struct {
	JobID     string
	BatchID   string
	Kind      string
	TargetRef string
	Payload   json.RawMessage
	Attempt   int
}

// Result 是执行结果。业务产出（分析结论、缩略图地址）应当由 Executor
// 自行写入业务库，这里只回传调度需要的摘要，避免调度库承载业务数据。
type Result struct {
	Summary string
}

// Executor 执行某一类动作。一个 Kind 对应一个实现。
type Executor interface {
	Execute(ctx context.Context, t Task) (Result, error)
}

// ExecutorRegistry 按 Job.Kind 路由到具体执行器。
type ExecutorRegistry interface {
	Lookup(kind string) (Executor, bool)
}

// TargetExpander 把一个 Scope 展开成具体的业务对象引用列表。
//
// 这是调度核心与业务的唯一耦合点，而且是倒过来的：调度定义接口，业务来实现。
// "三班有哪 42 个学生""这个相册有哪 800 张照片"是业务知识，
// 由业务侧（读当前的 SQLite 业务库）回答，调度器不需要认识"班级"这个概念。
type TargetExpander interface {
	Expand(ctx context.Context, scope scheduling.Scope, trigger scheduling.Trigger) ([]Target, error)
}

// Target 是展开后的单个执行目标。
type Target struct {
	Ref     string
	Payload json.RawMessage
}
