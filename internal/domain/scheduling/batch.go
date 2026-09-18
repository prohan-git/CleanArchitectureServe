package scheduling

import (
	"fmt"
	"time"
)

// BatchState 是一张调度单的状态。
type BatchState string

const (
	BatchOpen     BatchState = "open"     // 已创建，任务在执行中
	BatchDone     BatchState = "done"     // 所有任务进入终态
	BatchCanceled BatchState = "canceled" // 被整单取消
)

// Batch 是"一次派发"这件事本身：给三班 42 个学生派体检、对某相册 800 张照片跑 AI。
//
// 为什么必须有这一层：用户和管理员关心的单位是"这一批",不是"这一个任务"。
// 只有 task_id 的系统答不出"整体进度""整单取消""这批是谁在什么时候发的"。
// 把批当成一等公民建模，进度查询就是一次 group by，而不是前端自己聚合 N 个 id。
type Batch struct {
	ID      string
	Title   string
	Trigger Trigger
	Scope   Scope

	State BatchState

	// IdempotencyKey 让"同一次派发"可被识别。定时任务重复触发、用户连点两次提交，
	// 返回的都是同一张批，而不是多张内容相同的批。
	IdempotencyKey string

	// Total 在展开目标后写入，用于计算进度分母。
	Total int

	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewBatch(id, title string, trigger Trigger, scope Scope, now time.Time) (*Batch, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: batch id is empty", ErrInvalidArgument)
	}
	if title == "" {
		return nil, fmt.Errorf("%w: batch title is empty", ErrInvalidArgument)
	}
	if err := trigger.Validate(); err != nil {
		return nil, err
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	return &Batch{
		ID:        id,
		Title:     title,
		Trigger:   trigger,
		Scope:     scope,
		State:     BatchOpen,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func (b *Batch) Cancel(now time.Time) error {
	if b.State != BatchOpen {
		return fmt.Errorf("%w: cannot cancel batch in state %q", ErrIllegalTransition, b.State)
	}
	b.State = BatchCanceled
	b.UpdatedAt = now
	return nil
}

// Progress 是一张调度单的实时进度，由 Job 表聚合得到。
// 它是派生数据，不落库——落库就要维护一致性，而 group by 的代价远低于此。
type Progress struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Succeeded int `json:"succeeded"`
	Dead      int `json:"dead"`
	Canceled  int `json:"canceled"`
}

func (p Progress) Settled() int { return p.Succeeded + p.Dead + p.Canceled }

func (p Progress) IsComplete() bool { return p.Total > 0 && p.Settled() == p.Total }
