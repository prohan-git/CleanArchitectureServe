package scheduling

import (
	"encoding/json"
	"fmt"
	"time"
)

// JobState 是单个任务的生命周期状态。
//
//	pending ──claim──► running ──ok────► succeeded
//	   ▲                  │
//	   │                  ├──err, 还有重试次数──► pending（退避后再取）
//	   │                  ├──err, 次数耗尽──────► dead（死信，留底待人工处理）
//	   └──lease 过期──────┘
//
//	pending/running ──cancel──► canceled
//
// succeeded / dead / canceled 是终态，不可再迁移。
type JobState string

const (
	JobPending   JobState = "pending"
	JobRunning   JobState = "running"
	JobSucceeded JobState = "succeeded"
	JobDead      JobState = "dead"
	JobCanceled  JobState = "canceled"
)

func (s JobState) IsTerminal() bool {
	return s == JobSucceeded || s == JobDead || s == JobCanceled
}

// Job 是调度的最小执行单元，也是本系统的事实源。
//
// 设计要点：任务状态只以这张表为准。任何缓存、消息队列、result backend
// 都只能是它的派生物或加速层，绝不能成为唯一真相——否则组件一重启，
// "三班体检到哪一步了"就永远答不上来了。
type Job struct {
	ID      string
	BatchID string

	Lane string // 在哪条资源车道上执行
	Kind string // 执行什么动作，决定由哪个 Executor 处理，例如 "ai.analyze"

	// TargetRef 是业务对象的引用（学生 ID、照片 ID）。
	// 调度库只存引用，不复制业务数据——业务库（当前 SQLite）仍是业务数据的事实源，
	// 两边靠 ID 关联。这是"调度与业务解耦"的具体落点。
	TargetRef string
	Payload   json.RawMessage // 执行所需的额外参数

	// IdempotencyKey 保证同一语义的任务只入队一次。
	// 定时任务重复触发、用户连点两次提交、事件重放，都靠它去重。
	IdempotencyKey string

	State    JobState
	Priority int

	Attempt     int // 已经尝试过的次数
	MaxAttempts int

	AvailableAt time.Time // 早于此刻才可被领取，用于重试退避与延时任务

	// 租约：worker 领走任务时写入，用于崩溃检测。
	// 没有租约，worker 进程被 kill -9 之后这个任务就永远卡在 running。
	LeaseOwner   string
	LeaseExpires time.Time

	LastError string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewJob 构造一个待执行任务。
func NewJob(id, batchID, lane, kind, targetRef string, payload json.RawMessage, opts JobOptions, now time.Time) (*Job, error) {
	switch {
	case id == "":
		return nil, fmt.Errorf("%w: job id is empty", ErrInvalidArgument)
	case batchID == "":
		return nil, fmt.Errorf("%w: job batch id is empty", ErrInvalidArgument)
	case lane == "":
		return nil, fmt.Errorf("%w: job lane is empty", ErrInvalidArgument)
	case kind == "":
		return nil, fmt.Errorf("%w: job kind is empty", ErrInvalidArgument)
	}
	opts = opts.withDefaults()
	availableAt := now
	if !opts.NotBefore.IsZero() {
		availableAt = opts.NotBefore
	}
	return &Job{
		ID:             id,
		BatchID:        batchID,
		Lane:           lane,
		Kind:           kind,
		TargetRef:      targetRef,
		Payload:        payload,
		IdempotencyKey: opts.IdempotencyKey,
		State:          JobPending,
		Priority:       opts.Priority,
		MaxAttempts:    opts.MaxAttempts,
		AvailableAt:    availableAt,
		CreatedAt:      now,
		UpdatedAt:      now,
	}, nil
}

// JobOptions 是创建任务时的可选参数。
type JobOptions struct {
	Priority       int
	MaxAttempts    int
	NotBefore      time.Time
	IdempotencyKey string
}

const defaultMaxAttempts = 3

func (o JobOptions) withDefaults() JobOptions {
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = defaultMaxAttempts
	}
	return o
}

// Claim 把任务标记为被 owner 领取，持有到 leaseUntil 为止。
func (j *Job) Claim(owner string, leaseUntil time.Time, now time.Time) error {
	if j.State != JobPending {
		return fmt.Errorf("%w: cannot claim job in state %q", ErrIllegalTransition, j.State)
	}
	if owner == "" {
		return fmt.Errorf("%w: lease owner is empty", ErrInvalidArgument)
	}
	j.State = JobRunning
	j.Attempt++
	j.LeaseOwner = owner
	j.LeaseExpires = leaseUntil
	j.UpdatedAt = now
	return nil
}

// Succeed 由持有租约的 worker 在执行成功后调用。
func (j *Job) Succeed(owner string, now time.Time) error {
	if err := j.assertHeldBy(owner); err != nil {
		return err
	}
	j.State = JobSucceeded
	j.LastError = ""
	j.clearLease()
	j.UpdatedAt = now
	return nil
}

// Fail 记录一次失败。若仍有重试次数，任务退回 pending 并在 retryAfter 之后可再次领取；
// 否则进入 dead（死信），保留现场等待人工介入。返回值说明任务是否还会重试。
func (j *Job) Fail(owner string, cause string, retryAfter time.Duration, now time.Time) (willRetry bool, err error) {
	if err := j.assertHeldBy(owner); err != nil {
		return false, err
	}
	j.LastError = cause
	j.clearLease()
	j.UpdatedAt = now
	if j.Attempt < j.MaxAttempts {
		j.State = JobPending
		j.AvailableAt = now.Add(retryAfter)
		return true, nil
	}
	j.State = JobDead
	return false, nil
}

// Reclaim 由 reaper 调用，回收租约已过期的任务（通常意味着 worker 崩溃了）。
// 它与 Fail 走同一条重试/死信规则，因为"worker 死了"和"任务失败了"对调用方是一回事。
func (j *Job) Reclaim(reason string, retryAfter time.Duration, now time.Time) (willRetry bool, err error) {
	if j.State != JobRunning {
		return false, fmt.Errorf("%w: cannot reclaim job in state %q", ErrIllegalTransition, j.State)
	}
	if now.Before(j.LeaseExpires) {
		return false, fmt.Errorf("%w: lease of job %s is still valid", ErrConflict, j.ID)
	}
	owner := j.LeaseOwner
	j.LeaseOwner = owner // Fail 需要校验持有者，这里以原持有者身份收尾
	return j.Fail(owner, reason, retryAfter, now)
}

// Cancel 取消尚未进入终态的任务。已在 running 的任务同样置为 canceled：
// 正在跑的那次执行提交结果时会因租约校验失败而被拒绝，不会覆盖取消状态。
func (j *Job) Cancel(now time.Time) error {
	if j.State.IsTerminal() {
		return fmt.Errorf("%w: cannot cancel job in terminal state %q", ErrIllegalTransition, j.State)
	}
	j.State = JobCanceled
	j.clearLease()
	j.UpdatedAt = now
	return nil
}

func (j *Job) assertHeldBy(owner string) error {
	if j.State != JobRunning {
		return fmt.Errorf("%w: job %s is in state %q, not running", ErrIllegalTransition, j.ID, j.State)
	}
	if j.LeaseOwner != owner {
		return fmt.Errorf("%w: job %s is held by %q, not %q", ErrLeaseLost, j.ID, j.LeaseOwner, owner)
	}
	return nil
}

func (j *Job) clearLease() {
	j.LeaseOwner = ""
	j.LeaseExpires = time.Time{}
}

// BackoffFor 返回第 attempt 次失败后应等待的时长（指数退避，封顶 10 分钟）。
func BackoffFor(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	const (
		base = 2 * time.Second
		max  = 10 * time.Minute
	)
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 3
	}
	if d > max {
		d = max
	}
	return d
}
