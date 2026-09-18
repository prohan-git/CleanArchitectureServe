package scheduling

import "time"

// EventType 是审计事件的类型。
type EventType string

const (
	EventBatchCreated  EventType = "batch.created"
	EventBatchCanceled EventType = "batch.canceled"
	EventJobEnqueued   EventType = "job.enqueued"
	EventJobClaimed    EventType = "job.claimed"
	EventJobSucceeded  EventType = "job.succeeded"
	EventJobFailed     EventType = "job.failed"
	EventJobDead       EventType = "job.dead"
	EventJobReclaimed  EventType = "job.reclaimed"
	EventJobCanceled   EventType = "job.canceled"
)

// JobEvent 是一条不可变的留底记录。
//
// jobs 表回答"现在是什么状态"，job_events 表回答"它是怎么走到这一步的"。
// 体检和照片库都要求"全程可监控、可留底"——留底指的就是这张只追加的表。
// 它同时是排查生产问题唯一可靠的依据：状态字段会被覆盖，事件不会。
type JobEvent struct {
	ID        int64
	BatchID   string
	JobID     string // 批级事件（如 batch.created）此处为空
	Type      EventType
	FromState string
	ToState   string
	Actor     string // 触发者或 worker 标识
	Detail    string
	At        time.Time
}
