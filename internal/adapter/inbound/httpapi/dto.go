package httpapi

import (
	"time"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
)

// submitBatchRequest 是提交调度单的请求体。
//
// 注意这里没有 trigger 字段：触发身份由服务端从认证信息推导，不接受客户端声明。
// 否则任何用户都能把自己标成 admin 去调度全库数据。
type submitBatchRequest struct {
	Title      string `json:"title"`
	JobKind    string `json:"job_kind"`
	Lane       string `json:"lane"`
	ScopeKind  string `json:"scope_kind"`
	ScopeRef   string `json:"scope_ref"`
	Priority   int    `json:"priority"`
	MaxRetries int    `json:"max_retries"`
	// IdempotencyKey 由调用方生成；重复提交同一个 key 不会产生重复任务。
	IdempotencyKey string `json:"idempotency_key"`
}

type submitBatchResponse struct {
	BatchID   string `json:"batch_id"`
	Total     int    `json:"total"`
	Enqueued  int    `json:"enqueued"`
	Duplicate bool   `json:"duplicate"`
}

type batchResponse struct {
	ID             string               `json:"id"`
	Title          string               `json:"title"`
	State          string               `json:"state"`
	TriggerKind    string               `json:"trigger_kind"`
	TriggerSubject string               `json:"trigger_subject"`
	ScopeKind      string               `json:"scope_kind"`
	ScopeRef       string               `json:"scope_ref"`
	Total          int                  `json:"total"`
	CreatedAt      time.Time            `json:"created_at"`
	Progress       *scheduling.Progress `json:"progress,omitempty"`
}

func toBatchResponse(b *scheduling.Batch, p *scheduling.Progress) batchResponse {
	return batchResponse{
		ID: b.ID, Title: b.Title, State: string(b.State),
		TriggerKind: string(b.Trigger.Kind), TriggerSubject: b.Trigger.Subject,
		ScopeKind: b.Scope.Kind, ScopeRef: b.Scope.Ref,
		Total: b.Total, CreatedAt: b.CreatedAt, Progress: p,
	}
}

type jobResponse struct {
	ID          string    `json:"id"`
	Lane        string    `json:"lane"`
	Kind        string    `json:"kind"`
	TargetRef   string    `json:"target_ref"`
	State       string    `json:"state"`
	Attempt     int       `json:"attempt"`
	MaxAttempts int       `json:"max_attempts"`
	LastError   string    `json:"last_error,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func toJobResponse(j *scheduling.Job) jobResponse {
	return jobResponse{
		ID: j.ID, Lane: j.Lane, Kind: j.Kind, TargetRef: j.TargetRef,
		State: string(j.State), Attempt: j.Attempt, MaxAttempts: j.MaxAttempts,
		LastError: j.LastError, UpdatedAt: j.UpdatedAt,
	}
}

type eventResponse struct {
	ID        int64     `json:"id"`
	JobID     string    `json:"job_id,omitempty"`
	Type      string    `json:"type"`
	FromState string    `json:"from_state,omitempty"`
	ToState   string    `json:"to_state,omitempty"`
	Actor     string    `json:"actor,omitempty"`
	Detail    string    `json:"detail,omitempty"`
	At        time.Time `json:"at"`
}

func toEventResponse(e *scheduling.JobEvent) eventResponse {
	return eventResponse{
		ID: e.ID, JobID: e.JobID, Type: string(e.Type),
		FromState: e.FromState, ToState: e.ToState,
		Actor: e.Actor, Detail: e.Detail, At: e.At,
	}
}
