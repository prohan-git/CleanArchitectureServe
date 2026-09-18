package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// Inference 通过 HTTP 调用一个独立的推理服务（通常是 Python + FastAPI）。
//
// 这是 Go 调度与 Python 算法的接缝，也是本架构里"两种语言各取所长"的落点：
//
//	Go 侧   ── 批、车道、租约、重试、审计，全在调度库里
//	Python 侧 ── 只暴露一个无状态端点：给我 target，还我结论
//
// 推理服务不认识 job_id 语义上的重试，不需要 Celery，不需要 Redis，
// 也不需要维护任何状态；它崩溃了，租约到期后 Reaper 会把任务放回队列。
// 算法同学写的是一个函数，不是一个分布式组件——这正是我们想要的分工。
type Inference struct {
	endpoint string
	client   *http.Client
}

// NewInference 构造推理执行器。timeout 应当小于车道的租约时长，
// 否则租约会先于 HTTP 超时到期，任务被 Reaper 重复派发。
func NewInference(endpoint string, timeout time.Duration) *Inference {
	return &Inference{
		endpoint: endpoint,
		client:   &http.Client{Timeout: timeout},
	}
}

type inferenceRequest struct {
	JobID     string          `json:"job_id"` // 仅用于日志串联，推理侧不据此做任何决策
	Kind      string          `json:"kind"`
	TargetRef string          `json:"target_ref"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type inferenceResponse struct {
	Summary string `json:"summary"`
	Error   string `json:"error,omitempty"`
}

func (e *Inference) Execute(ctx context.Context, t port.Task) (port.Result, error) {
	body, err := json.Marshal(inferenceRequest{
		JobID: t.JobID, Kind: t.Kind, TargetRef: t.TargetRef, Payload: t.Payload,
	})
	if err != nil {
		return port.Result{}, fmt.Errorf("encode inference request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint, bytes.NewReader(body))
	if err != nil {
		return port.Result{}, fmt.Errorf("build inference request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return port.Result{}, fmt.Errorf("call inference service: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return port.Result{}, fmt.Errorf("read inference response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return port.Result{}, fmt.Errorf("inference service returned %d: %s", resp.StatusCode, truncate(raw, 256))
	}

	var out inferenceResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return port.Result{}, fmt.Errorf("decode inference response: %w", err)
	}
	if out.Error != "" {
		return port.Result{}, fmt.Errorf("inference failed: %s", out.Error)
	}
	return port.Result{Summary: out.Summary}, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
