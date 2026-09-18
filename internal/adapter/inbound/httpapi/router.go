package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// Server 是控制面 HTTP 接口。
//
// 它只做三件事：提交调度单、查询进度、取消。
// 重活（AI 分析、缩略图）一律不在这个进程里跑——这是在线路径与离线路径分离的落点。
// 上传下载这类在线请求应当由业务网关直接同步处理，根本不进这套队列。
type Server struct {
	submit *usecase.SubmitBatch
	query  *usecase.QueryBatch
	cancel *usecase.CancelBatch
	log    *slog.Logger
}

func NewServer(submit *usecase.SubmitBatch, query *usecase.QueryBatch, cancel *usecase.CancelBatch, log *slog.Logger) *Server {
	return &Server{submit: submit, query: query, cancel: cancel, log: log}
}

// Routes 使用标准库的 ServeMux（Go 1.22+ 支持方法与路径参数），不引入路由框架。
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /api/v1/batches", s.handleSubmitBatch)
	mux.HandleFunc("GET /api/v1/batches", s.handleListBatches)
	mux.HandleFunc("GET /api/v1/batches/{id}", s.handleGetBatch)
	mux.HandleFunc("POST /api/v1/batches/{id}/cancel", s.handleCancelBatch)
	mux.HandleFunc("GET /api/v1/batches/{id}/jobs", s.handleListJobs)
	mux.HandleFunc("GET /api/v1/batches/{id}/events", s.handleListEvents)
	return s.withLogging(mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.log, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleSubmitBatch(w http.ResponseWriter, r *http.Request) {
	var req submitBatchRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, s.log, scheduling.ErrInvalidArgument)
		return
	}

	trigger, err := resolveTrigger(r)
	if err != nil {
		writeError(w, s.log, err)
		return
	}
	scope := scheduling.Scope{Kind: req.ScopeKind, Ref: req.ScopeRef}
	if err := authorizeScope(trigger, scope); err != nil {
		writeError(w, s.log, err)
		return
	}

	out, err := s.submit.Execute(r.Context(), usecase.SubmitBatchInput{
		Title:          req.Title,
		Trigger:        trigger,
		Scope:          scope,
		JobKind:        req.JobKind,
		Lane:           req.Lane,
		Priority:       req.Priority,
		MaxRetries:     req.MaxRetries,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		writeError(w, s.log, err)
		return
	}
	writeJSON(w, s.log, http.StatusAccepted, submitBatchResponse(out))
}

func (s *Server) handleGetBatch(w http.ResponseWriter, r *http.Request) {
	view, err := s.query.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, s.log, err)
		return
	}
	writeJSON(w, s.log, http.StatusOK, toBatchResponse(view.Batch, &view.Progress))
}

func (s *Server) handleListBatches(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	batches, err := s.query.List(r.Context(), port.BatchFilter{
		TriggerKind:    scheduling.TriggerKind(q.Get("trigger_kind")),
		TriggerSubject: q.Get("trigger_subject"),
		State:          scheduling.BatchState(q.Get("state")),
		Limit:          atoiOr(q.Get("limit"), 50),
		Offset:         atoiOr(q.Get("offset"), 0),
	})
	if err != nil {
		writeError(w, s.log, err)
		return
	}
	out := make([]batchResponse, 0, len(batches))
	for _, b := range batches {
		out = append(out, toBatchResponse(b, nil))
	}
	writeJSON(w, s.log, http.StatusOK, map[string]any{"batches": out})
}

func (s *Server) handleCancelBatch(w http.ResponseWriter, r *http.Request) {
	trigger, err := resolveTrigger(r)
	if err != nil {
		writeError(w, s.log, err)
		return
	}
	n, err := s.cancel.Execute(r.Context(), r.PathValue("id"), trigger.Subject)
	if err != nil {
		writeError(w, s.log, err)
		return
	}
	writeJSON(w, s.log, http.StatusOK, map[string]any{"canceled_jobs": n})
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	jobs, err := s.query.Jobs(r.Context(), r.PathValue("id"), atoiOr(q.Get("limit"), 50), atoiOr(q.Get("offset"), 0))
	if err != nil {
		writeError(w, s.log, err)
		return
	}
	out := make([]jobResponse, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, toJobResponse(j))
	}
	writeJSON(w, s.log, http.StatusOK, map[string]any{"jobs": out})
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	events, err := s.query.Events(r.Context(), r.PathValue("id"), atoiOr(q.Get("limit"), 200), atoiOr(q.Get("offset"), 0))
	if err != nil {
		writeError(w, s.log, err)
		return
	}
	out := make([]eventResponse, 0, len(events))
	for _, e := range events {
		out = append(out, toEventResponse(e))
	}
	writeJSON(w, s.log, http.StatusOK, map[string]any{"events": out})
}

func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.log.Info("request", "method", r.Method, "path", r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func atoiOr(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return fallback
	}
	return n
}
