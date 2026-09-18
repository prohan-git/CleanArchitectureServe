// Package httpapi 是入站适配器：把 HTTP 请求翻译成用例调用。
//
// 这一层只做翻译，不含业务判断。所有业务规则都在 usecase 与 domain 里，
// 因此将来加一个 gRPC 或 CLI 入口，是新写一个适配器，而不是复制业务逻辑。
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
)

type errorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// writeError 把领域错误翻译成 HTTP 状态码。
// 领域层不认识 HTTP，这个映射表是两个世界之间唯一的翻译处。
func writeError(w http.ResponseWriter, log *slog.Logger, err error) {
	switch {
	case errors.Is(err, scheduling.ErrInvalidArgument):
		writeJSON(w, log, http.StatusBadRequest, errorBody{"invalid_argument", err.Error()})
	case errors.Is(err, scheduling.ErrNotFound):
		writeJSON(w, log, http.StatusNotFound, errorBody{"not_found", err.Error()})
	case errors.Is(err, scheduling.ErrConflict), errors.Is(err, scheduling.ErrIllegalTransition):
		writeJSON(w, log, http.StatusConflict, errorBody{"conflict", err.Error()})
	default:
		// 未预期的错误不把内部细节暴露给调用方，但要完整记进日志。
		log.Error("unhandled error", "error", err)
		writeJSON(w, log, http.StatusInternalServerError, errorBody{"internal", "internal server error"})
	}
}

func writeJSON(w http.ResponseWriter, log *slog.Logger, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Error("write response", "error", err)
	}
}
