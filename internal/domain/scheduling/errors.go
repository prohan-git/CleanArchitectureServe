package scheduling

import "errors"

// 领域错误。适配层负责把它们翻译成 HTTP 状态码等传输层概念，
// 领域层本身不认识 HTTP。
var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrNotFound        = errors.New("not found")
	ErrConflict        = errors.New("conflict")
	// ErrIllegalTransition 表示对聚合根做了状态机不允许的状态迁移。
	ErrIllegalTransition = errors.New("illegal state transition")
	// ErrLeaseLost 表示 worker 想提交结果，但它持有的租约已经过期或被他人接管。
	ErrLeaseLost = errors.New("lease lost")
)
