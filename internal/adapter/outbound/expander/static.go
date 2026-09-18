// Package expander 实现 port.TargetExpander：把调度范围展开成具体的业务对象。
//
// 这是调度核心与业务库之间唯一的连接点，而且方向是倒过来的：
// 调度定义接口、业务来实现。真实实现应当在这里读业务库（当前的 SQLite），
// 回答"三班有哪 42 个学生""这个相册有哪 800 张照片"。
//
// 调度器因此不需要认识"班级"或"相册"——换业务领域时这个包重写，调度内核不动。
package expander

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// Static 是骨架期的占位实现：直接把 scope.Ref 当成逗号分隔的目标列表。
// 接真实业务库时，把这个类型换成 SQLExpander 即可，用例层无需改动。
type Static struct{}

func NewStatic() *Static { return &Static{} }

func (s *Static) Expand(ctx context.Context, scope scheduling.Scope, trigger scheduling.Trigger) ([]port.Target, error) {
	switch scope.Kind {
	case "explicit":
		// scope.Ref 形如 "photo-1,photo-2,photo-3"
		refs := splitAndTrim(scope.Ref)
		if len(refs) == 0 {
			return nil, fmt.Errorf("%w: explicit scope has no refs", scheduling.ErrInvalidArgument)
		}
		out := make([]port.Target, 0, len(refs))
		for _, ref := range refs {
			out = append(out, port.Target{Ref: ref, Payload: json.RawMessage(`{}`)})
		}
		return out, nil

	case "self":
		// 终端用户只能调度"自己的"数据：目标范围由 trigger.Subject 决定，
		// 而不是由请求体里的字段决定。权限边界必须在服务端推导，不能由客户端声明。
		return []port.Target{{Ref: trigger.Subject, Payload: json.RawMessage(`{}`)}}, nil

	default:
		return nil, fmt.Errorf("%w: expander does not know scope kind %q (implement it against the business DB)",
			scheduling.ErrInvalidArgument, scope.Kind)
	}
}

func splitAndTrim(s string) []string {
	var out []string
	start := -1
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if start >= 0 {
				out = append(out, s[start:i])
				start = -1
			}
			continue
		}
		if s[i] != ' ' && start < 0 {
			start = i
		}
	}
	return out
}
