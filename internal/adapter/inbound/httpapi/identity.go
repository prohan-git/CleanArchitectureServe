package httpapi

import (
	"fmt"
	"net/http"

	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
)

// resolveTrigger 从请求中推导出触发身份。
//
// 骨架期用请求头占位；接入真实鉴权时把这里换成解析 JWT / session，
// 调用方代码不用改。关键是这个推导必须发生在服务端：
// 触发身份决定了 scope 能展开出什么，让客户端自报身份等于放弃权限控制。
func resolveTrigger(r *http.Request) (scheduling.Trigger, error) {
	t := scheduling.Trigger{
		Kind:    scheduling.TriggerKind(r.Header.Get("X-Actor-Kind")),
		Subject: r.Header.Get("X-Actor-Subject"),
	}
	if t.Kind == "" {
		t.Kind = scheduling.TriggerUser
	}
	if err := t.Validate(); err != nil {
		return scheduling.Trigger{}, fmt.Errorf("%w (set X-Actor-Kind and X-Actor-Subject)", err)
	}
	return t, nil
}

// authorizeScope 校验触发方是否有权调度这个范围。
//
// 这是"终端用户只能处理我自己的数据"这条需求的强制点。
// 放在入站适配器而不是用例层，是因为它依赖传输层的身份信息；
// 展开后的目标归属校验则应由 expander 负责。
func authorizeScope(t scheduling.Trigger, scope scheduling.Scope) error {
	if t.Kind == scheduling.TriggerUser && scope.Kind != "self" {
		return fmt.Errorf("%w: user %q may only schedule scope \"self\", not %q",
			scheduling.ErrInvalidArgument, t.Subject, scope.Kind)
	}
	return nil
}
