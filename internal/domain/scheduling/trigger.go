package scheduling

import "fmt"

// TriggerKind 记录"这批任务是谁发起的"。
//
// 四种触发方（终端用户 / 管理员 / 系统事件 / 定时任务）走同一个创建入口、
// 同一套状态机，只在这个字段上区分。这样"按触发方筛选"是一次索引查询，
// 而不是四套代码路径。
type TriggerKind string

const (
	TriggerUser     TriggerKind = "user"
	TriggerAdmin    TriggerKind = "admin"
	TriggerEvent    TriggerKind = "event"
	TriggerSchedule TriggerKind = "schedule"
)

func (k TriggerKind) Validate() error {
	switch k {
	case TriggerUser, TriggerAdmin, TriggerEvent, TriggerSchedule:
		return nil
	default:
		return fmt.Errorf("%w: unknown trigger kind %q", ErrInvalidArgument, k)
	}
}

// Trigger 是一次调度的来源凭据：谁、以什么身份发起。
type Trigger struct {
	Kind    TriggerKind
	Subject string // 用户 ID / 管理员 ID / 事件名 / cron 名
}

func (t Trigger) Validate() error {
	if err := t.Kind.Validate(); err != nil {
		return err
	}
	if t.Subject == "" {
		return fmt.Errorf("%w: trigger subject is empty", ErrInvalidArgument)
	}
	return nil
}

// Scope 描述"对哪一组数据"执行动作。
//
// 这是体检场景里的"本人/本班/全校"和照片库里的"我的/某相册/全库"的统一抽象。
// 调度核心只存 Kind+Ref，不解释它的含义；解释工作交给 TargetExpander 端口
// （见 usecase/port），由业务侧实现。调度器因此不需要认识"班级"这个概念。
type Scope struct {
	Kind string // 例如 "self" / "album" / "class" / "library"
	Ref  string // 对应的业务主键；"library"/"self" 这类可以为空
}

func (s Scope) Validate() error {
	if s.Kind == "" {
		return fmt.Errorf("%w: scope kind is empty", ErrInvalidArgument)
	}
	return nil
}
