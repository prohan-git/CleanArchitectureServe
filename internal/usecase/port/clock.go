package port

import "time"

// Clock 把"现在几点"变成可注入的依赖，让状态机和退避逻辑可以在测试里被确定性地驱动。
type Clock interface {
	Now() time.Time
}

// SystemClock 是生产环境使用的实现。
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// IDGenerator 生成批与任务的主键。抽成端口是为了在测试中产出可预期的 ID。
type IDGenerator interface {
	NewID() string
}
