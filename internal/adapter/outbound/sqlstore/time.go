package sqlstore

import "time"

// 时间在库中以 Unix 毫秒存储；零值用 0 表示，往返转换收敛在这两个函数里。

func toMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func fromMillis(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func nowMillis() int64 { return time.Now().UTC().UnixMilli() }
