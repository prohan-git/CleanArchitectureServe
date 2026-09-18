package sqlstore

import (
	"database/sql"
	"fmt"
)

// OpenSQLite 打开调度库。
//
// DSN 里的三个参数都不是可选的：
//   - _pragma=journal_mode(WAL)  读写不互相阻塞，否则一个长查询就会卡住所有入队
//   - _pragma=busy_timeout(5000) 拿不到锁时等待而不是立刻报 "database is locked"
//   - _txlock=immediate          事务一开始就取写锁，避免 deferred 事务升级锁时死锁
//
// 连接池限制为 1 是 SQLite 阶段的刻意取舍：SQLite 本来就只允许一个写者，
// 放开连接数只会把冲突从应用层挪到驱动层。调度的 DB 操作都是毫秒级短事务
// （任务执行本身不占连接），单连接足以支撑单机场景。
// 换 PostgreSQL 时把这里的上限调到几十，其余代码不动。
func OpenSQLite(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_txlock=immediate",
		path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite %s: %w", path, err)
	}
	return db, nil
}
