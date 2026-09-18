// Package migrations 把 SQL 迁移脚本嵌入二进制。
//
// 迁移脚本放在仓库顶层（DBA 和 code review 都能一眼看到），同时通过 embed
// 编译进程序，部署时不需要额外分发 .sql 文件——单二进制部署的前提。
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
