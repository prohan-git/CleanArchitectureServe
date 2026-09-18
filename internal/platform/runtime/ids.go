// Package runtime 提供进程级的通用设施：ID 生成与优雅退出。
package runtime

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// IDGen 生成按时间有序的随机 ID。
//
// 用时间前缀而非纯随机 UUID：ID 有序意味着主键索引是尾部追加而非随机写入，
// 在 B 树上这个差别在几十万行之后会非常明显。
type IDGen struct{}

func (IDGen) NewID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败属于系统级故障，没有合理的降级方案。
		panic(fmt.Sprintf("generate id: %v", err))
	}
	return fmt.Sprintf("%012x%s", time.Now().UTC().UnixMilli(), hex.EncodeToString(b[:]))
}
