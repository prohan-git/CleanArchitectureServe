package executor_test

import (
	"context"
	"testing"

	"github.com/prohan-git/CleanArchitectureServe/internal/adapter/outbound/executor"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

func makeThumbnail(ctx context.Context, ref string) error { return nil }

// TestRegisterGoExecutor 固定 README「接入你自己的业务 · 情况 B」里的代码示例。
//
// 文档里的代码会腐烂，除非它能被编译器检查。这个测试的唯一职责就是保证
// 那段示例始终可编译、可运行——改了执行器接口就会在这里先红。
func TestRegisterGoExecutor(t *testing.T) {
	reg := executor.NewRegistry()

	reg.Register("thumbnail.generate", executor.Func(
		func(ctx context.Context, t port.Task) (port.Result, error) {
			// t.TargetRef 是业务对象 ID（照片 ID、学生 ID）
			if err := makeThumbnail(ctx, t.TargetRef); err != nil {
				return port.Result{}, err // 返回 error 就会走重试
			}
			return port.Result{Summary: "thumbnail ok"}, nil
		}))

	e, ok := reg.Lookup("thumbnail.generate")
	if !ok {
		t.Fatal("executor not registered")
	}
	res, err := e.Execute(context.Background(), port.Task{TargetRef: "photo-1"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Summary != "thumbnail ok" {
		t.Fatalf("unexpected summary: %q", res.Summary)
	}
}

// TestUnregisteredKindIsReported 固定另一条文档承诺：
// 未注册的任务类型不会被静默忽略，而是被明确报告。
func TestUnregisteredKindIsReported(t *testing.T) {
	reg := executor.NewRegistry()
	if _, ok := reg.Lookup("never.registered"); ok {
		t.Fatal("lookup of an unregistered kind must report ok=false")
	}
}
