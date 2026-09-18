// Command worker 是数据面：按车道领取任务并执行。
//
// 它与 server 共享同一个调度库，但不共享任何内存状态。
// 想提高吞吐就多起几个 worker 进程——不需要改代码、不需要协调、不需要加锁，
// 因为"哪个任务归谁跑"由数据库的原子 Claim 裁决。
// （注意：多进程共享同一个 SQLite 文件仅限同机；跨机部署必须先换成 PostgreSQL。）
package main

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	"github.com/prohan-git/CleanArchitectureServe/internal/adapter/inbound/worker"
	"github.com/prohan-git/CleanArchitectureServe/internal/adapter/outbound/executor"
	"github.com/prohan-git/CleanArchitectureServe/internal/adapter/outbound/sqlstore"
	"github.com/prohan-git/CleanArchitectureServe/internal/platform/config"
	"github.com/prohan-git/CleanArchitectureServe/internal/platform/observability"
	apprt "github.com/prohan-git/CleanArchitectureServe/internal/platform/runtime"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	log := observability.NewLogger("worker").With("worker_id", cfg.WorkerID)

	ctx, stop := apprt.SignalContext()
	defer stop()

	db, err := sqlstore.OpenSQLite(cfg.DBPath)
	if err != nil {
		return err
	}
	if err := sqlstore.Migrate(ctx, db); err != nil {
		db.Close()
		return fmt.Errorf("migrate: %w", err)
	}
	store := sqlstore.New(db)
	defer store.Close()

	var clock port.Clock = port.SystemClock{}
	registry := buildRegistry(cfg, log)

	pool := worker.NewPool(
		usecase.NewProcessJob(store, registry, clock, log, cfg.WorkerID, cfg.LeaseDuration),
		cfg.Lanes.All(),
		cfg.PollInterval,
		log,
	)
	reaper := worker.NewReaper(
		usecase.NewReapExpired(store, clock, log, 100),
		cfg.ReapInterval,
		log,
	)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); pool.Run(ctx) }()
	go func() { defer wg.Done(); reaper.Run(ctx) }()

	<-ctx.Done()
	log.Info("shutdown signal received, waiting for in-flight jobs")
	wg.Wait()
	return nil
}

// buildRegistry 把任务类型映射到执行器。新增一种任务 = 在这里加一行。
func buildRegistry(cfg *config.Config, log *slog.Logger) *executor.Registry {
	reg := executor.NewRegistry()

	if cfg.InferenceEndpoint != "" {
		// 真实部署：GPU 推理交给独立的 Python 服务，Go 只负责调度。
		reg.Register("ai.analyze", executor.NewInference(cfg.InferenceEndpoint, cfg.InferenceTimeout))
	} else {
		// 未配置推理服务时用模拟执行器，便于先打通调度全链路。
		log.Warn("SCHED_INFERENCE_ENDPOINT is empty, registering demo executor for ai.analyze")
		reg.Register("ai.analyze", executor.NewDemo(2*time.Second, 0.2))
	}

	// 其余任务类型的占位，接真实实现时替换。
	reg.Register("thumbnail.generate", executor.NewDemo(300*time.Millisecond, 0))
	reg.Register("metadata.extract", executor.NewDemo(100*time.Millisecond, 0))

	log.Info("executors registered", "kinds", reg.Kinds())
	return reg
}
