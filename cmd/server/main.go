// Command server 是控制面：接受调度单的提交、查询与取消。
//
// 它不执行任何重活。AI 分析、缩略图这些在 worker 进程里跑，
// 因此 GPU 任务把 worker 拖垮时，这个进程照常响应"进度查询"和"取消"。
// 这是把在线路径和离线路径分成两个进程的全部理由。
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	_ "modernc.org/sqlite" // 纯 Go 的 SQLite 驱动，无需 cgo，保证单二进制可交叉编译

	"github.com/prohan-git/CleanArchitectureServe/internal/adapter/inbound/httpapi"
	"github.com/prohan-git/CleanArchitectureServe/internal/adapter/outbound/expander"
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
	log := observability.NewLogger("server")

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := apprt.SignalContext()
	defer stop()

	db, err := openDB(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	store := sqlstore.New(db)
	defer store.Close()

	// 依赖装配集中在 main：这是整个程序里唯一知道"接口由谁实现"的地方。
	// 换存储、换推理服务、换鉴权，改的都是这几行，用例与领域层一行不动。
	var (
		clock   port.Clock       = port.SystemClock{}
		ids     port.IDGenerator = apprt.IDGen{}
		targets                  = expander.NewStatic()
	)

	srv := httpapi.NewServer(
		usecase.NewSubmitBatch(store, targets, cfg.Lanes, clock, ids),
		usecase.NewQueryBatch(store),
		usecase.NewCancelBatch(store, clock),
		log,
	)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr, "db", cfg.DBPath)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		log.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown http server: %w", err)
	}
	return nil
}

func openDB(ctx context.Context, path string) (*sql.DB, error) {
	if err := os.MkdirAll(dirOf(path), 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	db, err := sqlstore.OpenSQLite(path)
	if err != nil {
		return nil, err
	}
	if err := sqlstore.Migrate(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
