package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/prohan-git/CleanArchitectureServe/internal/adapter/inbound/worker"
	"github.com/prohan-git/CleanArchitectureServe/internal/adapter/outbound/executor"
	"github.com/prohan-git/CleanArchitectureServe/internal/adapter/outbound/sqlstore"
	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

// peakRecorder 是记录实际并发峰值的执行器，用来验证车道容量真的兑现成了吞吐。
type peakRecorder struct {
	mu       sync.Mutex
	inflight int
	peak     int
	work     time.Duration
}

func (p *peakRecorder) Execute(ctx context.Context, t port.Task) (port.Result, error) {
	p.mu.Lock()
	p.inflight++
	if p.inflight > p.peak {
		p.peak = p.inflight
	}
	p.mu.Unlock()

	select {
	case <-time.After(p.work):
	case <-ctx.Done():
	}

	p.mu.Lock()
	p.inflight--
	p.mu.Unlock()
	return port.Result{Summary: "ok"}, nil
}

func (p *peakRecorder) Peak() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// TestPoolReachesLaneCapacity 锁定一个曾经出现过的缺陷：
//
// 数据库里的 COUNT(*) < capacity 只是上限保证，它能拦住超发，但制造不出并发。
// 曾经每条车道只起一个取号 goroutine，串行地 claim→执行→claim，
// 于是 capacity 配多大，实际在途都只有 1——容量形同虚设。
//
// 本测试同时验证两侧：pool 车道要真的跑满容量，serial 车道要真的只跑一个。
func TestPoolReachesLaneCapacity(t *testing.T) {
	cases := []struct {
		name     string
		kind     scheduling.LaneKind
		capacity int
		wantPeak int
	}{
		{"pool lane reaches capacity", scheduling.LanePool, 4, 4},
		{"serial lane stays exclusive", scheduling.LaneSerial, 1, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore(t)
			lane, err := scheduling.NewLane("test-lane", tc.kind, tc.capacity, 0)
			if err != nil {
				t.Fatalf("new lane: %v", err)
			}
			seed(t, store, lane.Name, 24)

			rec := &peakRecorder{work: 60 * time.Millisecond}
			reg := executor.NewRegistry().Register("work", rec)
			log := slog.New(slog.NewTextHandler(io.Discard, nil))

			pool := worker.NewPool(
				usecase.NewProcessJob(store, reg, port.SystemClock{}, log, "w1", time.Minute),
				[]scheduling.Lane{lane},
				5*time.Millisecond,
				log,
			)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			done := make(chan struct{})
			go func() { pool.Run(ctx); close(done) }()

			waitUntilSettled(t, store, 24)
			cancel()
			<-done

			if got := rec.Peak(); got != tc.wantPeak {
				t.Fatalf("lane %s(capacity=%d): peak concurrency = %d, want %d",
					tc.kind, tc.capacity, got, tc.wantPeak)
			}
		})
	}
}

func newStore(t *testing.T) *sqlstore.Store {
	t.Helper()
	db, err := sqlstore.OpenSQLite(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := sqlstore.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := sqlstore.New(db)
	t.Cleanup(func() { s.Close() })
	return s
}

func seed(t *testing.T, store *sqlstore.Store, lane string, n int) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()

	b, err := scheduling.NewBatch("b1", "t",
		scheduling.Trigger{Kind: scheduling.TriggerAdmin, Subject: "a"},
		scheduling.Scope{Kind: "explicit", Ref: "x"}, now)
	if err != nil {
		t.Fatalf("new batch: %v", err)
	}
	b.Total = n

	jobs := make([]*scheduling.Job, 0, n)
	for i := 0; i < n; i++ {
		j, err := scheduling.NewJob(
			"j"+itoa(i), b.ID, lane, "work", "target"+itoa(i),
			json.RawMessage(`{}`), scheduling.JobOptions{}, now)
		if err != nil {
			t.Fatalf("new job: %v", err)
		}
		jobs = append(jobs, j)
	}

	if err := store.WithTx(ctx, func(r port.Repos) error {
		if err := r.Batches().Create(ctx, b); err != nil {
			return err
		}
		_, err := r.Jobs().InsertMany(ctx, jobs)
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func waitUntilSettled(t *testing.T, store *sqlstore.Store, total int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p, err := store.Repos().Batches().Progress(context.Background(), "b1")
		if err != nil {
			t.Fatalf("progress: %v", err)
		}
		if p.Settled() == total {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("jobs did not settle in time")
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
