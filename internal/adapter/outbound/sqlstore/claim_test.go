package sqlstore_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/prohan-git/CleanArchitectureServe/internal/adapter/outbound/sqlstore"
	"github.com/prohan-git/CleanArchitectureServe/internal/domain/scheduling"
	"github.com/prohan-git/CleanArchitectureServe/internal/usecase/port"
)

func newTestStore(t *testing.T) *sqlstore.Store {
	t.Helper()
	db, err := sqlstore.OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := sqlstore.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store := sqlstore.New(db)
	t.Cleanup(func() { store.Close() })
	return store
}

func seedBatch(t *testing.T, store *sqlstore.Store, lane string, n int) string {
	t.Helper()
	now := time.Now().UTC()
	batch, err := scheduling.NewBatch("b1", "seed",
		scheduling.Trigger{Kind: scheduling.TriggerAdmin, Subject: "admin-1"},
		scheduling.Scope{Kind: "explicit", Ref: "x"}, now)
	if err != nil {
		t.Fatalf("new batch: %v", err)
	}
	batch.Total = n

	jobs := make([]*scheduling.Job, 0, n)
	for i := 0; i < n; i++ {
		j, err := scheduling.NewJob(
			"job-"+string(rune('a'+i)), batch.ID, lane, "ai.analyze", "target-"+string(rune('a'+i)),
			json.RawMessage(`{}`), scheduling.JobOptions{}, now)
		if err != nil {
			t.Fatalf("new job: %v", err)
		}
		jobs = append(jobs, j)
	}

	err = store.WithTx(context.Background(), func(r port.Repos) error {
		if err := r.Batches().Create(context.Background(), batch); err != nil {
			return err
		}
		_, err := r.Jobs().InsertMany(context.Background(), jobs)
		return err
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return batch.ID
}

// TestClaimRespectsSerialLane 是本项目最重要的一个测试。
//
// 它验证：20 个 goroutine 同时抢一条 capacity=1 的车道（CT 机 / GPU 独占），
// 有且只有一个能拿到任务。这正是"资源竞争"问题的答案——
// 而实现它用到的锁数量是零，全部由数据库的原子性兜底。
func TestClaimRespectsSerialLane(t *testing.T) {
	store := newTestStore(t)
	seedBatch(t, store, "gpu", 10)

	ctx := context.Background()
	now := time.Now().UTC()

	const contenders = 20
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex // 仅用于测试断言时汇总结果，被测代码本身不加锁
		claimed []string
	)
	wg.Add(contenders)
	for i := 0; i < contenders; i++ {
		go func(n int) {
			defer wg.Done()
			job, err := store.Repos().Jobs().Claim(ctx, "gpu", 1, "worker-"+string(rune('A'+n)), time.Minute, now)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			if job != nil {
				mu.Lock()
				claimed = append(claimed, job.ID)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if len(claimed) != 1 {
		t.Fatalf("serial lane must hand out exactly 1 job, got %d: %v", len(claimed), claimed)
	}
}

// TestClaimRespectsPoolCapacity 验证并发池车道不会超发。
func TestClaimRespectsPoolCapacity(t *testing.T) {
	store := newTestStore(t)
	seedBatch(t, store, "cpu", 10)

	ctx := context.Background()
	now := time.Now().UTC()
	const capacity = 4

	got := 0
	for i := 0; i < 10; i++ {
		job, err := store.Repos().Jobs().Claim(ctx, "cpu", capacity, "w", time.Minute, now)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if job == nil {
			break
		}
		got++
	}
	if got != capacity {
		t.Fatalf("pool lane with capacity %d handed out %d jobs", capacity, got)
	}
}

// TestExpiredLeaseDoesNotBlockLane 验证被 kill -9 的 worker 不会永久堵死车道。
// 没有这个保证，一次崩溃就要人工去数据库改状态。
func TestExpiredLeaseDoesNotBlockLane(t *testing.T) {
	store := newTestStore(t)
	seedBatch(t, store, "gpu", 3)
	ctx := context.Background()

	now := time.Now().UTC()

	// 一个 worker 领走任务后立刻"崩溃"：它再也不会提交结果，租约只剩 1 分钟。
	first, err := store.Repos().Jobs().Claim(ctx, "gpu", 1, "dead-worker", time.Minute, now)
	if err != nil || first == nil {
		t.Fatalf("first claim: job=%v err=%v", first, err)
	}

	// 两小时后租约早已过期，车道应当重新可用——哪怕 reaper 还没来得及跑。
	later := now.Add(2 * time.Hour)
	second, err := store.Repos().Jobs().Claim(ctx, "gpu", 1, "live-worker", time.Minute, later)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if second == nil {
		t.Fatal("expired lease still blocks the serial lane")
	}
}

// TestIdempotencyKeyPreventsDuplicates 验证重复提交不会产生重复任务。
func TestIdempotencyKeyPreventsDuplicates(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	batch, _ := scheduling.NewBatch("b-idem", "t",
		scheduling.Trigger{Kind: scheduling.TriggerSchedule, Subject: "nightly"},
		scheduling.Scope{Kind: "library"}, now)
	mk := func(id string) *scheduling.Job {
		j, err := scheduling.NewJob(id, batch.ID, "cpu", "ai.analyze", "photo-1",
			json.RawMessage(`{}`), scheduling.JobOptions{IdempotencyKey: "nightly:ai.analyze:photo-1"}, now)
		if err != nil {
			t.Fatalf("new job: %v", err)
		}
		return j
	}

	var inserted int
	err := store.WithTx(ctx, func(r port.Repos) error {
		if err := r.Batches().Create(ctx, batch); err != nil {
			return err
		}
		n, err := r.Jobs().InsertMany(ctx, []*scheduling.Job{mk("j1"), mk("j2"), mk("j3")})
		inserted = n
		return err
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("idempotency key should collapse 3 identical jobs into 1, got %d", inserted)
	}
}

// TestListCompletableOnlyReturnsSettledBatches 锁定批的收尾逻辑。
//
// 这里曾经有个缺口：BatchDone 这个状态被定义了，却没有任何代码设置它，
// 于是批永远停在 open——任务全跑完了，任务中心还显示"进行中"，
// 按 state=done 筛选永远是空的。
func TestListCompletableOnlyReturnsSettledBatches(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	batchID := seedBatch(t, store, "cpu", 3)

	// 任务还在排队时，批不该被收尾。
	got, err := store.Repos().Batches().ListCompletable(ctx, 10)
	if err != nil {
		t.Fatalf("list completable: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("batch with pending jobs must not be completable, got %d", len(got))
	}

	// 领走一个任务（running），仍然不该被收尾。
	now := time.Now().UTC()
	if _, err := store.Repos().Jobs().Claim(ctx, "cpu", 4, "w1", time.Minute, now); err != nil {
		t.Fatalf("claim: %v", err)
	}
	got, err = store.Repos().Batches().ListCompletable(ctx, 10)
	if err != nil {
		t.Fatalf("list completable: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("batch with a running job must not be completable, got %d", len(got))
	}

	// 把所有任务推进终态，批才应当可收尾。
	jobs, err := store.Repos().Jobs().ListByBatch(ctx, batchID, 100, 0)
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	for _, j := range jobs {
		j.State = scheduling.JobSucceeded
		j.LeaseOwner = ""
		j.UpdatedAt = now
		if err := store.Repos().Jobs().Update(ctx, j); err != nil {
			t.Fatalf("update job: %v", err)
		}
	}

	got, err = store.Repos().Batches().ListCompletable(ctx, 10)
	if err != nil {
		t.Fatalf("list completable: %v", err)
	}
	if len(got) != 1 || got[0].ID != batchID {
		t.Fatalf("settled batch must be completable, got %d batches", len(got))
	}

	// 收尾之后不该再被反复挑出来。
	if err := got[0].Complete(now); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := store.Repos().Batches().Update(ctx, got[0]); err != nil {
		t.Fatalf("update batch: %v", err)
	}
	got, err = store.Repos().Batches().ListCompletable(ctx, 10)
	if err != nil {
		t.Fatalf("list completable: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("already-done batch must not be listed again, got %d", len(got))
	}
}
