-- 调度内核的事实源。
--
-- 时间一律存 Unix 毫秒（INTEGER），不用各家方言的 timestamp 类型。
-- 这样 SQLite 与 PostgreSQL 的建表语句和扫描代码完全一致，
-- 将来换库时 sqlstore 包几乎不用改——这是"为迁移预留"的具体做法，
-- 代价是牺牲一点直接 SQL 查询时的可读性。

CREATE TABLE IF NOT EXISTS batches (
    id              TEXT    PRIMARY KEY,
    title           TEXT    NOT NULL,
    trigger_kind    TEXT    NOT NULL,   -- user / admin / event / schedule
    trigger_subject TEXT    NOT NULL,
    scope_kind      TEXT    NOT NULL,   -- self / album / class / library ...
    scope_ref       TEXT    NOT NULL,
    state           TEXT    NOT NULL,   -- open / done / canceled
    total           INTEGER NOT NULL DEFAULT 0,
    -- 整单幂等键：重复提交同一个 key 直接返回已有的批，不再新建一张空批。
    idempotency_key TEXT    NOT NULL DEFAULT '',
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

-- 支撑任务中心的"按触发方筛选"。
CREATE INDEX IF NOT EXISTS idx_batches_trigger ON batches (trigger_kind, trigger_subject, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_batches_state   ON batches (state, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_batches_idempotency
    ON batches (idempotency_key) WHERE idempotency_key <> '';

CREATE TABLE IF NOT EXISTS jobs (
    id               TEXT    PRIMARY KEY,
    batch_id         TEXT    NOT NULL REFERENCES batches (id),
    lane             TEXT    NOT NULL,
    kind             TEXT    NOT NULL,
    target_ref       TEXT    NOT NULL,
    payload          TEXT    NOT NULL DEFAULT '',
    idempotency_key  TEXT    NOT NULL DEFAULT '',
    state            TEXT    NOT NULL,  -- pending / running / succeeded / dead / canceled
    priority         INTEGER NOT NULL DEFAULT 0,
    attempt          INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 3,
    available_at     INTEGER NOT NULL,
    lease_owner      TEXT    NOT NULL DEFAULT '',
    lease_expires_at INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT    NOT NULL DEFAULT '',
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL
);

-- 领取任务的热路径索引：按车道找到最该跑的那一个。
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs (lane, state, priority DESC, available_at);
-- 车道在途计数与 reaper 扫描。
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs (state, lease_expires_at);
-- 批进度聚合与明细列表。
CREATE INDEX IF NOT EXISTS idx_jobs_batch ON jobs (batch_id, state);

-- 幂等：同一语义的任务只入队一次。空串表示本次提交不参与去重，
-- 因此用部分索引把空串排除在唯一约束之外。
CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_idempotency
    ON jobs (idempotency_key) WHERE idempotency_key <> '';

-- 只追加的审计表。jobs 回答"现在什么状态"，它回答"怎么走到这一步"。
CREATE TABLE IF NOT EXISTS job_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id   TEXT    NOT NULL,
    job_id     TEXT    NOT NULL DEFAULT '',
    type       TEXT    NOT NULL,
    from_state TEXT    NOT NULL DEFAULT '',
    to_state   TEXT    NOT NULL DEFAULT '',
    actor      TEXT    NOT NULL DEFAULT '',
    detail     TEXT    NOT NULL DEFAULT '',
    at         INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_batch ON job_events (batch_id, id);
CREATE INDEX IF NOT EXISTS idx_events_job   ON job_events (job_id, id);
