# CleanArchitectureServe

**自托管照片库的后台任务调度内核。**

如果你在做 Immich 这类东西 —— 用户传照片进来，后台要跑元数据提取、缩略图、
视频转码、语义搜索向量、人脸检测 —— 那这个项目解决的就是你那堆后台任务的调度问题：

- 一次导入 5 万张照片，任务要排队跑完，**不能把机器打死**
- 显卡只有一张，ML 任务必须**串行**；缩略图是 CPU 活，可以**并行**
- 转码跑到一半进程挂了，重启后**不能丢，也不能重复跑**
- 点了「重新生成全部缩略图」，要能**看进度、能中途取消**
- 某张照片一直处理失败，要能查出**它失败了几次、报的什么错**

核心设计只有一条：**任务状态的唯一真相在数据库里。**
带来的直接好处是 —— 调度代码里**没有一把锁**，
「谁能跑、跑了几个、轮到谁」全部由一条 SQL 的原子性裁决。

---

## 目录

- [5 分钟跑起来](#5-分钟跑起来)
- [核心概念（3 个，必读）](#核心概念3-个必读)
- [照片库的典型配置](#照片库的典型配置)
- [任务链：上传之后发生什么](#任务链上传之后发生什么)
- [接入你自己的业务](#接入你自己的业务)
- [API 参考](#api-参考)
- [配置参考](#配置参考)
- [部署与运维](#部署与运维)
- [常见问题](#常见问题)
- [架构原理](#架构原理)

---

## 5 分钟跑起来

需要 Go 1.22+。**不需要 Redis，不需要数据库服务，不需要 cgo。**

```bash
git clone <this-repo> && cd CleanArchitectureServe
make build
```

开两个终端：

```bash
# 终端 1 —— 控制面（收任务、查进度、取消）
./bin/server

# 终端 2 —— 数据面（真正干活的）
./bin/worker
```

> 没接业务时，worker 用内置 demo 执行器（假装干 2 秒，20% 概率失败），
> 方便你先看清整条链路。

给三张照片排队生成缩略图：

```bash
curl -X POST localhost:8080/api/v1/batches \
  -H 'Content-Type: application/json' \
  -H 'X-Actor-Kind: admin' -H 'X-Actor-Subject: admin-1' \
  -d '{
        "title": "重建缩略图",
        "job_kind": "thumbnail.generate",
        "lane": "cpu",
        "scope_kind": "explicit",
        "scope_ref": "asset-1,asset-2,asset-3"
      }'
```

```json
{"batch_id":"01a0b3...","total":3,"enqueued":3,"duplicate":false}
```

查进度：

```bash
curl localhost:8080/api/v1/batches/01a0b3...
```

```json
{
  "id": "01a0b3...", "title": "重建缩略图", "state": "open", "total": 3,
  "progress": {"total":3,"pending":1,"running":1,"succeeded":1,"dead":0,"canceled":0}
}
```

看它一步步是怎么跑过来的（**排查问题全靠这个**）：

```bash
curl localhost:8080/api/v1/batches/01a0b3.../events
```

```
batch.created                  -> open       kind=thumbnail.generate lane=cpu targets=3
job.claimed    pending -> running    attempt=1 lease_until=...
job.succeeded  running -> succeeded  asset-1 ok
job.claimed    pending -> running    attempt=1
job.failed     running -> pending    decode error on asset-2 (attempt 1)
job.claimed    pending -> running    attempt=2      ← 自动重试了
job.succeeded  running -> succeeded  asset-2 ok
```

到这儿你已经看懂这个系统了。

---

## 核心概念（3 个，必读）

### Batch（批）—— 一次派发

「重新生成全部缩略图」「给这个相册跑人脸检测」就是一个 Batch。
提交一次拿到一个 `batch_id`，之后查进度、取消都用它。

> **这是和常见队列方案（BullMQ / Celery）最大的区别。**
> 队列方案通常只有「这个队列还剩 N 个任务」，没有「这一次重建」的概念。
> 于是你点了"重建全部缩略图"之后：进度看不了、中途停不下来、
> 上次是谁点的什么时候点的也查不到。Batch 就是补这三个洞的。

### Job（任务）—— 批里的一个

Batch 展开成 N 个 Job，每个对应一个具体对象（一张照片、一个视频）。
Job 有完整生命周期和自动重试：

```
pending ──领取──► running ──成功──► succeeded
   ▲                 │
   │                 ├──失败，还有次数──► pending（退避后重试）
   │                 ├──失败，次数用尽──► dead（死信，留现场等人工）
   └──进程崩溃，租约过期──┘
```

### Lane（车道）—— **被争抢的那个东西**

这是最容易配错、也最关键的概念。

> **车道建模的不是「任务种类」，是「争用」。**

问自己一句话就能配对：

> **如果我同时发 100 个过去，会发生什么坏事？**

| 会发生的坏事 | 车道类型 | 照片库里的例子 |
|---|---|---|
| 物理上只能进一个 | `serial` | 本机独占那张显卡跑 CLIP / 人脸检测 |
| 机器会被打死 | `pool` + capacity | ffmpeg 转码，配 1~2 |
| 对方排队超时 / OOM | `pool` + capacity | 远程推理 API 只扛得住 3 并发 |
| 对方 429 / 按量计费 | `rate_limited` + rate | 调第三方地理编码 API |
| 什么也不会发生 | `pool` + 大 capacity | 读 EXIF 这种纯 IO |

**Lane 和 job_kind 是正交的两件事：**

- `lane` = **在哪排队**（争用什么资源）
- `job_kind` = **干什么活**（谁来执行）

所以「人脸检测和 CLIP 向量共用同一张显卡」= 两个不同的 `job_kind`，
**同一条 `ml` 车道**。它们会自动互相排队，不会同时抢显存。这是配置，不是代码。

---

## 照片库的典型配置

### 任务类型与车道怎么分

| `job_kind` | 干什么 | 真正的瓶颈 | 建议车道 |
|---|---|---|---|
| `metadata.extract` | 读 EXIF、拍摄时间、GPS | 磁盘 IO | `io` pool 8 |
| `thumbnail.generate` | 预览图 / 缩略图 | CPU（libvips） | `cpu` pool = 核数 |
| `video.transcode` | 转码 | CPU/GPU 极重 | `transcode` pool 1~2 |
| `ml.clip` | 语义搜索向量 | 显存 | `ml` serial |
| `ml.face.detect` | 人脸检测 | 显存 | `ml` serial（**和上面共用**） |
| `ml.face.cluster` | 人脸聚类归人 | CPU，需批量 | `cpu` pool |
| `dedupe.detect` | 重复照片检测 | CPU，依赖向量 | `cpu` pool |
| `storage.migrate` | 按模板移动文件 | 磁盘 IO | `io` **小容量** |

注意三个要点：

1. **`ml.clip` 和 `ml.face.detect` 共用一条 `ml` 车道** —— 因为它们抢的是同一张显卡。
   分成两条车道就会同时跑，然后一起 OOM。
2. **`video.transcode` 单独一条车道** —— ffmpeg 吃满所有核。
   和缩略图共用 `cpu` 车道的话，转一个 4K 视频能让缩略图饿死半小时。
3. **`storage.migrate` 要小容量** —— 它是大文件搬运，并发高了磁盘会被打满，
   反而拖慢所有其它任务。

### 完整配置示例

```bash
export SCHED_LANES='[
  {"name":"io",        "kind":"pool",   "capacity":8},
  {"name":"cpu",       "kind":"pool",   "capacity":4},
  {"name":"transcode", "kind":"pool",   "capacity":1},
  {"name":"ml",        "kind":"serial"}
]'
```

`cpu` 的 capacity 按你的核数调；如果 ML 推理是远程 API 而不是本机显卡，
把 `ml` 改成 `{"name":"ml","kind":"pool","capacity":3}`，容量填对方扛得住的并发。

### 两种规模的派发，同一套模型

| 场景 | 怎么提交 | Batch 大小 |
|---|---|---|
| 用户传了 1 张照片 | `scope_kind: explicit`，1 个 ID | 1 个 job |
| 用户导入一个文件夹 | `scope_kind: explicit`，一串 ID | 几百 |
| 管理员点「重建全部缩略图」 | `scope_kind: library` | 几万 |
| 某个相册重跑人脸检测 | `scope_kind: album` | 几百 |

全部走同一个接口、同一个状态机。差别只在 `scope_kind`。

---

## 任务链：上传之后发生什么

照片库的后台任务**是有先后顺序的**，这是它和「一批独立任务」最大的不同：

```
上传完成
   └─► metadata.extract          先读出拍摄时间、朝向、GPS
         ├─► thumbnail.generate  （要用到朝向信息才能转正）
         ├─► video.transcode     （仅视频）
         ├─► ml.clip ──────────► dedupe.detect   （去重要用向量）
         └─► ml.face.detect ───► ml.face.cluster （聚类要先有人脸）
```

### 现状：框架不内置 DAG，用链式触发解决

**说清楚边界：本项目没有 `depends_on` 字段，不做依赖图调度。**
照片库的依赖是「A 完成后触发 B」这种线性链，用链式触发就够了，
不需要引入 DAG 的复杂度。

做法：在上游任务的执行器里，完成后提交下游 Batch。

```go
reg.Register("metadata.extract", executor.Func(
    func(ctx context.Context, t port.Task) (port.Result, error) {
        meta, err := extractEXIF(ctx, t.TargetRef)
        if err != nil {
            return port.Result{}, err     // 返回 error 就会自动重试
        }
        saveToBusinessDB(t.TargetRef, meta)

        // 元数据好了，触发下游。trigger_kind 用 event，
        // 任务中心里就能一眼看出这批是系统自动触发的，不是人点的。
        for _, next := range []string{"thumbnail.generate", "ml.clip", "ml.face.detect"} {
            if err := enqueue(ctx, next, laneOf(next), t.TargetRef); err != nil {
                // 下游入队失败不该让上游重跑一遍 EXIF，
                // 记下来让对账任务补即可。
                log.Warn("enqueue downstream failed", "kind", next, "err", err)
            }
        }
        return port.Result{Summary: "metadata ok"}, nil
    }))
```

`enqueue` 就是往 `POST /api/v1/batches` 发一个请求，带上：

```json
{
  "title": "asset-123 缩略图",
  "job_kind": "thumbnail.generate",
  "lane": "cpu",
  "scope_kind": "explicit",
  "scope_ref": "asset-123",
  "idempotency_key": "asset-123:thumbnail.generate:v1"
}
```

**`idempotency_key` 在这里是刚需**：上游任务重试时会再走一遍这段代码，
有这个 key，下游不会被重复入队。

### 什么时候链式触发不够用

如果你出现了**「等多个上游都完成才能开始」**的需求
（比如人脸聚类要等全库人脸检测都跑完），链式触发就不合适了。

那种情况有两条路：

1. **用 Batch 完成事件驱动** —— 轮询上游 Batch 的 `progress.IsComplete()`，
   完成后提交下游。简单，够用。
2. **真上依赖字段** —— 给 `jobs` 加 `depends_on`，`Claim` 时校验前置是否完成。
   改动不大（一个字段 + Claim 的 WHERE 多一个条件），但会让模型复杂一档。

我建议**先用第 1 种**，等真的被卡到了再上第 2 种。

---

## 接入你自己的业务

三步。前两步基本是配置，第三步是唯一需要动脑子的。

### Step 1 · 配置车道

见上面[照片库的典型配置](#照片库的典型配置)，改环境变量即可，不用动代码。

### Step 2 · 接上你的业务执行

**情况 A：业务已经是 HTTP 接口（推荐）**

如果你的 ML 推理已经是独立服务（Immich 就是这么拆的），那你**一行代码不用写**：

```bash
export SCHED_INFERENCE_ENDPOINT=http://your-ml-service:3003/predict
```

你的接口只需接受：

```json
{"job_id":"01a0...","kind":"ml.clip","target_ref":"asset-123","payload":{}}
```

返回：

```json
{"summary":"embedded asset-123"}
```

失败约定：

| 你的情况 | 你该返回 | 调度器的反应 |
|---|---|---|
| 临时故障（显存不足、下游超时） | 非 200 | 指数退避后**自动重试** |
| 数据本身坏了（文件损坏） | 200 + `{"error":"文件损坏"}` | 记为失败，原因留在审计里 |

> ⚠️ **不要在你的接口里自己实现重试。** 重试是调度器的职责。
> 两边都重试，实际执行次数会变成**乘积**。

`examples/inference-service/app.py` 是可直接运行的 Python 示例。

**情况 B：业务逻辑用 Go 写**

在 `cmd/worker/main.go` 的 `buildRegistry` 里加一行：

```go
reg.Register("thumbnail.generate", executor.Func(
    func(ctx context.Context, t port.Task) (port.Result, error) {
        // t.TargetRef 是你的业务对象 ID（asset ID）
        if err := makeThumbnail(ctx, t.TargetRef); err != nil {
            return port.Result{}, err   // 返回 error 就会走重试
        }
        return port.Result{Summary: "thumbnail ok"}, nil
    }))
```

> 这段代码有测试盯着（`executor/example_test.go`），改了接口它会先红，
> 保证文档不会腐烂。

**业务产出（缩略图路径、向量、人脸框）请写进你自己的业务库。**
调度库只存调度状态，两边靠 asset ID 关联。

### Step 3 · 告诉系统「这一批」包含哪些对象

唯一需要你写业务代码的地方：调度器不认识「相册」「全库」，
得你告诉它「这个相册里有哪 800 张照片」。

打开 `internal/adapter/outbound/expander/static.go`，换成查你业务库的实现：

```go
func (e *SQLExpander) Expand(ctx context.Context, scope scheduling.Scope,
    trigger scheduling.Trigger) ([]port.Target, error) {

    switch scope.Kind {
    case "album":
        rows, err := e.bizDB.QueryContext(ctx,
            `SELECT id FROM assets WHERE album_id = ?`, scope.Ref)
        // ... 扫描成 []port.Target{{Ref: assetID}}

    case "library":   // 全库重建
        // SELECT id FROM assets WHERE deleted_at IS NULL

    case "missing":   // 只补缺失的，重建时最常用
        // SELECT id FROM assets WHERE thumbnail_path IS NULL

    case "self":      // 终端用户只能处理自己的
        // ⚠️ 用 trigger.Subject，绝不要信客户端传来的 scope.Ref
        return []port.Target{{Ref: trigger.Subject}}, nil
    }
}
```

> 强烈建议实现 `missing` 这个范围。「重建全部缩略图」通常真实意图是
> 「把缺的补上」，几万张里可能只有几百张真的缺。

**暂时不想写？** 用现成的 `explicit` 直接传 ID 列表先跑通：

```json
{"scope_kind": "explicit", "scope_ref": "asset-1,asset-2,asset-3"}
```

---

## API 参考

所有请求带身份头。**身份决定能调度什么范围**，不能由客户端随便声明：

```
X-Actor-Kind:    user | admin | event | schedule
X-Actor-Subject: 用户ID / 管理员ID / 事件名 / cron任务名
```

> `X-Actor-Kind: user` 只允许 `scope_kind: self`，服务端强制拦截越权。
> 接真实鉴权时改 `internal/adapter/inbound/httpapi/identity.go` 一处即可。

### 提交一批任务

```
POST /api/v1/batches
```

| 字段 | 必填 | 说明 |
|---|---|---|
| `title` | ✅ | 给人看的名字，显示在任务中心 |
| `job_kind` | ✅ | 干什么活，决定由哪个 executor 执行 |
| `lane` | ✅ | 在哪条车道排队 |
| `scope_kind` | ✅ | 范围类型：`explicit` / `self` / 你自己定义的 |
| `scope_ref` | | 范围的具体值（相册 ID、ID 列表） |
| `priority` | | 越大越优先，默认 0 |
| `max_retries` | | 最大尝试次数，默认 3 |
| `idempotency_key` | | **强烈建议填**，见下 |

```bash
curl -X POST localhost:8080/api/v1/batches \
  -H 'Content-Type: application/json' \
  -H 'X-Actor-Kind: admin' -H 'X-Actor-Subject: admin-1' \
  -d '{"title":"全库人脸检测","job_kind":"ml.face.detect","lane":"ml",
       "scope_kind":"library","idempotency_key":"face-detect-full-20260918"}'
```

```json
{"batch_id":"01a0b3...","total":51203,"enqueued":51203,"duplicate":false}
```

**关于 `idempotency_key`：** 填了它，同一个 key 重复提交直接返回**原来那个批**
（`duplicate: true`，`enqueued: 0`），不会重复跑。
用户连点两次、上游任务重试、网络重试，全靠它兜底。

建议格式：`<业务含义>-<时间窗>`，例如 `face-detect-full-20260918`、
`asset-123:thumbnail:v1`。

### 查进度

```
GET /api/v1/batches/{id}
```

```json
{
  "id": "01a0b3...", "title": "全库人脸检测", "state": "open",
  "trigger_kind": "admin", "trigger_subject": "admin-1", "total": 51203,
  "progress": {"total":51203,"pending":48910,"running":1,"succeeded":2290,"dead":2,"canceled":0}
}
```

**批的状态（`state`）：**

| 状态 | 含义 |
|---|---|
| `open` | 还有任务没跑完 |
| `done` | 所有任务都已收尾 |
| `canceled` | 整批被取消 |

> `done` 由后台收尾任务批量标记，有一个扫描周期（默认 30 秒）的延迟。
> **`progress` 本身始终是实时的** —— 要判断"是不是真跑完了"看 `progress`，
> 要做列表筛选用 `state`。

**任务的状态（`progress` 里的计数）：**

| 状态 | 含义 |
|---|---|
| `pending` | 排队中 |
| `running` | 正在跑 |
| `succeeded` | 成功 |
| `dead` | 重试用尽，**需要人工看一眼** |
| `canceled` | 被取消 |

### 查任务明细

```
GET /api/v1/batches/{id}/jobs?limit=50&offset=0
```

失败任务带 `last_error` 和 `attempt`，直接告诉你失败几次、为什么。

### 查审计轨迹（排查问题用这个）

```
GET /api/v1/batches/{id}/events?limit=200
```

只追加、不修改。状态字段会被覆盖，**但事件不会**。
生产上「这张照片为什么一直处理不了」只能靠它回答。

### 取消整批

```
POST /api/v1/batches/{id}/cancel
```

```json
{"canceled_jobs": 48911}
```

**立即生效**，不用等正在跑的任务结束。
正在转码的那个视频跑完后提交结果时，会发现自己已被取消，结果被丢弃。

### 列出批（任务中心）

```
GET /api/v1/batches?trigger_kind=admin&state=open&limit=50
```

四个筛选条件都可选。看「系统自动触发的」就传 `trigger_kind=event`。

### 健康检查

```
GET /healthz   →   {"status":"ok"}
```

---

## 配置参考

全部环境变量，不需要分发配置文件。

| 变量 | 默认值 | 说明 |
|---|---|---|
| `SCHED_DB_PATH` | `./data/scheduler.db` | 调度库路径，**与你的业务库是两个独立文件** |
| `SCHED_HTTP_ADDR` | `:8080` | 控制面监听地址 |
| `SCHED_LANES` | 见下 | 车道拓扑（JSON） |
| `SCHED_WORKER_ID` | `主机名-pid` | worker 标识，多实例时必须唯一 |
| `SCHED_POLL_INTERVAL` | `1s` | 车道空闲时的轮询间隔 |
| `SCHED_LEASE_DURATION` | `5m` | 租约时长，**必须显著大于最慢任务耗时** |
| `SCHED_REAP_INTERVAL` | `30s` | 回收崩溃任务的扫描周期 |
| `SCHED_INFERENCE_ENDPOINT` | 空 | 你的业务 API 地址；留空则用 demo 执行器 |
| `SCHED_INFERENCE_TIMEOUT` | `2m` | **必须小于 `LEASE_DURATION`**，启动时强制校验 |
| `SCHED_SHUTDOWN_TIMEOUT` | `15s` | 优雅退出等待时长 |

> ⚠️ **有 4K 视频转码的话，`SCHED_LEASE_DURATION` 一定要调大**（比如 `2h`）。
> 租约短于任务耗时 = 系统以为 worker 崩了 = 同一个视频被转两遍。

默认车道拓扑（照片库建议改成上面那套）：

```json
[
  {"name":"gpu","kind":"serial"},
  {"name":"cpu","kind":"pool","capacity":4},
  {"name":"io","kind":"pool","capacity":8},
  {"name":"api","kind":"rate_limited","capacity":2,"rate_per_s":5}
]
```

---

## 部署与运维

### 想跑更快？多起几个 worker

```bash
SCHED_WORKER_ID=w1 ./bin/worker &
SCHED_WORKER_ID=w2 ./bin/worker &
```

**不用改代码、不用协调、不用加锁。** 数据库保证不超发、不重复执行。

> ⚠️ 同机多进程共享一个 SQLite 文件可以（已开 WAL）。
> **跨机器必须先换 PostgreSQL。**

### 什么时候该换 PostgreSQL？

判据只有一个：**要不要跨机器部署 worker。**

自托管照片库基本都是单机/NAS，SQLite 完全够用，别急着上 PG。

真要换的时候，只动 `sqlstore` 包里 `Claim` 一处：

```sql
-- SQLite:     BEGIN IMMEDIATE + 条件 UPDATE
-- PostgreSQL: SELECT ... FOR UPDATE SKIP LOCKED
```

语义完全一致，其余代码不动。时间统一存 Unix 毫秒、SQL 全收敛在一个包内，
就是为这天准备的。

### 该盯什么

| 信号 | 含义 | 该做什么 |
|---|---|---|
| `dead` 在涨 | 重试用尽的真实失败 | 查 `/events` 看 `last_error` |
| `pending` 长期不降 | 容量不够，或 worker 没起来 | 调 capacity 或加 worker |
| 日志 `reclaimed expired jobs` | 有 worker 崩溃过 | 会自愈，但要查为什么崩 |
| 日志 `discarding result` | 任务被取消/回收后结果作废 | 正常，预期内的竞态处理 |
| 日志 `closed completed batches` | 批被标记为 done | 正常收尾 |

日志是 JSON 结构化的，可直接喂日志系统。

### 上生产前记得

- [ ] 删掉 `cmd/worker/main.go` 里的 demo 执行器注册
- [ ] 把 `identity.go` 的请求头鉴权换成真实的 JWT / session
- [ ] 把 `expander` 换成查你业务库的实现（至少实现 `library` 和 `missing`）
- [ ] 确认 `SCHED_LEASE_DURATION` > 最慢任务耗时（**有转码的话按小时算**）

---

## 常见问题

**Q：任务卡在 `running` 不动了？**
不用管，会自愈。worker 崩溃后租约到期，Reaper 自动把它放回队列重试。
这正是租约机制存在的理由。

**Q：转码任务被反复执行了好几遍？**
`SCHED_LEASE_DURATION` 配短了。租约必须大于最慢任务耗时，4K 转码请按小时配。

**Q：怎么让用户刚传的照片立刻开始处理？**
调小 `SCHED_POLL_INTERVAL`（比如 `200ms`）。
这是不引入 Redis 的**全部代价**：首个任务最多延迟一个轮询周期。
只影响启动延迟，不影响吞吐 —— 有活干时 worker 不会等待。

**Q：同一批提交两次会跑两遍吗？**
填了 `idempotency_key` 不会，直接返回原来那个批。不填会。**建议一直填。**

**Q：任务失败会自动重试吗？**
会。默认最多 3 次，指数退避（2s → 6s → 18s，封顶 10 分钟）。
用完还失败进 `dead`，**保留现场，不会静默消失**。

**Q：业务逻辑必须用 Go 写吗？**
不必。做成 HTTP 接口配个 `SCHED_INFERENCE_ENDPOINT` 就行，
Python / Node / TypeScript 都可以。Go 侧只是调度引擎。

**Q：能替代 Immich 的 BullMQ 吗？**
定位不同。最大区别是**任务状态的事实源在关系库，不在队列组件里**：
重启不丢状态、进度可 SQL 查询、审计天然完整、**批级别的进度与取消**。
代价是要自己实现领取和重试（已实现，有并发测试覆盖）。
详见 [`docs/ADR-0001`](docs/ADR-0001-scheduling-core.md)。

**Q：任务全跑完了，批的 `state` 还是 `open`？**
等最多 30 秒。批的收尾是后台批量做的（`SCHED_REAP_INTERVAL` 控制周期），
不在每个任务完成时顺手改 —— 否则一个五万任务的批要白白聚合五万次。
`progress` 是实时的，想立刻知道跑没跑完就看它。

**Q：上传接口本身也要走队列吗？**
**不要。** 上传、下载、看图这些在线请求应该由业务网关同步处理直接返回。
只有「耗时的、可以晚点做的、需要排队的」才进这套系统。

**Q：支持任务依赖 / DAG 吗？**
不内置。照片库的依赖是线性链，用[链式触发](#任务链上传之后发生什么)解决就够了。
真需要「等多个上游完成」时有两条升级路径，见那一节。

---

## 架构原理

```
                     ┌──────────────┐
 上传事件/管理员/定时 ──►│ cmd/server   │  控制面：提交、查询、取消
                     └──────┬───────┘  不执行任何重活
                            │ 写入
                     ┌──────▼───────────────────────┐
                     │  调度库（唯一事实源）           │
                     │  batches / jobs / job_events │
                     └──────▲───────────────────────┘
                            │ 原子 Claim（按车道领取）
                     ┌──────┴───────┐
                     │ cmd/worker   │  数据面：按车道消费
                     └──────┬───────┘  ml 串行 / cpu 并行 / io 并行
                            │ HTTP
                     ┌──────▼─────────────┐
                     │ 你的 ML 服务（任意语言）│  无状态，只管干活
                     └────────────────────┘
```

`server` 和 `worker` 是两个进程，**不共享任何内存状态**。
转码把 worker 拖垮时，控制面照常响应「查进度」和「取消」。

### 三条保证正确性的机制

| 机制 | 解决的问题 |
|---|---|
| 原子 `Claim`（一条 SQL 完成选任务 + 校验容量 + 打租约） | 资源竞争。**代码里零 `sync.Mutex`** |
| 租约 + Reaper | worker 被 `kill -9` 后任务不会永久卡死 |
| 只追加的 `job_events` | 出问题时能回答「它是怎么走到这一步的」 |

### 代码结构

依赖方向只能由外向内：`adapter → usecase → domain`。

```
cmd/
  server/              控制面入口；依赖装配
  worker/              数据面入口；★ buildRegistry 是你注册业务的地方
internal/
  domain/scheduling/   实体与状态机。零外部依赖，连 database/sql 都不 import
  usecase/             用例编排，只依赖 port 定义的接口
    port/              出站端口（依赖倒置）。换存储 = 换个实现，用例不动
  adapter/
    inbound/httpapi/   HTTP 入口（标准库 ServeMux，无路由框架）
    inbound/worker/    轮询驱动（与 httpapi 同层，只是被 ticker 驱动）
    outbound/sqlstore/ ★ job_repo.go 的 Claim 是全系统正确性核心
    outbound/executor/ ★ 你的业务执行器（inference.go 是 HTTP 客户端）
    outbound/expander/ ★ 你的范围展开逻辑（业务知识的唯一落点）
  platform/            配置、日志、ID 生成、优雅退出
migrations/            SQL 迁移（embed 进二进制，单文件部署）
examples/
  inference-service/   Python 业务服务示例（FastAPI）
```

标 ★ 的三处是你会接触到的，其余可当黑盒。

### 深入阅读

- [`docs/ADR-0001`](docs/ADR-0001-scheduling-core.md) —— 为什么选这个方案，
  为什么不用 Celery / BullMQ + Redis
- [`docs/ADR-0002`](docs/ADR-0002-simplification-budget.md) —— **哪些功能可以砍、哪些不能砍**

---

## 测试

```bash
make test    # 带 -race
```

| 测试 | 锁定的行为 |
|---|---|
| `sqlstore/claim_test.go` | 20 协程同抢 `serial` 车道只出一个 |
| | `pool` 车道不超发 |
| | 过期租约不会永久堵死车道 |
| | 相同幂等键的重复提交被折叠 |
| | 只有全部任务收尾的批才会被标记为 done |
| `worker/pool_test.go` | `pool` 车道实际并发峰值 = 配置容量 |
| | `serial` 车道实际并发峰值恒为 1 |
| `executor/example_test.go` | README 里的代码示例始终可编译 |

---

## 不要这样做

- ❌ 在上传接口里同步跑 ML，或开个协程跑但任务不落库
- ❌ 把转码和缩略图放同一条车道（一个 4K 视频能饿死缩略图半小时）
- ❌ 把 `ml.clip` 和 `ml.face.detect` 分成两条车道（它们抢同一张显卡）
- ❌ 在你的业务接口里自己实现重试（会和调度器的重试变成乘积）
- ❌ 租约时长小于最慢任务耗时（会导致同一个任务被执行多次）
