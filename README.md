# CleanArchitectureServe

一个以数据库为事实源的任务调度内核，按资源车道执行，批量派发、进度可查、过程留底。

适用场景：学校体检（CT 串行独占、血常规多人并行、按班/全校派发）、
照片库批量 AI 分析（GPU 独占）、缩略图生成、限流调用外部 API。

## 核心主张

**任务状态的唯一真相在关系库里。** 缓存、消息队列、result backend 都可以有，
但它们只能是这张表的派生物或加速层。一旦让它们成为唯一真相，
组件重启之后"这批任务跑到哪一步了"就永远答不上来。

由此推导出的三条设计：

| 机制 | 解决的问题 |
|---|---|
| 原子 `Claim`（一条 SQL 完成选任务 + 校验车道容量 + 打租约） | 资源竞争。**代码里没有任何 `sync.Mutex`** |
| 租约 + Reaper | worker 被 `kill -9` 后任务不会永久卡在 running |
| 只追加的 `job_events` 表 | 出问题时能回答"它是怎么走到这一步的" |

## 架构

```
                    ┌──────────────┐
  用户/管理员/事件/定时 ─►│ cmd/server   │  控制面：提交、查询、取消
                    └──────┬───────┘  不执行任何重活
                           │ 写入
                    ┌──────▼───────────────────────┐
                    │  调度库（事实源）              │
                    │  batches / jobs / job_events │
                    └──────▲───────────────────────┘
                           │ 原子 Claim（按车道）
                    ┌──────┴───────┐
                    │ cmd/worker   │  数据面：按 lane 消费
                    └──────┬───────┘  gpu 串行 / cpu 池 / api 限流
                           │ HTTP
                    ┌──────▼─────────────┐
                    │ Python 推理服务      │  无状态，不认识 job/batch/重试
                    └────────────────────┘
```

`server` 与 `worker` 是两个进程，**不共享任何内存状态**。
想提高吞吐就多起几个 worker，不需要改代码、不需要协调、不需要加锁。

## 目录结构与依赖方向

依赖只能由外向内：`adapter → usecase → domain`。内层永远不认识外层。

```
cmd/
  server/              控制面入口；依赖装配（唯一知道"接口由谁实现"的地方）
  worker/              数据面入口
internal/
  domain/scheduling/   实体与状态机。零外部依赖，连 database/sql 都不 import
    job.go             ★ Job 状态机：claim / succeed / fail / reclaim / cancel
    batch.go           批聚合与进度
    lane.go            资源车道（serial / pool / rate_limited）
    trigger.go         触发来源与调度范围
    event.go           审计事件
  usecase/             用例编排，只依赖 port 定义的接口
    port/              ★ 出站端口（依赖倒置）。换存储 = 换一个实现，用例不动
    submit_batch.go    提交调度单（四种触发方共用此入口）
    process_job.go     ★ worker 单次工作循环
    reap_expired.go    回收过期租约
    query_batch.go     进度与留底查询
    cancel_batch.go    整单取消
  adapter/
    inbound/httpapi/   HTTP 入口（标准库 ServeMux，无路由框架）
    inbound/worker/    轮询驱动（与 httpapi 同层，只是被 ticker 驱动而非请求）
    outbound/sqlstore/ ★ job_repo.go 的 Claim 是全系统正确性核心
    outbound/executor/ 任务执行器；inference.go 是调 Python 推理服务的客户端
    outbound/expander/ 把「三班」展开成 42 个学生——业务知识的唯一落点
  platform/            配置、日志、ID 生成、优雅退出
migrations/            SQL 迁移（embed 进二进制，单文件部署）
examples/
  inference-service/   Python 推理服务示例（FastAPI）
```

## 快速开始

```bash
make build

# 终端 1：控制面
./bin/server

# 终端 2：数据面（未配置推理服务时使用内置 demo 执行器）
./bin/worker
```

提交一张调度单：

```bash
curl -X POST localhost:8080/api/v1/batches \
  -H 'Content-Type: application/json' \
  -H 'X-Actor-Kind: admin' -H 'X-Actor-Subject: admin-1' \
  -d '{
        "title": "相册 AI 分析",
        "job_kind": "ai.analyze",
        "lane": "gpu",
        "scope_kind": "explicit",
        "scope_ref": "photo-1,photo-2,photo-3",
        "idempotency_key": "album-42-20260918"
      }'
```

查进度、看留底、取消：

```bash
curl localhost:8080/api/v1/batches/<batch_id>          # 含实时进度
curl localhost:8080/api/v1/batches/<batch_id>/events   # 审计轨迹
curl -X POST localhost:8080/api/v1/batches/<batch_id>/cancel \
  -H 'X-Actor-Kind: admin' -H 'X-Actor-Subject: admin-1'
```

按触发方筛选（任务中心的核心查询）：

```bash
curl 'localhost:8080/api/v1/batches?trigger_kind=admin&trigger_subject=admin-1'
```

## 接入 Python 推理

```bash
cd examples/inference-service && pip install fastapi uvicorn
uvicorn app:app --port 9000

export SCHED_INFERENCE_ENDPOINT=http://127.0.0.1:9000/infer
./bin/worker
```

推理服务只需实现一个无状态端点。重试、状态、批进度全部由 Go 侧负责，
**推理服务里不要再实现一层重试**——两边都重试会让实际执行次数变成乘积。

## 配置

全部通过环境变量，无需分发配置文件。

| 变量 | 默认值 | 说明 |
|---|---|---|
| `SCHED_DB_PATH` | `./data/scheduler.db` | 调度库路径（与业务库是两个独立文件） |
| `SCHED_HTTP_ADDR` | `:8080` | 控制面监听地址 |
| `SCHED_LANES` | 见下 | 车道拓扑（JSON） |
| `SCHED_WORKER_ID` | `<主机名>-<pid>` | worker 标识，多实例时必须唯一 |
| `SCHED_POLL_INTERVAL` | `1s` | 车道空闲时的轮询间隔 |
| `SCHED_LEASE_DURATION` | `5m` | 租约时长，须显著大于任务正常执行时间 |
| `SCHED_REAP_INTERVAL` | `30s` | 回收过期租约的扫描周期 |
| `SCHED_INFERENCE_ENDPOINT` | 空 | 推理服务地址；留空则用 demo 执行器 |
| `SCHED_INFERENCE_TIMEOUT` | `2m` | 必须小于 `SCHED_LEASE_DURATION`，启动时强制校验 |

默认车道拓扑：

```json
[
  {"name":"gpu","kind":"serial"},
  {"name":"cpu","kind":"pool","capacity":4},
  {"name":"io","kind":"pool","capacity":8},
  {"name":"api","kind":"rate_limited","capacity":2,"rate_per_s":5}
]
```

`serial` 车道的独占性由数据库保证，而不是靠"记得给 worker 配 concurrency=1"。

## 关于存储演进

现在用 SQLite，将来可换 PostgreSQL。为此付出的设计代价只有两处：

1. 时间统一存 Unix 毫秒整数，不用各家方言的 timestamp 类型；
2. 所有 SQL 写在 `sqlstore` 包内，用例层只认识 `port` 定义的接口。

真正需要换库的信号只有一个：**多机部署 worker**。
同机多进程共享一个 SQLite 文件是可行的（WAL 模式）；跨机器则必须换 PostgreSQL。
届时 `Claim` 从「`BEGIN IMMEDIATE` + 条件 UPDATE」改成「`FOR UPDATE SKIP LOCKED`」，
语义完全一致，其余代码不动。

## 不要这样做

- 在 API 进程里开 goroutine 承接全库 AI 分析，且任务不落库
- 把上传下载这类在线请求也塞进任务队列（它们应当由业务网关同步处理）
- 把 Kafka 当任务队列主体（它适合事件流与多订阅回放，不是 Job 调度）
- 只有 worker 运维面板，却没有业务级的批量进度与取消

## 测试

```bash
make test   # 带 -race
```

`internal/adapter/outbound/sqlstore/claim_test.go` 覆盖了四条正确性底线：

- 20 个 goroutine 同抢一条 `serial` 车道，有且只有一个拿到任务
- `pool` 车道不超发
- 过期租约不会永久堵死车道（即使 Reaper 尚未运行）
- 相同幂等键的重复提交被折叠成一个任务
