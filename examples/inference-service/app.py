"""独立推理服务示例：Go 调度与 Python 算法之间的接缝。

这个服务是一个"无状态执行端点"：
  - 它不认识 batch、lane、租约、重试、进度；
  - 它不需要 Celery、不需要 Redis、不需要数据库；
  - 它崩溃了也不会丢任务——租约到期后 Go 侧的 Reaper 会把任务放回队列。

算法同学在这里写的是一个函数，不是一个分布式组件。
这就是"Python 做推理、Go 做调度"的全部集成成本。

    pip install fastapi uvicorn
    uvicorn app:app --host 0.0.0.0 --port 9000

然后在 worker 侧设置：
    export SCHED_INFERENCE_ENDPOINT=http://127.0.0.1:9000/infer
"""

from typing import Any, Optional

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="inference-service")


class InferRequest(BaseModel):
    job_id: str          # 仅用于日志串联；不要据此做任何调度决策
    kind: str            # 例如 "ai.analyze"
    target_ref: str      # 业务对象 ID（照片 ID、学生 ID）
    payload: Optional[dict[str, Any]] = None


class InferResponse(BaseModel):
    summary: str = ""
    error: str = ""


@app.post("/infer", response_model=InferResponse)
def infer(req: InferRequest) -> InferResponse:
    # 真实实现：按 target_ref 从业务库/对象存储取数据，跑模型，把结果写回业务库。
    #
    # 关于失败的约定：
    #   - 可重试的失败（显存不足、下游超时）→ 抛异常或返回非 200 状态码，
    #     Go 侧会按指数退避重试；
    #   - 不可重试的失败（图片损坏、参数非法）→ 返回 200 且 error 非空，
    #     Go 侧同样记为失败，但 detail 里会留下明确原因，便于人工排查。
    #
    # 不要在这里自己实现重试：重试是调度器的职责。
    # 两边都重试，实际执行次数会变成两者的乘积。
    if req.kind != "ai.analyze":
        return InferResponse(error=f"unsupported kind: {req.kind}")

    return InferResponse(summary=f"analyzed {req.target_ref}")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
