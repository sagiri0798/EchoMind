"""Runs API：任务运行时的 HTTP 入口（实施计划第 7 节）。

资源隔离：普通用户只能访问自己的 Run，他人 Run 一律返回 404 隐藏资源存在性；
管理员只读查看任意 Run。Bearer 鉴权由 api.main 的 access_control 中间件完成，
本模块只做属主判断；Token 身份覆盖请求体中的 user_id，与 /chat 一致。
"""
import hashlib
import logging
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from core.redaction import redact
from runtime.models import RunStatus
from runtime.store import (
    IllegalTransitionError,
    LeaseConflictError,
    LeaseLostError,
    RunNotFoundError,
    RunStore,
    VersionConflictError,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Runs"])

# ── 运行时依赖（lifespan 注入）───────────────────────────
_store: Optional[RunStore] = None
_dispatch = None  # PR4 引擎接入点：创建 Run 后的后台调度回调


def configure(store: RunStore) -> None:
    """由 api.main.lifespan 注入存储实例。"""
    global _store
    _store = store


def configure_dispatch(dispatch) -> None:
    """由 TaskRuntime（PR4）注入后台调度回调。"""
    global _dispatch
    _dispatch = dispatch


def _require_store() -> RunStore:
    if _store is None:
        raise HTTPException(503, "任务运行时未初始化")
    return _store


async def _load_owned_run(store: RunStore, request: Request, run_id: str):
    """读取 Run 并做属主判断；非所有者的普通用户按 404 隐藏资源存在性。"""
    try:
        run = await store.get_run(run_id)
    except RunNotFoundError as ex:
        raise HTTPException(404, "Run 不存在") from ex
    account = getattr(request.state, "account", None)
    if account and account["role"] != "admin" and run.user_id != account["user_id"]:
        raise HTTPException(404, "Run 不存在")
    return run


# ── 请求/响应模型 ────────────────────────────────────────


class RunCreateRequest(BaseModel):
    message:           str          = Field(min_length=1, max_length=12000)
    user_id:           str          = "anonymous"
    conv_id:           Optional[str] = None
    client_request_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    task_type:         str          = Field(default="duplicate_charge_refund",
                                            min_length=1, max_length=64)


class RunCreatedResponse(BaseModel):
    run_id:       str
    status:       str
    current_step: Optional[str] = None
    poll_after_ms: int          = 500


class RunStepView(BaseModel):
    step_key:        str
    ordinal:         int
    tool_name:       Optional[str]       = None
    status:          str
    attempt:         int
    idempotency_key: Optional[str]       = None
    input_json:      Optional[Dict[str, Any]] = None
    output_json:     Optional[Dict[str, Any]] = None
    evidence_json:   Optional[Dict[str, Any]] = None
    error_code:      Optional[str]       = None
    started_at:      Optional[str]       = None
    finished_at:     Optional[str]       = None


class RunApprovalView(BaseModel):
    step_key:     str
    risk_summary: str
    decision:     str
    decided_by:   Optional[str] = None
    comment:      Optional[str] = None
    created_at:   str
    decided_at:   Optional[str] = None


class RunEventView(BaseModel):
    seq:          int
    event_type:   str
    step_key:     Optional[str]       = None
    payload_json: Dict[str, Any]      = Field(default_factory=dict)
    created_at:   str


class RunView(BaseModel):
    run_id:               str
    task_type:            str
    status:               str
    current_step:         Optional[str]       = None
    version:              int
    cancel_requested:     bool
    transcript_committed: bool
    error_code:           Optional[str]       = None
    retryable:            bool
    input_json:           Optional[Dict[str, Any]] = None
    result_json:          Optional[Dict[str, Any]] = None
    response_json:        Optional[Dict[str, Any]] = None
    created_at:           str
    updated_at:           str
    finished_at:          Optional[str]       = None
    steps:                List[RunStepView]    = Field(default_factory=list)
    approvals:            List[RunApprovalView] = Field(default_factory=list)
    events:               List[RunEventView]   = Field(default_factory=list)


# ── 路由 ────────────────────────────────────────────────


@router.post("/runs", status_code=202, response_model=RunCreatedResponse)
async def create_run(body: RunCreateRequest, request: Request):
    """创建任务：持久化后返回 202，再由后台执行器接管（PR4）。

    同一 (user_id, conv_id, client_request_id) 且消息相同 → 幂等返回原 Run；
    消息不同 → 409。鉴权开启时 Token 身份覆盖 user_id。
    """
    store = _require_store()
    account = getattr(request.state, "account", None)
    if account:
        body.user_id = account["user_id"]
    client_id = body.client_request_id or str(uuid.uuid4())
    conv_id = body.conv_id or str(uuid.uuid5(uuid.NAMESPACE_URL, body.user_id + ":" + client_id))
    message_hash = hashlib.sha256(body.message.encode("utf-8")).hexdigest()

    existing = await store.find_run(body.user_id, conv_id, client_id)
    if existing is not None:
        if existing.message_hash != message_hash:
            raise HTTPException(409, "同一 client_request_id 不能用于不同消息")
        return RunCreatedResponse(run_id=existing.id, status=existing.status.value,
                                  current_step=existing.current_step)

    run = await store.create_run(
        user_id=body.user_id,
        conv_id=conv_id,
        client_request_id=client_id,
        message_hash=message_hash,
        task_type=body.task_type,
        plan_version="v1",
        context_json={},
        input_json={"message": body.message},
    )
    if _dispatch is not None:
        await _dispatch(run.id)
    return RunCreatedResponse(run_id=run.id, status=run.status.value,
                              current_step=run.current_step)


@router.get("/runs/{run_id}", response_model=RunView)
async def get_run(run_id: str, request: Request):
    """返回 Run 状态、当前 Step、全部 Step、审批、可用结果与脱敏 Event。"""
    store = _require_store()
    run = await _load_owned_run(store, request, run_id)
    steps = await store.get_steps(run_id)
    approvals = await store.get_approvals(run_id)
    events = await store.get_events(run_id)
    return RunView(
        run_id=run.id,
        task_type=run.task_type,
        status=run.status.value,
        current_step=run.current_step,
        version=run.version,
        cancel_requested=run.cancel_requested,
        transcript_committed=run.transcript_committed,
        error_code=run.error_code,
        retryable=run.retryable,
        input_json=run.input_json,
        result_json=run.result_json,
        response_json=run.response_json,
        created_at=run.created_at,
        updated_at=run.updated_at,
        finished_at=run.finished_at,
        steps=[RunStepView(step_key=s.step_key, ordinal=s.ordinal, tool_name=s.tool_name,
                           status=s.status.value, attempt=s.attempt,
                           idempotency_key=s.idempotency_key, input_json=s.input_json,
                           output_json=s.output_json, evidence_json=s.evidence_json,
                           error_code=s.error_code, started_at=s.started_at,
                           finished_at=s.finished_at) for s in steps],
        approvals=[RunApprovalView(step_key=a.step_key, risk_summary=a.risk_summary,
                                   decision=a.decision.value, decided_by=a.decided_by,
                                   comment=a.comment, created_at=a.created_at,
                                   decided_at=a.decided_at) for a in approvals],
        events=[RunEventView(seq=e.seq, event_type=e.event_type, step_key=e.step_key,
                             payload_json=redact(e.payload_json), created_at=e.created_at)
                for e in events],
    )


_CANCELABLE_WAITING = {RunStatus.WAITING_USER, RunStatus.WAITING_APPROVAL, RunStatus.PAUSED}


@router.post("/runs/{run_id}/cancel", status_code=202)
async def cancel_run(run_id: str, request: Request):
    """等待类状态立即取消；RUNNING 只设置取消标记，由引擎在安全检查点停止。

    取消仅限任务所有者；管理员对他人 Run 只读（实施计划第 7 节）。
    """
    store = _require_store()
    run = await _load_owned_run(store, request, run_id)
    account = getattr(request.state, "account", None)
    if account and run.user_id != account["user_id"]:
        raise HTTPException(403, "仅任务所有者可以取消")
    try:
        if run.status == RunStatus.RUNNING:
            updated = await store.request_cancel(run_id, expected_version=run.version)
        elif run.status in _CANCELABLE_WAITING:
            updated = await store.update_run_status(
                run_id, expected_version=run.version, next_status=RunStatus.CANCELLED,
                event_type="RUN_CANCELLED", event_payload={"by": "user"},
            )
        else:
            raise HTTPException(409, f"当前状态 {run.status.value} 不允许取消")
    except VersionConflictError as ex:
        raise HTTPException(409, "Run 已被修改，请重新查询后再操作") from ex
    except (IllegalTransitionError, LeaseConflictError, LeaseLostError) as ex:
        raise HTTPException(409, str(ex)) from ex
    return {"run_id": run_id, "status": updated.status.value,
            "cancel_requested": updated.cancel_requested}
