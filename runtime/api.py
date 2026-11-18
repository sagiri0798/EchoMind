"""Runs API：任务运行时的 HTTP 入口（实施计划第 7 节）。

资源隔离：普通用户只能访问自己的 Run，他人 Run 一律返回 404 隐藏资源存在性；
管理员只读查看任意 Run。Bearer 鉴权由 api.main 的 access_control 中间件完成，
本模块只做属主判断；Token 身份覆盖请求体中的 user_id，与 /chat 一致。
"""
import hashlib
import logging
import uuid
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from core.redaction import redact
from runtime import metrics as runtime_metrics
from runtime.models import ApprovalDecision, RunStatus, StepStatus
from runtime.store import (
    ApprovalAlreadyDecidedError,
    ApprovalNotFoundError,
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

_RESUMABLE = {RunStatus.WAITING_USER, RunStatus.PAUSED, RunStatus.OUTCOME_UNKNOWN, RunStatus.RUNNING}


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


class RunResumeRequest(BaseModel):
    message:          str = Field(min_length=1, max_length=12000)
    expected_version: int = Field(ge=0)


class ApprovalRequest(BaseModel):
    step_key:         str = Field(min_length=1, max_length=64)
    decision:         Literal["approve", "reject"]
    comment:          str = Field(default="", max_length=500)
    expected_version: int = Field(ge=0)


@router.post("/runs/{run_id}/resume", status_code=202)
async def resume_run(run_id: str, body: RunResumeRequest, request: Request):
    """恢复/补充输入（7.3 节）：补充输入合并进任务，不重新创建 Run、不扩大工具权限。"""
    store = _require_store()
    run = await _load_owned_run(store, request, run_id)
    account = getattr(request.state, "account", None)
    if account and run.user_id != account["user_id"]:
        raise HTTPException(403, "仅任务所有者可以恢复")
    if run.status not in _RESUMABLE:
        raise HTTPException(409, f"当前状态 {run.status.value} 不允许恢复")
    if run.version != body.expected_version:
        raise HTTPException(409, "expected_version 不匹配，请重新查询后再操作")
    try:
        run = await store.append_run_input(run_id, run.version, body.message)
        if run.status != RunStatus.RUNNING:
            run = await store.update_run_status(
                run_id, run.version, RunStatus.RUNNING,
                event_type="RUN_RESUMED", event_payload={"reason": "user_input"})
    except VersionConflictError as ex:
        raise HTTPException(409, "Run 已被修改，请重新查询后再操作") from ex
    if _dispatch is not None:
        await _dispatch(run_id)
    return {"run_id": run_id, "status": run.status.value}


@router.post("/runs/{run_id}/approval", status_code=202)
async def decide_approval(run_id: str, body: ApprovalRequest, request: Request):
    """审批（7.4 节）：只有任务所有者或管理员可以审批；决定不可覆盖。

    approve → Run 回到 RUNNING 并调度引擎继续；reject → 安全失败路径，绝不执行退款。
    """
    store = _require_store()
    run = await _load_owned_run(store, request, run_id)
    if run.version != body.expected_version:
        raise HTTPException(409, "expected_version 不匹配，请重新查询后再操作")
    if run.status != RunStatus.WAITING_APPROVAL:
        raise HTTPException(409, f"当前状态 {run.status.value} 不在等待审批")
    approval = await store.get_approval(run_id, body.step_key)
    if approval is None:
        raise HTTPException(404, "审批记录不存在")
    account = getattr(request.state, "account", None)
    decided_by = account["user_id"] if account else "anonymous"
    decision = (ApprovalDecision.APPROVED if body.decision == "approve"
                else ApprovalDecision.REJECTED)
    try:
        await store.decide_approval(run_id, body.step_key, decision,
                                    decided_by=decided_by, comment=body.comment or None)
    except ApprovalAlreadyDecidedError as ex:
        raise HTTPException(409, str(ex)) from ex
    except ApprovalNotFoundError as ex:
        raise HTTPException(404, str(ex)) from ex
    runtime_metrics.record_approval(decision.value)
    try:
        if decision == ApprovalDecision.REJECTED:
            # 拒绝：安全失败/升级路径，不执行退款；写工具相关步骤跳过
            # 状态机要求 WAITING_APPROVAL → RUNNING → FAILED 两步走（5.3 节）
            try:
                await store.update_step_status(
                    run_id, body.step_key, StepStatus.WAITING_APPROVAL, StepStatus.FAILED,
                    error_code="approval_rejected")
            except (IllegalTransitionError, RuntimeError):
                # 步骤状态异常时由引擎在重新调度后兜底处理；Run 级失败仍然生效
                logger.warning("拒绝审批时步骤状态更新失败，交由引擎兜底: %s/%s", run_id, body.step_key)
            run = await store.update_run_status(
                run_id, run.version, RunStatus.RUNNING,
                event_type="RUN_RESUMED", event_payload={"reason": "approval_rejected"})
            run = await store.update_run_status(
                run_id, run.version, RunStatus.FAILED,
                error_code="approval_rejected", retryable=False,
                response_json={"response": "退款申请未获批准，本次不执行退款。如有疑问请联系人工客服。"},
                event_type="RUN_FAILED", event_payload={"reason": "approval_rejected"})
            return {"run_id": run_id, "status": run.status.value, "decision": "rejected"}
        # approve：转回 RUNNING 并调度引擎继续
        run = await store.update_run_status(
            run_id, run.version, RunStatus.RUNNING,
            event_type="RUN_RESUMED", event_payload={"reason": "approval_approved"})
        if _dispatch is not None:
            await _dispatch(run_id)
        return {"run_id": run_id, "status": run.status.value, "decision": "approved"}
    except VersionConflictError as ex:
        raise HTTPException(409, "Run 已被修改，请重新查询后再操作") from ex
    except (IllegalTransitionError, LeaseConflictError, LeaseLostError) as ex:
        raise HTTPException(409, str(ex)) from ex
