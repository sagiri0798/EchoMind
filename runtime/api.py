"""Runs API：任务运行时的 HTTP 入口（实施计划第 7 节）。

资源隔离：普通用户只能访问自己的 Run，他人 Run 一律返回 404 隐藏资源存在性；
管理员只读查看任意 Run。Bearer 鉴权由 api.main 的 access_control 中间件完成，
本模块只做属主判断；Token 身份覆盖请求体中的 user_id，与 /chat 一致。
"""
import hashlib
import inspect
import logging
import re
import uuid
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from core.redaction import redact
from runtime import metrics as runtime_metrics
from runtime.models import ApprovalDecision, RunStatus, StepStatus
from runtime.policy import anonymous_approval_allowed
from runtime.store import (
    ApprovalAlreadyDecidedError,
    ApprovalNotFoundError,
    DuplicateRunError,
    IllegalTransitionError,
    LeaseConflictError,
    LeaseLostError,
    RunNotFoundError,
    RunStore,
    SelfApprovalDeniedError,
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


async def _dispatch_run(run_id: str) -> None:
    """兼容同步的 engine.schedule 与异步测试注入；直接 await 同步回调会抛 TypeError。"""
    if _dispatch is None:
        return
    result = _dispatch(run_id)
    if inspect.isawaitable(result):
        await result


def _require_store() -> RunStore:
    if _store is None:
        raise HTTPException(503, "任务运行时未初始化")
    return _store


# 12.3 节：脱敏 Step 与 Event。除常见凭据字段外，业务标识做部分掩码。
_PII_KEYS = {"user_id", "order_id", "charge_id", "charge_ids", "operation_id",
             "refund_operation_id", "approved_by", "decided_by"}
_ID_KEYS = _PII_KEYS | {"idempotency_key"}

# 与引擎实体提取文法一致，并简化掉高成本 lookahead。
# - 订单/订单号 / order_id / order id：ID 直接掩码（支持 "订单 ABCD"）
# - 裸 order / #：仅当 ID 含数字或全大写时才掩码，避免 "# Overview"、"order overview" 误伤
_ORDER_TEXT_RE = re.compile(
    r"(?P<head>订单号?|order(?:_id|\s*id)?|#)"
    r"(?P<sep>\s*(?:是|为)?\s*[:：#]?\s*)"
    r"(?P<id>[A-Za-z0-9_-]{4,32})",
    re.IGNORECASE)


def _mask_identifier(value: Optional[str]) -> Optional[str]:
    """业务标识掩码：保留末 4 位，短值整体隐藏。"""
    if not value:
        return value
    return "***" + str(value)[-4:] if len(str(value)) > 4 else "***"


def _replace_order_id(match: "re.Match") -> str:
    head, sep, ident = match.group("head"), match.group("sep"), match.group("id")
    if head == "#" or head.lower() == "order":
        if not any(ch.isdigit() for ch in ident) and not ident.isupper():
            return match.group(0)  # 普通词（# Overview / order overview）不误伤
    return head + sep + str(_mask_identifier(ident))


def _scrub_text(value: str, known_ids: Optional[set] = None) -> str:
    """自由文本脱敏：先按本次 Run 已知业务 ID 精确替换，再用引擎同源文法兜底。"""
    value = redact(value)
    if known_ids:
        for ident in sorted(known_ids, key=len, reverse=True):
            if not ident:
                continue
            value = re.sub(
                rf"(?<![A-Za-z0-9_-]){re.escape(ident)}(?![A-Za-z0-9_-])",
                str(_mask_identifier(ident)), value)
    return _ORDER_TEXT_RE.sub(_replace_order_id, value)


def _collect_known_ids(value: Any, out: set) -> None:
    """从 Run/Step/Event 结构中收集业务 ID，用于自由文本精确替换。"""
    if isinstance(value, dict):
        for key, item in value.items():
            if key in _ID_KEYS:
                if isinstance(item, str) and len(item) >= 4:
                    out.add(item)
                elif isinstance(item, list):
                    out.update(str(v) for v in item if isinstance(v, str) and len(str(v)) >= 4)
            else:
                _collect_known_ids(item, out)
    elif isinstance(value, list):
        for item in value:
            _collect_known_ids(item, out)


def _scrub(value: Any, known_ids: Optional[set] = None) -> Any:
    """递归脱敏 + 业务标识掩码（保留末 4 位，短值整体隐藏）。"""
    value = redact(value)
    if isinstance(value, dict):
        return {
            key: (_scrub_pii(item) if key in _PII_KEYS and item
                  else _scrub(item, known_ids))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_scrub(item, known_ids) for item in value]
    if isinstance(value, str):
        return _scrub_text(value, known_ids)
    return value


def _scrub_pii(item: Any) -> Any:
    """PII 字段值：列表逐项掩码，标量整体掩码。"""
    if isinstance(item, list):
        return [_mask_identifier(value) for value in item]
    return _mask_identifier(item)


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
    task_type:         Literal["duplicate_charge_refund"] = "duplicate_charge_refund"


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

    try:
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
    except DuplicateRunError as ex:
        # P1：find_run 与 create_run 之间的并发竞争（TOCTOU）——另一请求已创建，
        # 幂等返回原 Run，绝不把竞争冒泡成 500
        existing = await store.get_run(ex.run_id)
        if existing.message_hash != message_hash:
            raise HTTPException(409, "同一 client_request_id 不能用于不同消息")
        return RunCreatedResponse(run_id=existing.id, status=existing.status.value,
                                  current_step=existing.current_step)
    if _dispatch is not None:
        await _dispatch_run(run.id)
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

    # 本次 Run 已加载的业务 ID：自由文本按精确值替换，覆盖 "order_id XXX" 等非标准写法
    known_ids: set = set()
    if run.user_id:
        known_ids.add(run.user_id)  # 短 ID（如 u1）也精确替换
    for payload in (run.input_json, run.result_json, run.response_json):
        _collect_known_ids(payload, known_ids)
    for s in steps:
        _collect_known_ids(s.input_json, known_ids)
        _collect_known_ids(s.output_json, known_ids)
        _collect_known_ids(s.evidence_json, known_ids)
        if s.idempotency_key:
            known_ids.add(s.idempotency_key)
    for a in approvals:
        if a.decided_by:
            known_ids.add(a.decided_by)
    for e in events:
        _collect_known_ids(e.payload_json, known_ids)

    return RunView(
        run_id=run.id,
        task_type=_scrub_text(run.task_type, known_ids),
        status=run.status.value,
        current_step=run.current_step,
        version=run.version,
        cancel_requested=run.cancel_requested,
        transcript_committed=run.transcript_committed,
        error_code=run.error_code,
        retryable=run.retryable,
        input_json=_scrub(run.input_json, known_ids),
        result_json=_scrub(run.result_json, known_ids),
        response_json=_scrub(run.response_json, known_ids),
        created_at=run.created_at,
        updated_at=run.updated_at,
        finished_at=run.finished_at,
        steps=[RunStepView(step_key=s.step_key, ordinal=s.ordinal, tool_name=s.tool_name,
                           status=s.status.value, attempt=s.attempt,
                           idempotency_key=_mask_identifier(s.idempotency_key),
                           input_json=_scrub(s.input_json, known_ids),
                           output_json=_scrub(s.output_json, known_ids),
                           evidence_json=_scrub(s.evidence_json, known_ids),
                           error_code=s.error_code, started_at=s.started_at,
                           finished_at=s.finished_at) for s in steps],
        approvals=[RunApprovalView(step_key=a.step_key,
                                   risk_summary=_scrub_text(a.risk_summary, known_ids),
                                   decision=a.decision.value,
                                   decided_by=_mask_identifier(a.decided_by),
                                   comment=_scrub_text(a.comment, known_ids)
                                   if a.comment else a.comment,
                                   created_at=a.created_at,
                                   decided_at=a.decided_at) for a in approvals],
        events=[RunEventView(seq=e.seq, event_type=e.event_type, step_key=e.step_key,
                             payload_json=_scrub(e.payload_json, known_ids),
                             created_at=e.created_at)
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
            # 唤醒引擎执行取消安全检查点：若写工具已执行/结果未知，必须先对账（7.5 节）；
            # engine.schedule 对同 Run 去重，in-flight 时会登记待重调度，避免版本冲突后漏跑。
            await _dispatch_run(run_id)
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
    runtime_metrics.record_resume("user_input")
    if _dispatch is not None:
        await _dispatch_run(run_id)
    return {"run_id": run_id, "status": run.status.value}


@router.post("/runs/{run_id}/approval", status_code=202)
async def decide_approval(run_id: str, body: ApprovalRequest, request: Request):
    """审批（7.4 节）：只有任务所有者或管理员可以审批；决定不可覆盖。

    鉴权语义：
    - 有认证身份：任何角色（含 admin）都不能审批自己发起的 Run → 403；
      admin 可批他人 Run；非所有者由资源加载层返回 404（隐藏存在性）。
    - 无认证身份：默认拒绝（403，表示授权策略拒绝，而非服务不可用）；
      仅当 ECHOMIND_RUNTIME_ALLOW_ANONYMOUS_APPROVAL=1 时允许匿名审批，
      且只允许 `run.user_id == "anonymous"` 的本地 Run，避免匿名者审批他人任务。

    approve → Run 回到 RUNNING 并调度引擎继续；reject → 安全失败路径，绝不执行退款。
    """
    store = _require_store()
    run = await _load_owned_run(store, request, run_id)
    account = getattr(request.state, "account", None)
    if account is None:
        if not anonymous_approval_allowed():
            raise HTTPException(
                403, "无认证环境默认拒绝匿名审批；本地调试请设置 "
                     "ECHOMIND_RUNTIME_ALLOW_ANONYMOUS_APPROVAL=1")
        if run.user_id != "anonymous":
            raise HTTPException(403, "匿名审批仅允许 anonymous 用户自己的本地 Run")
    elif account["user_id"] == run.user_id:
        raise HTTPException(403, "不能审批自己发起的退款申请")
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
        # P0-2：审批决定 + Run 状态转换 + 事件在同一事务完成，不存在悬空中间态
        run = await store.decide_approval_with_consequences(
            run_id, body.step_key, decision, decided_by, body.comment or None,
            expected_version=run.version)
    except ApprovalAlreadyDecidedError as ex:
        raise HTTPException(409, str(ex)) from ex
    except SelfApprovalDeniedError as ex:
        raise HTTPException(403, str(ex)) from ex
    except ApprovalNotFoundError as ex:
        raise HTTPException(404, str(ex)) from ex
    except (VersionConflictError, IllegalTransitionError, LeaseConflictError,
            LeaseLostError) as ex:
        raise HTTPException(409, "Run 已被修改或状态不允许审批") from ex
    runtime_metrics.record_approval(decision.value)
    if decision == ApprovalDecision.APPROVED and _dispatch is not None:
        await _dispatch_run(run_id)
    return {"run_id": run_id, "status": run.status.value, "decision": decision.value}
