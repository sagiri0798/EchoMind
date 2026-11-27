"""审批决定的统一后果执行（7.4 节）。

api 与引擎共用同一实现，避免"拒绝"语义出现两份分歧（A6）：
- 审批步 FAILED（error_code=approval_rejected）
- 退款相关步骤 SKIPPED（不执行退款）
- Run 进入安全失败路径 FAILED（WAITING_APPROVAL 时先经 RUNNING 中转，5.3 节）
"""
import logging
from typing import Optional

from runtime.models import ApprovalDecision, Run, RunStatus, StepStatus
from runtime.plans import StepKey
from runtime.store import IllegalTransitionError, RunStore, StepNotFoundError

logger = logging.getLogger(__name__)

REJECTED_ANSWER = "退款申请未获批准，本次不执行退款。如有疑问请联系人工客服。"


async def apply_rejected(store: RunStore, run: Run, step_key: str,
                         lease_owner: Optional[str] = None) -> Run:
    """执行拒绝后果。非拒绝决定或 Run 已终态时原样返回（防重复执行）。"""
    if run.status in (RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.SUCCEEDED):
        return run  # 已由 API 原子决定或前次执行完成
    approval = await store.get_approval(run.id, step_key)
    if approval is None or approval.decision != ApprovalDecision.REJECTED:
        return run
    try:
        await store.update_step_status(
            run.id, step_key, StepStatus.WAITING_APPROVAL, StepStatus.FAILED,
            error_code="approval_rejected")
    except (IllegalTransitionError, StepNotFoundError):
        # 步骤状态异常时以 Run 级失败为准；事件已由拒绝方写入
        logger.warning("拒绝审批时审批步状态更新失败: %s/%s", run.id, step_key)
    for key in (StepKey.SUBMIT_REFUND, StepKey.POLL_REFUND_STATUS,
                StepKey.BUILD_FINAL_ANSWER):
        try:
            current = await store.get_step(run.id, key.value)
        except StepNotFoundError:
            continue
        if current.status == StepStatus.PENDING:
            await store.update_step_status(
                run.id, key.value, StepStatus.PENDING, StepStatus.SKIPPED,
                event_type="STEP_SKIPPED", event_payload={"reason": "approval_rejected"})
    # 状态机：WAITING_APPROVAL → RUNNING → FAILED 两步走（5.3 节）
    if run.status == RunStatus.WAITING_APPROVAL:
        run = await store.update_run_status(
            run.id, run.version, RunStatus.RUNNING,
            lease_owner=lease_owner, event_type="RUN_RESUMED", event_payload={"reason": "approval_rejected"})
    return await store.update_run_status(
        run.id, run.version, RunStatus.FAILED,
        lease_owner=lease_owner, error_code="approval_rejected", retryable=False,
        response_json={"response": REJECTED_ANSWER},
        event_type="RUN_FAILED", event_payload={"reason": "approval_rejected"})
