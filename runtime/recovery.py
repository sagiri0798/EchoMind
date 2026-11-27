"""TaskRecovery：启动扫描与对账（实施计划 11.3 节）。

进程重启后扫描非终态 Run：
- 只读 Step 的过期 RUNNING → READY 重试；
- 写 Step 的过期 RUNNING → OUTCOME_UNKNOWN，交给引擎对账收敛
  （远端已执行不重复退款；未执行复用同一幂等键重试）。
"""
import logging
from datetime import datetime
from typing import List

from runtime.billing import SandboxBillingClient
from runtime.models import ApprovalDecision, EventSpec, RunStatus, StepStatus
from runtime.plans import StepKey
from runtime.store import RunStore
from runtime.tools import build_runtime_tools, write_tool_names

logger = logging.getLogger(__name__)


class TaskRecovery:
    """启动扫描器：把崩溃遗留的中间状态收敛到可继续/可对账的状态。"""

    def __init__(self, store: RunStore, billing: SandboxBillingClient):
        self._store = store
        self._billing = billing
        # 写工具名单的单一来源（runtime.tools 注册表，9.1/11.3 节）
        self._write_tools = write_tool_names(build_runtime_tools(billing))

    async def scan(self) -> List[str]:
        """扫描并返回需要交给引擎重新调度的 Run ID 列表。

        只调度有实际工作的 Run：崩溃遗留的 RUNNING/OUTCOME_UNKNOWN，
        或本次扫描修复了步骤状态的 Run；等待用户/审批的 Run 保持原状。
        """
        run_ids: List[str] = []
        # 11.4 节：SUCCEEDED 但会话提交标记未写回 → 交给引擎补提交一次
        run_ids.extend(run.id for run in await self._store.list_uncommitted_succeeded())
        for run in await self._store.list_active_runs():
            lease_expired = not run.lease_until \
                or datetime.fromisoformat(run.lease_until) <= datetime.now()
            if not lease_expired:
                continue  # 租约有效：另一个实例仍在执行，不动
            changed = False
            for step in await self._store.get_steps(run.id):
                if step.status != StepStatus.RUNNING:
                    continue
                if step.tool_name in self._write_tools:
                    # 写 Step 结果未知：先对账再决定，不能直接重试
                    await self._store.update_step_status(
                        run.id, step.step_key, StepStatus.RUNNING, StepStatus.OUTCOME_UNKNOWN,
                        event=EventSpec("RECOVERY_SCAN", {"action": "outcome_unknown"}))
                else:
                    # 只读 Step：回到 READY 允许重试
                    await self._store.update_step_status(
                        run.id, step.step_key, StepStatus.RUNNING, StepStatus.READY,
                        event=EventSpec("RECOVERY_SCAN", {"action": "retry"}))
                changed = True
            if changed or run.status in (RunStatus.RUNNING, RunStatus.OUTCOME_UNKNOWN,
                                         RunStatus.PAUSED):
                run_ids.append(run.id)
                continue
            # P0-2 悬空收敛：审批已决定但 Run 仍停在 WAITING_APPROVAL（决定提交后崩溃）
            if run.status == RunStatus.WAITING_APPROVAL:
                approval = await self._store.get_approval(run.id, StepKey.APPROVAL_GATE.value)
                if approval is not None and approval.decision != ApprovalDecision.PENDING:
                    run_ids.append(run.id)
        if run_ids:
            logger.info("恢复扫描发现 %d 个待调度 Run: %s", len(run_ids), run_ids)
        return run_ids
