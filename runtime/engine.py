"""TaskEngine：固定 DAG 的任务执行器（实施计划第 11 节）。

亮点：租约 + 逐 Step 检查点。每个 Step 执行前标记 RUNNING 并写 STEP_STARTED 事件，
执行后持久化输出与证据；Run 的每次状态提交都带版本与租约校验，
过期 runner 无法覆盖新状态。取消只在 Step 之间的安全检查点响应，
绝不中断正在执行的写工具。

分支与等待：
- 疑似重复扣款：政策检索 → 提案 → APPROVAL_GATE（创建审批，等待人工决定）
- 审批通过 → SUBMIT_REFUND（幂等键，执行前落库）→ POLL_REFUND_STATUS → 最终回答
- 审批拒绝 → 安全失败路径，不执行退款
- 不重复：BUILD_NO_REFUND_ANSWER 最终回答，Run SUCCEEDED 并提交会话
- 写工具结果未知（BillingOutcomeUnknown）→ OUTCOME_UNKNOWN → 对账收敛
"""
import asyncio
import hashlib
import json
import logging
import re
import socket
import time
import uuid
from datetime import date, datetime
from typing import Callable, Dict, List, Optional

from runtime import metrics as runtime_metrics
from runtime.approval import apply_rejected
from runtime.billing import BillingOutcomeUnknown, BillingUnavailable, SandboxBillingClient
from runtime.models import ApprovalDecision, Run, RunStatus, RunStep, StepStatus
from runtime.plans import DUP_BRANCH, StepKey, plan_for
from runtime.policy import can_execute, requires_approval
from runtime.store import (
    LeaseConflictError,
    LeaseLostError,
    RunStore,
    StepExistsError,
    VersionConflictError,
)
from runtime.tools import build_runtime_tools

logger = logging.getLogger(__name__)


class StepFailure(RuntimeError):
    """Step 业务失败：引擎把 error_code 与说明落为 Run 的失败原因。"""

    def __init__(self, message: str, error_code: str, *, answer: Optional[str] = None):
        super().__init__(message)
        self.error_code = error_code
        self.answer = answer


class StepOutcome:
    """Step 执行结果。

    needs_user_input：进入 WAITING_USER，Step 回到 READY 等恢复重试；
    answer：该 Step 产出最终回答，Run 走向 SUCCEEDED。
    执行器自行写入等待态/终态时设置 context["_finished"]，_execute_step 跳过常规写回。
    """

    def __init__(self, *, output: Optional[dict] = None, evidence: Optional[dict] = None,
                 needs_user_input: Optional[List[str]] = None, answer: Optional[str] = None):
        self.output = output
        self.evidence = evidence
        self.needs_user_input = needs_user_input
        self.answer = answer


class TaskEngine:
    """执行固定 DAG。同一 Run 同一时刻只有一个执行者（数据库租约保证）。"""

    def __init__(self, store: RunStore, billing: SandboxBillingClient,
                 tools: Optional[Dict[str, Callable]] = None,
                 transcript_commit: Optional[Callable] = None,
                 entity_extractor: Optional[Callable] = None):
        self._store = store
        self._billing = billing
        self._tools = dict(tools or {})
        self._transcript_commit = transcript_commit
        # 7.3 节：补充输入经过现有意图实体提取；默认回退到引擎内置同源规则
        self._entity_extractor = entity_extractor
        # 9.1/8.3 节：DAG 工具白名单与风险治理元数据（写工具名单的唯一来源）
        self._tool_registry = build_runtime_tools(
            billing, knowledge_handler=self._tools.get("search_knowledge_base"))
        # 实例 ID + 随机令牌（11.2 节）
        self._owner = f"{socket.gethostname() or 'engine'}-{uuid.uuid4().hex[:8]}"
        self._inflight: set = set()
        self._executors = {
            StepKey.COLLECT_CONTEXT.value: self._step_collect_context,
            StepKey.LOAD_ORDER.value: self._step_load_order,
            StepKey.LOAD_CHARGES.value: self._step_load_charges,
            StepKey.EVALUATE_DUPLICATE.value: self._step_evaluate_duplicate,
            StepKey.SEARCH_REFUND_POLICY.value: self._step_search_policy,
            StepKey.PREPARE_REFUND_PROPOSAL.value: self._step_prepare_proposal,
            StepKey.APPROVAL_GATE.value: self._step_approval_gate,
            StepKey.SUBMIT_REFUND.value: self._step_submit_refund,
            StepKey.POLL_REFUND_STATUS.value: self._step_poll_refund_status,
            StepKey.BUILD_FINAL_ANSWER.value: self._step_build_final_answer,
            StepKey.BUILD_NO_REFUND_ANSWER.value: self._step_no_refund_answer,
        }

    # ── 调度入口 ──────────────────────────────────────

    def schedule(self, run_id: str) -> None:
        """后台调度入口（挂在 runtime.api 的 _dispatch）。内存去重只是优化，租约才是保证。"""
        if run_id in self._inflight:
            return
        self._inflight.add(run_id)
        asyncio.create_task(self._guarded(run_id))

    async def _guarded(self, run_id: str) -> None:
        try:
            await self.run(run_id)
        except Exception as ex:
            # P1-2：基础设施类失败（未分类异常）→ Step 按读写分类收敛 + Run PAUSED
            # 可恢复；resume/重启扫描都能重新调度，绝不落成不可恢复的 FAILED 终态。
            logger.exception("任务执行异常: %s", run_id)
            try:
                run = await self._store.get_run(run_id)
                if run.status in (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED):
                    return
                if run.status != RunStatus.RUNNING:
                    return  # 其他等待态本身可收敛，不强行改写
                write_tools = {name for name, spec in self._tool_registry.items()
                               if spec.side_effect}
                for step in await self._store.get_steps(run_id):
                    if step.status != StepStatus.RUNNING:
                        continue
                    if step.tool_name in write_tools:
                        await self._store.update_step_status(
                            run_id, step.step_key, StepStatus.RUNNING,
                            StepStatus.OUTCOME_UNKNOWN,
                            event_type="RECOVERY_SCAN",
                            event_payload={"action": "outcome_unknown"})
                    else:
                        await self._store.update_step_status(
                            run_id, step.step_key, StepStatus.RUNNING, StepStatus.READY,
                            event_type="RECOVERY_SCAN", event_payload={"action": "retry"})
                run = await self._store.update_run_status(
                    run_id, run.version, RunStatus.PAUSED,
                    lease_owner=self._owner, error_code="engine_error", retryable=True,
                    event_type="RUN_PAUSED", event_payload={"error": str(ex)[:200]},
                )
                await self._release_lease(run)
            except Exception:
                logger.exception("失败状态写入也失败: %s", run_id)
        finally:
            self._inflight.discard(run_id)

    # ── 执行循环 ──────────────────────────────────────

    async def run(self, run_id: str) -> None:
        """执行 Run 直到等待输入/审批/终态。测试直接 await 本方法获得确定性结果。

        允许从 CREATED / RUNNING / WAITING_APPROVAL / OUTCOME_UNKNOWN / PAUSED 进入：
        后三种先转回 RUNNING（7.3 节恢复语义），再进入执行循环。
        """
        run = await self._store.get_run(run_id)
        # 11.4 节：回答已提交但 transcript 标记未写回时，恢复路径只补提交一次，避免重复写入
        if run.status == RunStatus.SUCCEEDED and not run.transcript_committed \
                and (run.response_json or {}).get("response"):
            await self._commit_transcript(run, run.response_json["response"])
            return
        if run.status not in (RunStatus.CREATED, RunStatus.RUNNING, RunStatus.WAITING_APPROVAL,
                              RunStatus.OUTCOME_UNKNOWN, RunStatus.PAUSED):
            return
        try:
            steps = await self._ensure_steps(run_id, run)
        except ValueError as ex:
            await self._store.update_run_status(
                run_id, run.version, RunStatus.FAILED,
                error_code="unknown_task_type", retryable=False,
                event_type="RUN_FAILED", event_payload={"error": str(ex)},
            )
            return
        try:
            run = await self._store.try_acquire_lease(run_id, self._owner, run.version)
        except (LeaseConflictError, LeaseLostError, VersionConflictError):
            # 版本冲突同样说明已有其他 runner 接管
            logger.warning("租约被其他 runner 持有，放弃执行: %s", run_id)
            return
        if run.status == RunStatus.CREATED:
            run = await self._store.update_run_status(
                run_id, run.version, RunStatus.RUNNING,
                lease_owner=self._owner, event_type="RUN_STARTED",
            )
        elif run.status == RunStatus.WAITING_APPROVAL:
            gate_approval = await self._store.get_approval(run_id, StepKey.APPROVAL_GATE.value)
            if gate_approval is None or gate_approval.decision == ApprovalDecision.PENDING:
                return  # 仍在等待审批，无事可做
            run = await self._store.update_run_status(
                run_id, run.version, RunStatus.RUNNING,
                lease_owner=self._owner, event_type="RUN_RESUMED",
            )
            runtime_metrics.record_resume("approval")
        elif run.status in (RunStatus.OUTCOME_UNKNOWN, RunStatus.PAUSED):
            run = await self._store.update_run_status(
                run_id, run.version, RunStatus.RUNNING,
                lease_owner=self._owner, event_type="RUN_RESUMED",
            )
            runtime_metrics.record_resume("recovery")
        context: dict = await self._rebuild_context(steps)
        if run.context_json:
            context.update(run.context_json)

        while True:
            run = await self._store.get_run(run_id)
            if run.status != RunStatus.RUNNING:
                return  # 其他路径已接管（cancel/审批决定等）
            steps = await self._store.get_steps(run_id)
            # 续租（每次提交都验证 owner 和 version，防止过期 runner 写入）
            try:
                run = await self._store.try_acquire_lease(run_id, self._owner, run.version)
            except (LeaseConflictError, LeaseLostError, VersionConflictError):
                logger.warning("执行中失去租约，放弃: %s", run_id)
                return
            # 安全检查点：Step 之间响应取消，绝不中断正在执行的写工具。
            # 7.5 节：写工具已执行成功或结果未知时，不能宣称已取消外部操作，必须先对账。
            if run.cancel_requested:
                submit = next((s for s in steps if s.step_key == StepKey.SUBMIT_REFUND.value), None)
                if submit is not None and submit.status == StepStatus.OUTCOME_UNKNOWN:
                    # 写操作结果未知：先对账；远端失败已写终态则退出
                    if not await self._reconcile(run):
                        return
                    steps = await self._store.get_steps(run_id)
                    submit = next((s for s in steps if s.step_key == StepKey.SUBMIT_REFUND.value), None)
                if submit is not None and submit.status == StepStatus.SUCCEEDED:
                    # 外部退款已确认成功：不能宣称已取消，继续完成最终回答
                    pass
                else:
                    # 写操作尚未执行/已跳过：安全取消，未执行的分支步骤标记 SKIPPED
                    for pending in steps:
                        if pending.status == StepStatus.PENDING:
                            await self._store.update_step_status(
                                run_id, pending.step_key, StepStatus.PENDING, StepStatus.SKIPPED,
                                event_type="STEP_SKIPPED", event_payload={"reason": "cancelled"})
                    run = await self._store.update_run_status(
                        run_id, run.version, RunStatus.CANCELLED,
                        lease_owner=self._owner, event_type="RUN_CANCELLED",
                        event_payload={"by": "cancel_requested"},
                    )
                    runtime_metrics.record_run_terminal(run.task_type, RunStatus.CANCELLED.value)
                    await self._release_lease(run)
                    return
            # OUTCOME_UNKNOWN 对账：远端成功不重复提交，未执行复用同一幂等键
            if not await self._reconcile(run):
                return
            # 对账可能激活/修复了 Step，重新读取再决策
            steps = await self._store.get_steps(run_id)
            # 审批已决定的 APPROVAL_GATE：继续执行（approve 激活退款，reject 安全失败）
            gate = self._gate_awaiting_decision(steps)
            if gate is not None:
                approval = await self._store.get_approval(run.id, gate.step_key)
                if approval is None or approval.decision == ApprovalDecision.PENDING:
                    return  # 仍在等待审批
                await self._execute_step(run, context, gate)
                if context.get("_finished"):
                    return
                continue
            step = self._next_ready(steps)
            if step is None:
                await self._finalize(run_id, run)
                return
            await self._execute_step(run, context, step)
            if context.get("_finished"):
                return

    async def _enforce_tool_policy(self, run: Run, step: RunStep) -> None:
        """9.3 节：白名单、风险与审批在程序层强制，模型、工具和用户都不能绕过。"""
        if not step.tool_name:
            return  # 无工具步骤（收集/评估/回答）
        spec = self._tool_registry.get(step.tool_name)
        if spec is None:
            raise StepFailure(f"工具不在 DAG 白名单: {step.tool_name}", "tool_not_whitelisted")
        if not can_execute(spec):
            raise StepFailure(f"工具被风险策略禁止: {step.tool_name}", "tool_blocked")
        if requires_approval(spec):
            approval = await self._store.get_approval(run.id, StepKey.APPROVAL_GATE.value)
            if approval is None or approval.decision != ApprovalDecision.APPROVED:
                raise StepFailure(
                    f"写工具 {step.tool_name} 需要人工审批，审批前绝不执行", "approval_required")

    async def _execute_step(self, run: Run, context: dict, step: RunStep) -> None:
        expected = step.status  # READY 或（已决定的）WAITING_APPROVAL
        step = await self._store.update_step_status(
            run.id, step.step_key, expected, StepStatus.RUNNING,
            attempt=step.attempt + 1,
            event_type="STEP_STARTED", event_payload={"step": step.step_key},
        )
        executor = self._executors.get(step.step_key)
        started = time.monotonic()
        try:
            if executor is None:
                raise StepFailure(f"Step 未实现: {step.step_key}", "step_not_implemented")
            await self._enforce_tool_policy(run, step)
            outcome = await executor(run, context, step)
        except StepFailure as ex:
            await self._store.update_step_status(
                run.id, step.step_key, StepStatus.RUNNING, StepStatus.FAILED,
                error_code=ex.error_code,
                event_type="STEP_FAILED", event_payload={"error": ex.error_code},
            )
            runtime_metrics.record_step(step.step_key, StepStatus.FAILED.value,
                                        time.monotonic() - started)
            await self._store.record_event(
                run.id, "CHECKPOINT_COMMITTED", step_key=step.step_key,
                payload=self._checkpoint_payload(step, StepStatus.FAILED,
                                                 time.monotonic() - started, ex.error_code))
            run = await self._store.update_run_status(
                run.id, run.version, RunStatus.FAILED,
                lease_owner=self._owner, error_code=ex.error_code, retryable=False,
                result_json={"summary": str(ex)},
                response_json={"response": ex.answer or f"处理失败：{ex}"},
                event_type="RUN_FAILED", event_payload={"step": step.step_key},
            )
            runtime_metrics.record_run_terminal(run.task_type, RunStatus.FAILED.value)
            await self._release_lease(run)
            context["_finished"] = True
            return
        if context.get("_finished"):
            return  # 执行器已自行写入等待态/终态
        if outcome.needs_user_input:
            # 等待用户补充：Step 回到 READY，Run 进入 WAITING_USER（8.1 节）
            await self._store.update_step_status(
                run.id, step.step_key, StepStatus.RUNNING, StepStatus.READY,
                event_type="STEP_NEEDS_INPUT",
                event_payload={"missing": outcome.needs_user_input},
            )
            runtime_metrics.record_step(step.step_key, StepStatus.READY.value,
                                        time.monotonic() - started)
            run = await self._store.update_run_status(
                run.id, run.version, RunStatus.WAITING_USER,
                current_step=step.step_key, lease_owner=self._owner,
                result_json={"missing_fields": outcome.needs_user_input},
                event_type="WAITING_USER_INPUT",
                event_payload={"missing": outcome.needs_user_input},
            )
            await self._release_lease(run)
            context["_finished"] = True
            return
        # 正常完成：持久化输出与证据（检查点）
        await self._store.update_step_status(
            run.id, step.step_key, StepStatus.RUNNING, StepStatus.SUCCEEDED,
            output_json=outcome.output, evidence_json=outcome.evidence,
            event_type="STEP_SUCCEEDED", event_payload={"step": step.step_key},
        )
        duration = time.monotonic() - started
        runtime_metrics.record_step(step.step_key, StepStatus.SUCCEEDED.value, duration)
        await self._store.record_event(
            run.id, "CHECKPOINT_COMMITTED", step_key=step.step_key,
            payload=self._checkpoint_payload(step, StepStatus.SUCCEEDED, duration))
        if outcome.answer is not None:
            run = await self._store.update_run_status(
                run.id, run.version, RunStatus.SUCCEEDED,
                current_step=step.step_key, lease_owner=self._owner,
                result_json={"collected": context.get("collected", {})},
                response_json={"response": outcome.answer},
                event_type="RUN_SUCCEEDED",
            )
            runtime_metrics.record_run_terminal(run.task_type, RunStatus.SUCCEEDED.value)
            await self._commit_transcript(run, outcome.answer)
            await self._release_lease(run)
            context["_finished"] = True

    @staticmethod
    def _checkpoint_payload(step: RunStep, status: StepStatus, duration_s: float,
                            error_code: Optional[str] = None) -> dict:
        """12.1 节：检查点事件记录输入摘要、工具、幂等键、尝试次数、耗时、状态与证据 ID。"""
        evidence = step.evidence_json or {}
        payload = {
            "step": step.step_key,
            "tool": step.tool_name,
            "attempt": step.attempt,
            "status": status.value,
            "duration_ms": round(duration_s * 1000, 1),
            "idempotency_key_hash": hashlib.sha256(step.idempotency_key.encode()).hexdigest()[:16]
            if step.idempotency_key else None,
            "evidence_keys": sorted(evidence.keys()),
            "input_summary": (json.dumps(step.input_json or {}, ensure_ascii=False)[:200]),
        }
        if error_code:
            payload["error_code"] = error_code
        return payload

    # ── 对账（OUTCOME_UNKNOWN）────────────────────────

    async def _reconcile(self, run: Run) -> bool:
        """对 OUTCOME_UNKNOWN 步骤查询远端结果（11.3 节）。

        - 远端成功 → Step SUCCEEDED，激活轮询，不重复退款；
        - 远端未执行/处理中 → Step READY：重试是幂等的
          （同一 operation_id 重复提交返回相同退款记录，不会重复退款）；
        - 远端失败 → Run 走安全失败路径。
        """
        for step in await self._store.get_steps(run.id):
            if step.status != StepStatus.OUTCOME_UNKNOWN:
                continue
            if not step.idempotency_key:
                # 9.2 节：幂等键在执行写工具前持久化；无键 ⇒ 外部调用从未发出 → 安全重试
                await self._store.update_step_status(
                    run.id, step.step_key, StepStatus.OUTCOME_UNKNOWN, StepStatus.READY,
                    event_type="RECONCILED", event_payload={"result": "no_idempotency_key"})
                continue
            record = await self._billing.get_refund_status(step.idempotency_key, run.user_id)
            if record is None:
                await self._store.update_step_status(
                    run.id, step.step_key, StepStatus.OUTCOME_UNKNOWN, StepStatus.READY,
                    event_type="RECONCILED", event_payload={"result": "not_found"})
            elif record["status"] == "succeeded":
                await self._store.update_step_status(
                    run.id, step.step_key, StepStatus.OUTCOME_UNKNOWN, StepStatus.SUCCEEDED,
                    event_type="RECONCILED", event_payload={"result": "succeeded"})
                await self._activate(run.id, StepKey.POLL_REFUND_STATUS.value)
            elif record["status"] == "failed":
                await self._store.update_step_status(
                    run.id, step.step_key, StepStatus.OUTCOME_UNKNOWN, StepStatus.FAILED,
                    error_code="refund_failed",
                    event_type="RECONCILED", event_payload={"result": "failed"})
                run = await self._store.update_run_status(
                    run.id, run.version, RunStatus.FAILED,
                    lease_owner=self._owner, error_code="refund_failed", retryable=False,
                    response_json={"response": "退款最终状态为失败，请核对账单或联系人工客服。"},
                    event_type="RUN_FAILED", event_payload={"reason": "refund_failed"})
                runtime_metrics.record_run_terminal(run.task_type, RunStatus.FAILED.value)
                await self._release_lease(run)
                return False
            else:
                # submitted/processing：幂等重提（相同 operation_id 返回相同记录）
                await self._store.update_step_status(
                    run.id, step.step_key, StepStatus.OUTCOME_UNKNOWN, StepStatus.READY,
                    event_type="RECONCILED", event_payload={"result": "in_flight"})
        return True

    async def _rebuild_context(self, steps: List[RunStep]) -> dict:
        """从已持久化的 Step 输出重建执行上下文（重启/恢复后继续用）。

        表驱动：step_key → 上下文提取器（A9 修复 Repeated Switches）。
        """
        context: dict = {"collected": {}, "entities": {}}
        for step in steps:
            extract = self._CONTEXT_EXTRACTORS.get(step.step_key)
            if extract is None:
                continue
            fields = extract(step, step.output_json or {})
            if fields:
                context.update(fields)
        return context

    @staticmethod
    def _collect_context_fields(step: RunStep, output: dict):
        if not output.get("entities"):
            return None
        return {"entities": output["entities"], "collected": output["entities"]}

    @staticmethod
    def _order_context_fields(step: RunStep, output: dict):
        return {"order": output["order"]} if output.get("order") else None

    @staticmethod
    def _charges_context_fields(step: RunStep, output: dict):
        return {"charges": (step.evidence_json or {}).get("charges", [])}

    @staticmethod
    def _duplicate_context_fields(step: RunStep, output: dict):
        return {"duplicate": output.get("duplicate")}

    @staticmethod
    def _proposal_context_fields(step: RunStep, output: dict):
        return {"proposal": output["proposal"]} if output.get("proposal") else None

    @staticmethod
    def _refund_context_fields(step: RunStep, output: dict):
        return {"refund": output["refund"]} if output.get("refund") else None

    _CONTEXT_EXTRACTORS = {
        StepKey.COLLECT_CONTEXT.value: _collect_context_fields,
        StepKey.LOAD_ORDER.value: _order_context_fields,
        StepKey.LOAD_CHARGES.value: _charges_context_fields,
        StepKey.EVALUATE_DUPLICATE.value: _duplicate_context_fields,
        StepKey.PREPARE_REFUND_PROPOSAL.value: _proposal_context_fields,
        StepKey.SUBMIT_REFUND.value: _refund_context_fields,
    }

    async def _finalize(self, run_id: str, run: Run) -> None:
        """没有 READY Step 且未写终态：全部完成但缺最终回答，按计划耗尽失败。"""
        steps = await self._store.get_steps(run_id)
        if all(s.status in (StepStatus.SUCCEEDED, StepStatus.SKIPPED) for s in steps) \
                and not (run.response_json or {}).get("response"):
            run = await self._store.update_run_status(
                run_id, run.version, RunStatus.FAILED,
                lease_owner=self._owner, error_code="plan_exhausted", retryable=False,
                event_type="RUN_FAILED", event_payload={"reason": "plan_exhausted"},
            )
            runtime_metrics.record_run_terminal(run.task_type, RunStatus.FAILED.value)
            await self._release_lease(run)

    async def _ensure_steps(self, run_id: str, run: Run) -> List[RunStep]:
        steps = await self._store.get_steps(run_id)
        if steps:
            return steps
        try:
            await self._store.create_steps(run_id, plan_for(run.task_type))
        except StepExistsError:
            pass  # 并发调度时另一个 runner 已创建
        return await self._store.get_steps(run_id)

    @staticmethod
    def _next_ready(steps: List[RunStep]) -> Optional[RunStep]:
        for step in steps:
            if step.status == StepStatus.READY:
                return step
        return None

    @staticmethod
    def _gate_awaiting_decision(steps: List[RunStep]) -> Optional[RunStep]:
        for step in steps:
            if step.step_key == StepKey.APPROVAL_GATE.value \
                    and step.status == StepStatus.WAITING_APPROVAL:
                return step
        return None

    async def _activate(self, run_id: str, step_key: str) -> None:
        """幂等激活分支步骤：PENDING/OUTCOME_UNKNOWN → READY；其余状态不动。"""
        current = await self._store.get_step(run_id, step_key)
        if current.status not in (StepStatus.PENDING, StepStatus.OUTCOME_UNKNOWN):
            return
        await self._store.update_step_status(
            run_id, step_key, current.status, StepStatus.READY)

    # ── 租约辅助 ──────────────────────────────────────

    async def _release_lease(self, run: Run) -> None:
        """停靠等待态/终态后释放租约，用户操作（resume/approve/cancel）不受阻塞。"""
        try:
            await self._store.release_lease(run.id, run.version)
        except Exception:
            logger.warning("释放租约失败: %s", run.id, exc_info=True)

    # ── 会话提交 ──────────────────────────────────────

    async def _commit_transcript(self, run: Run, answer: str) -> None:
        """最终回答一次性提交会话（11.4 节）；提交成功才标记 transcript_committed。

        重复调度/恢复时先检查标记，避免同一回答重复写入会话。
        """
        if self._transcript_commit is None:
            logger.warning("未配置会话提交回调，跳过 transcript 提交: %s", run.id)
            return
        fresh = await self._store.get_run(run.id)
        if fresh.transcript_committed:
            return  # 已提交过（11.4 节恢复检查）
        try:
            await self._transcript_commit(fresh, answer)
            await self._store.mark_transcript_committed(fresh.id, fresh.version)
        except Exception:
            logger.exception("会话提交失败: %s", run.id)

    # ── 幂等键（9.2 节）───────────────────────────────

    @staticmethod
    def _idempotency_key(run: Run, step: RunStep, proposal: dict) -> str:
        """写操作幂等键：sha256(user_id + conv_id + run_id + step_key + tool_name + canonical_json(input))。"""
        payload = {
            "user_id": run.user_id,
            "conv_id": run.conv_id,
            "run_id": run.id,
            "step_key": step.step_key,
            "tool_name": step.tool_name or "submit_refund",
            "input": proposal,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()

    # ── Step 执行器 ───────────────────────────────────

    _ORDER_RE = re.compile(r"(?:订单号?|order(?:_id)?|#)\s*[:：#]?\s*([A-Za-z0-9_-]{4,32})", re.I)
    _AMOUNT_RE = re.compile(
        r"((?:¥|￥)\s*\d+(?:\.\d{1,2})?|\d+(?:\.\d{1,2})?\s*(?:元|块|rmb|cny|usd|美元))", re.I)
    _DATE_RE = re.compile(r"(今天|明天|昨天|本周|这周|下周|\d{4}[-/.年]\d{1,2}[-/.月]\d{1,2}日?)")

    @staticmethod
    def _unique(values: List[str]) -> List[str]:
        return list(dict.fromkeys(value.strip() for value in values if value and value.strip()))

    def _extract_order_fields(self, message: str) -> Dict[str, List[str]]:
        """与意图识别器同源的规则提取：订单号、金额、支付时间。"""
        return {
            "order_id": self._unique(self._ORDER_RE.findall(message)),
            "amount": self._unique(self._AMOUNT_RE.findall(message)),
            "date": self._unique(self._DATE_RE.findall(message)),
        }

    @staticmethod
    def _parse_amount_cents(text: Optional[str]) -> Optional[int]:
        """「99元」「100 元」「¥99」→ 分；无法解析返回 None。"""
        match = re.search(r"(\d+(?:\.\d{1,2})?)", text or "")
        if not match:
            return None
        return int(round(float(match.group(1)) * 100))

    @staticmethod
    def _check_claimed_amount(amounts: Optional[List[str]], charges: list) -> Optional[str]:
        """用户声称金额与扣款记录比对；相对不可比时返回 None。"""
        if not amounts or not charges:
            return None
        claimed = TaskEngine._parse_amount_cents(amounts[0])
        if claimed is None:
            return None
        actual = {charge["amount_cents"] for charge in charges}
        if claimed not in actual:
            return (f"用户声称金额 {amounts[0]} 与扣款记录"
                    f"（{', '.join(f'{v / 100:.2f} 元' for v in sorted(actual))}）不一致")
        return None

    @staticmethod
    def _parse_absolute_date(text: Optional[str]):
        """「2026-09-01」「2026年9月1日」→ date；相对日期无法解析返回 None。"""
        match = re.search(r"(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})", text or "")
        if not match:
            return None
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None

    @staticmethod
    def _check_claimed_date(dates: Optional[List[str]], charges: list) -> Optional[str]:
        """用户声称支付时间与扣款记录比对；相对日期（昨天/今天）无法确定性比对，跳过。"""
        if not dates or not charges:
            return None
        claimed = TaskEngine._parse_absolute_date(dates[0])
        if claimed is None:
            return None
        paid = min(datetime.fromisoformat(charge["paid_at"]).date() for charge in charges)
        if abs((claimed - paid).days) > 1:
            return f"用户声称支付时间 {dates[0]} 与扣款记录 {paid.isoformat()} 不符"
        return None

    async def _step_collect_context(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """收集订单信息（含用户补充）；缺少订单号/金额/支付时间时进入 WAITING_USER（8.1 节）。

        7.3 节：补充输入经过现有意图实体提取链路（lifespan 注入），
        内置同源规则仅作无注入时的回退。
        """
        input_json = run.input_json or {}
        message = input_json.get("message", "")
        for extra in input_json.get("supplements", []):
            message += " " + extra  # resume 补充的输入一并提取
        extractor = self._entity_extractor or self._extract_order_fields
        entities = extractor(message)
        # 与既有收集结果合并（补充信息填空缺，不清空已有值）
        merged = {}
        for key in ("order_id", "amount", "date"):
            merged[key] = self._unique(
                list(context.get("collected", {}).get(key, [])) + list(entities.get(key, [])))
        missing = []
        if not merged["order_id"]:
            missing.append("订单号")
        if not merged["amount"]:
            missing.append("金额")
        if not merged["date"]:
            missing.append("支付时间")
        if missing:
            return StepOutcome(output={"entities": merged}, needs_user_input=missing)
        context["collected"] = merged
        context["entities"] = merged
        return StepOutcome(output={"entities": merged}, evidence={"from": "message"})

    async def _step_load_order(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        order_id = (context.get("collected", {}).get("order_id") or [None])[0]
        spec = self._tool_registry["get_order"]
        order = await spec.handler({"order_id": order_id}, {"user_id": run.user_id})
        if order is None:
            raise StepFailure(
                "订单不存在或不属于当前用户，请核对订单号", "order_not_found",
                answer="未能找到这笔订单，请核对订单号后重新提交。",
            )
        context["order"] = order
        return StepOutcome(output={"order": order}, evidence={"order_id": order_id})

    async def _step_load_charges(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        order_id = context["order"]["order_id"]
        spec = self._tool_registry["get_charges"]
        charges = await spec.handler({"order_id": order_id}, {"user_id": run.user_id})
        context["charges"] = charges
        return StepOutcome(output={"charge_count": len(charges)}, evidence={"charges": charges})

    async def _step_evaluate_duplicate(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """业务一致性校验 + 判断疑似重复扣款并激活对应分支（8.2 节）。

        - 一致性（13.3 评测用例）：用户声称金额/支付时间与扣款记录不符 → 失败，
          以实际扣款记录为准，不自动退款；
        - 重复规则：同一订单至少两笔金额相同的扣款 → 疑似重复扣款。
        """
        charges = context.get("charges", [])
        claimed = context.get("collected", {})
        mismatch = self._check_claimed_amount(claimed.get("amount"), charges)
        if mismatch:
            raise StepFailure(
                mismatch, "amount_mismatch",
                answer="您提供的金额与扣款记录不一致，本次不自动退款。请核对支付凭证后重新提交或联系人工客服。")
        mismatch = self._check_claimed_date(claimed.get("date"), charges)
        if mismatch:
            raise StepFailure(
                mismatch, "date_mismatch",
                answer="您提供的支付时间与扣款记录不一致，本次不自动退款。请核对支付凭证后重新提交或联系人工客服。")
        amounts = [charge["amount_cents"] for charge in charges]
        duplicate = len(amounts) >= 2 and len(set(amounts)) < len(amounts)
        if duplicate:
            context["duplicate"] = True
            await self._activate(run.id, StepKey.SEARCH_REFUND_POLICY.value)
        else:
            context["duplicate"] = False
            for key in DUP_BRANCH:  # 不检索退款执行工具，不创建审批
                await self._store.update_step_status(run.id, key.value, StepStatus.PENDING, StepStatus.SKIPPED)
            await self._activate(run.id, StepKey.BUILD_NO_REFUND_ANSWER.value)
        return StepOutcome(
            output={"duplicate": duplicate, "charge_count": len(amounts)},
            evidence={"charges": charges},
        )

    async def _step_search_policy(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """检索退款政策（只读）。知识库失败按 11.3 测试要求：不进自动退款，保留失败原因。"""
        handler = self._tools.get("search_knowledge_base")
        if handler is None:
            raise StepFailure("知识库检索未接入", "knowledge_unavailable")
        try:
            result = await handler({"query": "重复扣款 退款政策", "top_k": 5}, {})
        except Exception as ex:
            raise StepFailure(f"知识库检索失败: {ex}", "knowledge_unavailable") from ex
        docs = result if isinstance(result, list) else list(getattr(result, "data", None) or [])
        if not docs:
            raise StepFailure("知识库未返回退款政策", "knowledge_empty")
        context["policy_docs"] = docs
        await self._activate(run.id, StepKey.PREPARE_REFUND_PROPOSAL.value)
        return StepOutcome(output={"policy_docs": docs[:3]},
                           evidence={"source": "search_knowledge_base", "hits": len(docs)})

    async def _step_prepare_proposal(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """生成退款提案（确定性模板）：退还重复的一笔。"""
        order = context["order"]
        charges = context["charges"]
        proposal = {
            "order_id": order["order_id"],
            "amount_cents": charges[0]["amount_cents"],
            "reason": f"疑似重复扣款：共 {len(charges)} 笔金额相同的扣款，申请退还多余的一笔",
            "charge_ids": [charge["charge_id"] for charge in charges],
        }
        context["proposal"] = proposal
        await self._activate(run.id, StepKey.APPROVAL_GATE.value)
        return StepOutcome(output={"proposal": proposal})

    async def _step_approval_gate(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """审批门：显式三分支（P0-1）。

        - 无审批或仍 PENDING：原子创建审批并进入等待态（含崩溃恢复场景），绝不激活退款；
        - REJECTED：统一拒绝后果（防御路径，API 原子决定通常已完成）；
        - APPROVED：唯一能激活 SUBMIT_REFUND 的分支。
        """
        approval = await self._store.get_approval(run.id, step.step_key)
        if approval is None or approval.decision == ApprovalDecision.PENDING:
            proposal = context["proposal"]
            run = await self._store.request_approval_atomic(
                run.id, step.step_key,
                f"退款 {proposal['amount_cents'] / 100:.2f} 元：{proposal['reason']}",
                expected_version=run.version, lease_owner=self._owner)
            await self._release_lease(run)  # 等待人工决定期间不占用租约
            context["_finished"] = True
            return StepOutcome(output={"approval": "requested"})
        if approval.decision == ApprovalDecision.REJECTED:
            # 拒绝：统一后果执行（与 api 共用同一实现），安全失败路径，不执行退款
            run = await apply_rejected(self._store, run, step.step_key,
                                       lease_owner=self._owner)
            runtime_metrics.record_run_terminal(run.task_type, RunStatus.FAILED.value)
            await self._release_lease(run)
            context["_finished"] = True
            return StepOutcome()
        # APPROVED：显式分支才激活退款提交
        await self._activate(run.id, StepKey.SUBMIT_REFUND.value)
        return StepOutcome(output={"approved_by": approval.decided_by,
                                   "risk_summary": approval.risk_summary})

    async def _step_submit_refund(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """提交退款（写工具）：幂等键执行前落库；结果未知进入 OUTCOME_UNKNOWN 等待对账。"""
        proposal = context["proposal"]
        key = self._idempotency_key(run, step, proposal)
        # 幂等键已存在退款记录：重放被拦截（指标口径见 12.2 节 duplicate_prevented）
        if await self._billing.get_refund_status(key, run.user_id) is not None:
            runtime_metrics.record_duplicate_prevented("submit_refund")
        # 执行写工具前持久化幂等键，重试复用相同键（9.2 节）
        await self._store.set_step_idempotency_key(
            run.id, step.step_key, key,
            event_type="TOOL_ATTEMPTED",
            event_payload={"tool": "submit_refund",
                           "idempotency_key_hash": hashlib.sha256(key.encode()).hexdigest()[:16]})
        try:
            spec = self._tool_registry["submit_refund"]
            record = await spec.handler(
                {"order_id": proposal["order_id"], "amount_cents": proposal["amount_cents"],
                 "reason": proposal["reason"], "operation_id": key},
                {"user_id": run.user_id})
        except BillingUnavailable as ex:
            # P1-2：账单系统瞬时不可用（副作用未发生）→ 安全可重试，落 PAUSED 而非死 FAILED
            await self._store.update_step_status(
                run.id, step.step_key, StepStatus.RUNNING, StepStatus.READY,
                event_type="STEP_NEEDS_RETRY",
                event_payload={"error": "billing_unavailable"})
            run = await self._store.update_run_status(
                run.id, run.version, RunStatus.PAUSED,
                lease_owner=self._owner, current_step=step.step_key,
                error_code="billing_unavailable", retryable=True,
                event_type="RUN_PAUSED", event_payload={"reason": "billing_unavailable"})
            await self._release_lease(run)
            context["_finished"] = True
            return StepOutcome()
        except BillingOutcomeUnknown as ex:
            # 外部可能已执行：进入 OUTCOME_UNKNOWN，随后对账收敛（11.3 节）
            await self._store.update_step_status(
                run.id, step.step_key, StepStatus.RUNNING, StepStatus.OUTCOME_UNKNOWN,
                event_type="OUTCOME_UNKNOWN", event_payload={"operation_id": ex.operation_id})
            run = await self._store.update_run_status(
                run.id, run.version, RunStatus.OUTCOME_UNKNOWN,
                lease_owner=self._owner, current_step=step.step_key,
                event_type="OUTCOME_UNKNOWN")
            runtime_metrics.record_outcome_unknown(run.task_type)
            await self._release_lease(run)
            context["_finished"] = True
            return StepOutcome()
        context["refund"] = record
        await self._activate(run.id, StepKey.POLL_REFUND_STATUS.value)
        return StepOutcome(output={"refund_operation_id": record["operation_id"]},
                           evidence={"refund": record})

    async def _step_poll_refund_status(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """查询退款最终状态；对账恢复时从 SUBMIT_REFUND 步骤的幂等键重建记录。"""
        record = context.get("refund")
        if record is None:
            submit_step = await self._store.get_step(run.id, StepKey.SUBMIT_REFUND.value)
            if submit_step.idempotency_key:
                spec = self._tool_registry["get_refund_status"]
                record = await spec.handler(
                    {"operation_id": submit_step.idempotency_key}, {"user_id": run.user_id})
            if record is None:
                raise StepFailure("缺少退款提交结果", "refund_missing")
        spec = self._tool_registry["get_refund_status"]
        for _ in range(5):
            record = await spec.handler(
                {"operation_id": record["operation_id"]}, {"user_id": run.user_id})
            if record and record["status"] in ("succeeded", "failed"):
                break
            await asyncio.sleep(0.05)
        if record is None or record["status"] != "succeeded":
            raise StepFailure(
                "退款最终状态异常", "refund_status_unresolved",
                answer="退款状态暂时无法确认，请稍后查询或联系人工客服。")
        context["refund"] = record
        context["refund_status"] = record["status"]
        await self._activate(run.id, StepKey.BUILD_FINAL_ANSWER.value)
        return StepOutcome(output={"refund": record}, evidence={"refund": record})

    async def _step_build_final_answer(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """退款成功后的最终回答。"""
        order = context.get("order", {})
        proposal = context.get("proposal", {})
        amount = proposal.get("amount_cents", 0) / 100
        answer = (
            f"退款已处理完成：订单 {order.get('order_id')} 重复扣款多收的 {amount:.2f} 元"
            "已原路退回，退款状态：已成功到账。如有疑问欢迎随时咨询。"
        )
        return StepOutcome(answer=answer,
                           evidence={"refund_status": context.get("refund_status", "succeeded")})

    async def _step_no_refund_answer(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """不重复扣款分支的最终回答：解释 + 证据 + 后续建议（8.2 节）。"""
        order = context.get("order", {})
        charges = context.get("charges", [])
        answer = (
            f"经核对，订单 {order.get('order_id')} 共有 {len(charges)} 笔扣款记录，"
            "金额与渠道均正常，没有发现重复扣款，无需发起退款。"
            "如仍有疑问，可以提供支付凭证，我们会进一步核实。"
        )
        return StepOutcome(answer=answer, evidence={"charge_count": len(charges)})
