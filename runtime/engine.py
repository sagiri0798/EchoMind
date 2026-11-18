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
from typing import Callable, Dict, List, Optional

from runtime import metrics as runtime_metrics
from runtime.billing import BillingOutcomeUnknown, SandboxBillingClient
from runtime.models import ApprovalDecision, Run, RunStatus, RunStep, StepStatus
from runtime.plans import DUP_BRANCH, plan_for
from runtime.store import (
    LeaseConflictError,
    LeaseLostError,
    RunStore,
    StepExistsError,
    VersionConflictError,
)

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
                 transcript_commit: Optional[Callable] = None):
        self._store = store
        self._billing = billing
        self._tools = dict(tools or {})
        self._transcript_commit = transcript_commit
        # 实例 ID + 随机令牌（11.2 节）
        self._owner = f"{socket.gethostname() or 'engine'}-{uuid.uuid4().hex[:8]}"
        self._inflight: set = set()
        self._executors = {
            "COLLECT_CONTEXT": self._step_collect_context,
            "LOAD_ORDER": self._step_load_order,
            "LOAD_CHARGES": self._step_load_charges,
            "EVALUATE_DUPLICATE": self._step_evaluate_duplicate,
            "SEARCH_REFUND_POLICY": self._step_search_policy,
            "PREPARE_REFUND_PROPOSAL": self._step_prepare_proposal,
            "APPROVAL_GATE": self._step_approval_gate,
            "SUBMIT_REFUND": self._step_submit_refund,
            "POLL_REFUND_STATUS": self._step_poll_refund_status,
            "BUILD_FINAL_ANSWER": self._step_build_final_answer,
            "BUILD_NO_REFUND_ANSWER": self._step_no_refund_answer,
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
            logger.exception("任务执行异常: %s", run_id)
            try:
                run = await self._store.get_run(run_id)
                if run.status not in (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED):
                    run = await self._store.update_run_status(
                        run_id, run.version, RunStatus.FAILED,
                        lease_owner=self._owner, error_code="engine_crash", retryable=True,
                        event_type="RUN_FAILED", event_payload={"error": str(ex)[:200]},
                    )
                    runtime_metrics.record_run_terminal(run.task_type, RunStatus.FAILED.value)
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
            gate_approval = await self._store.get_approval(run_id, "APPROVAL_GATE")
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
            # 安全检查点：Step 之间响应取消，绝不中断正在执行的写工具
            if run.cancel_requested:
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

    async def _execute_step(self, run: Run, context: dict, step: RunStep) -> None:
        expected = step.status  # READY 或（已决定的）WAITING_APPROVAL
        step = await self._store.update_step_status(
            run.id, step.step_key, expected, StepStatus.RUNNING,
            attempt=step.attempt + 1,
            event_type="STEP_STARTED", event_payload={"step": step.step_key},
        )
        executor = self._executors.get(step.step_key)
        if executor is None:
            raise StepFailure(f"Step 未实现: {step.step_key}", "step_not_implemented")
        started = time.monotonic()
        try:
            outcome = await executor(run, context, step)
        except StepFailure as ex:
            await self._store.update_step_status(
                run.id, step.step_key, StepStatus.RUNNING, StepStatus.FAILED,
                error_code=ex.error_code,
                event_type="STEP_FAILED", event_payload={"error": ex.error_code},
            )
            runtime_metrics.record_step(step.step_key, StepStatus.FAILED.value,
                                        time.monotonic() - started)
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
        runtime_metrics.record_step(step.step_key, StepStatus.SUCCEEDED.value,
                                    time.monotonic() - started)
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
                await self._activate(run.id, "POLL_REFUND_STATUS")
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
        """从已持久化的 Step 输出重建执行上下文（重启/恢复后继续用）。"""
        context: dict = {"collected": {}, "entities": {}}
        for step in steps:
            output = step.output_json or {}
            if step.step_key == "COLLECT_CONTEXT" and output.get("entities"):
                context["entities"] = output["entities"]
                context["collected"] = output["entities"]
            elif step.step_key == "LOAD_ORDER" and output.get("order"):
                context["order"] = output["order"]
            elif step.step_key == "LOAD_CHARGES":
                context["charges"] = (step.evidence_json or {}).get("charges", [])
            elif step.step_key == "EVALUATE_DUPLICATE":
                context["duplicate"] = output.get("duplicate")
            elif step.step_key == "PREPARE_REFUND_PROPOSAL" and output.get("proposal"):
                context["proposal"] = output["proposal"]
            elif step.step_key == "SUBMIT_REFUND" and output.get("refund"):
                context["refund"] = output["refund"]
        return context

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
            if step.step_key == "APPROVAL_GATE" and step.status == StepStatus.WAITING_APPROVAL:
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
        """最终回答一次性提交会话（11.4 节）；提交成功才标记 transcript_committed。"""
        if self._transcript_commit is None:
            logger.warning("未配置会话提交回调，跳过 transcript 提交: %s", run.id)
            return
        try:
            await self._transcript_commit(run, answer)
            await self._store.mark_transcript_committed(run.id, run.version)
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

    async def _step_collect_context(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """收集订单信息（含用户补充）；缺少订单号/金额/支付时间时进入 WAITING_USER（8.1 节）。"""
        input_json = run.input_json or {}
        message = input_json.get("message", "")
        for extra in input_json.get("supplements", []):
            message += " " + extra  # resume 补充的输入一并提取
        entities = self._extract_order_fields(message)
        missing = []
        if not entities["order_id"]:
            missing.append("订单号")
        if not entities["amount"]:
            missing.append("金额")
        if not entities["date"]:
            missing.append("支付时间")
        if missing:
            return StepOutcome(output={"entities": entities}, needs_user_input=missing)
        context["collected"] = entities
        context["entities"] = entities
        return StepOutcome(output={"entities": entities}, evidence={"from": "message"})

    async def _step_load_order(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        order_id = (context.get("collected", {}).get("order_id") or [None])[0]
        order = await self._billing.get_order(order_id, run.user_id)
        if order is None:
            raise StepFailure(
                "订单不存在或不属于当前用户，请核对订单号", "order_not_found",
                answer="未能找到这笔订单，请核对订单号后重新提交。",
            )
        context["order"] = order
        return StepOutcome(output={"order": order}, evidence={"order_id": order_id})

    async def _step_load_charges(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        order_id = context["order"]["order_id"]
        charges = await self._billing.get_charges(order_id, run.user_id)
        context["charges"] = charges
        return StepOutcome(output={"charge_count": len(charges)}, evidence={"charges": charges})

    async def _step_evaluate_duplicate(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """判断疑似重复扣款并激活对应分支（8.2 节）。

        规则：同一订单至少两笔金额相同的扣款 → 疑似重复扣款。
        """
        charges = context.get("charges", [])
        amounts = [charge["amount_cents"] for charge in charges]
        duplicate = len(amounts) >= 2 and len(set(amounts)) < len(amounts)
        if duplicate:
            context["duplicate"] = True
            await self._activate(run.id, "SEARCH_REFUND_POLICY")
        else:
            context["duplicate"] = False
            for key in DUP_BRANCH:  # 不检索退款执行工具，不创建审批
                await self._store.update_step_status(run.id, key, StepStatus.PENDING, StepStatus.SKIPPED)
            await self._activate(run.id, "BUILD_NO_REFUND_ANSWER")
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
        await self._activate(run.id, "PREPARE_REFUND_PROPOSAL")
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
        await self._activate(run.id, "APPROVAL_GATE")
        return StepOutcome(output={"proposal": proposal})

    async def _step_approval_gate(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """审批门：第一阶段创建审批并等待；决定后 approve 激活退款提交，reject 走安全失败。"""
        approval = await self._store.get_approval(run.id, step.step_key)
        if approval is None:
            proposal = context["proposal"]
            await self._store.create_approval(
                run.id, step.step_key,
                f"退款 {proposal['amount_cents'] / 100:.2f} 元：{proposal['reason']}")
            await self._store.update_step_status(
                run.id, step.step_key, StepStatus.RUNNING, StepStatus.WAITING_APPROVAL,
                event_type="APPROVAL_REQUESTED")
            run = await self._store.update_run_status(
                run.id, run.version, RunStatus.WAITING_APPROVAL,
                current_step=step.step_key, lease_owner=self._owner,
                event_type="WAITING_APPROVAL", event_payload={"step": step.step_key})
            await self._release_lease(run)  # 等待人工决定期间不占用租约
            context["_finished"] = True
            return StepOutcome(output={"approval": "requested"})
        if approval.decision == ApprovalDecision.REJECTED:
            # 拒绝：安全的失败/升级路径，不执行退款（7.4 节）
            await self._store.update_step_status(
                run.id, step.step_key, StepStatus.RUNNING, StepStatus.FAILED,
                error_code="approval_rejected",
                event_type="APPROVAL_DECIDED", event_payload={"decision": "rejected"})
            for key in ("SUBMIT_REFUND", "POLL_REFUND_STATUS", "BUILD_FINAL_ANSWER"):
                await self._store.update_step_status(
                    run.id, key, StepStatus.PENDING, StepStatus.SKIPPED)
            run = await self._store.update_run_status(
                run.id, run.version, RunStatus.FAILED,
                lease_owner=self._owner, error_code="approval_rejected", retryable=False,
                response_json={"response": "退款申请未获批准，本次不执行退款。如有疑问请联系人工客服。"},
                event_type="RUN_FAILED", event_payload={"reason": "approval_rejected"})
            runtime_metrics.record_run_terminal(run.task_type, RunStatus.FAILED.value)
            await self._release_lease(run)
            context["_finished"] = True
            return StepOutcome()
        # approved：激活退款提交
        await self._activate(run.id, "SUBMIT_REFUND")
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
            record = await self._billing.submit_refund(
                order_id=proposal["order_id"], user_id=run.user_id,
                amount_cents=proposal["amount_cents"], reason=proposal["reason"],
                operation_id=key)
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
        await self._activate(run.id, "POLL_REFUND_STATUS")
        return StepOutcome(output={"refund_operation_id": record["operation_id"]},
                           evidence={"refund": record})

    async def _step_poll_refund_status(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """查询退款最终状态；对账恢复时从 SUBMIT_REFUND 步骤的幂等键重建记录。"""
        record = context.get("refund")
        if record is None:
            submit_step = await self._store.get_step(run.id, "SUBMIT_REFUND")
            if submit_step.idempotency_key:
                record = await self._billing.get_refund_status(
                    submit_step.idempotency_key, run.user_id)
            if record is None:
                raise StepFailure("缺少退款提交结果", "refund_missing")
        for _ in range(5):
            record = await self._billing.get_refund_status(record["operation_id"], run.user_id)
            if record and record["status"] in ("succeeded", "failed"):
                break
            await asyncio.sleep(0.05)
        if record is None or record["status"] != "succeeded":
            raise StepFailure(
                "退款最终状态异常", "refund_status_unresolved",
                answer="退款状态暂时无法确认，请稍后查询或联系人工客服。")
        context["refund"] = record
        context["refund_status"] = record["status"]
        await self._activate(run.id, "BUILD_FINAL_ANSWER")
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
