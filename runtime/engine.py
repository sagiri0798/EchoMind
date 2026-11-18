"""TaskEngine：固定 DAG 的任务执行器（实施计划第 11 节）。

亮点：租约 + 逐 Step 检查点。每个 Step 执行前标记 RUNNING 并写 STEP_STARTED 事件，
执行后持久化输出与证据；Run 的每次状态提交都带版本与租约校验，
过期 runner 无法覆盖新状态。取消只在 Step 之间的安全检查点响应，
绝不中断正在执行的写工具。

PR4 范围：COLLECT_CONTEXT → EVALUATE_DUPLICATE 两条分支
- 疑似重复扣款：检索政策 → 生成提案 → 停在 APPROVAL_GATE（WAITING_APPROVAL，PR5 实现审批）
- 不重复：直接生成"无需退款"最终回答，Run 走向 SUCCEEDED 并提交会话
审批、退款写入、OUTCOME_UNKNOWN 对账与 resume 在 PR5。
"""
import asyncio
import logging
import re
import socket
import uuid
from typing import Callable, Dict, List, Optional

from runtime.billing import SandboxBillingClient
from runtime.models import Run, RunStatus, RunStep, StepStatus
from runtime.plans import DUP_BRANCH, plan_for
from runtime.store import (
    IllegalTransitionError,
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
            "BUILD_NO_REFUND_ANSWER": self._step_no_refund_answer,
        }
        # PR5 补充：APPROVAL_GATE / SUBMIT_REFUND / POLL_REFUND_STATUS / BUILD_FINAL_ANSWER

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
                    await self._store.update_run_status(
                        run_id, run.version, RunStatus.FAILED,
                        lease_owner=self._owner, error_code="engine_crash", retryable=True,
                        event_type="RUN_FAILED", event_payload={"error": str(ex)[:200]},
                    )
            except Exception:
                logger.exception("失败状态写入也失败: %s", run_id)
        finally:
            self._inflight.discard(run_id)

    # ── 执行循环 ──────────────────────────────────────

    async def run(self, run_id: str) -> None:
        """执行 Run 直到等待输入/审批/终态。测试直接 await 本方法获得确定性结果。

        允许从 CREATED 或 RUNNING 进入：后者用于调度重试（原任务崩溃后重新接管）。
        """
        run = await self._store.get_run(run_id)
        if run.status not in (RunStatus.CREATED, RunStatus.RUNNING):
            return  # resume/对账在 PR5
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
        context: dict = {"collected": {}, "entities": {}}
        if run.context_json:
            context.update(run.context_json)

        while True:
            run = await self._store.get_run(run_id)
            if run.status != RunStatus.RUNNING:
                return  # 其他路径已接管（cancel/resume/审批）
            steps = await self._store.get_steps(run_id)
            step = self._next_ready(steps)
            if step is None:
                await self._finalize(run_id, run)
                return
            # 续租（每次提交都验证 owner 和 version，防止过期 runner 写入）
            try:
                run = await self._store.try_acquire_lease(run_id, self._owner, run.version)
            except (LeaseConflictError, LeaseLostError, VersionConflictError):
                logger.warning("执行中失去租约，放弃: %s", run_id)
                return
            # 安全检查点：Step 之间响应取消，绝不中断正在执行的写工具
            if run.cancel_requested:
                await self._store.update_run_status(
                    run_id, run.version, RunStatus.CANCELLED,
                    lease_owner=self._owner, event_type="RUN_CANCELLED",
                    event_payload={"by": "cancel_requested"},
                )
                return
            await self._execute_step(run, context, step)
            if context.get("_finished"):
                return  # 终态已由对应路径写入

    async def _execute_step(self, run: Run, context: dict, step: RunStep) -> None:
        step = await self._store.update_step_status(
            run.id, step.step_key, StepStatus.READY, StepStatus.RUNNING,
            attempt=step.attempt + 1,
            event_type="STEP_STARTED", event_payload={"step": step.step_key},
        )
        executor = self._executors.get(step.step_key)
        if executor is None:
            # PR4 未实现的后续步骤：重复扣款分支停在审批门前（PR5 实现 APPROVAL_GATE）
            await self._store.update_step_status(
                run.id, step.step_key, StepStatus.RUNNING, StepStatus.WAITING_APPROVAL,
                event_type="STEP_WAITING_APPROVAL",
            )
            await self._store.update_run_status(
                run.id, run.version, RunStatus.WAITING_APPROVAL,
                current_step=step.step_key, lease_owner=self._owner,
                event_type="WAITING_APPROVAL", event_payload={"step": step.step_key},
            )
            context["_finished"] = True
            return
        try:
            outcome = await executor(run, context, step)
        except StepFailure as ex:
            await self._store.update_step_status(
                run.id, step.step_key, StepStatus.RUNNING, StepStatus.FAILED,
                error_code=ex.error_code,
                event_type="STEP_FAILED", event_payload={"error": ex.error_code},
            )
            await self._store.update_run_status(
                run.id, run.version, RunStatus.FAILED,
                lease_owner=self._owner, error_code=ex.error_code, retryable=False,
                result_json={"summary": str(ex)},
                response_json={"response": ex.answer or f"处理失败：{ex}"},
                event_type="RUN_FAILED", event_payload={"step": step.step_key},
            )
            context["_finished"] = True
            return
        if outcome.needs_user_input:
            # 等待用户补充：Step 回到 READY，Run 进入 WAITING_USER（8.1 节）
            await self._store.update_step_status(
                run.id, step.step_key, StepStatus.RUNNING, StepStatus.READY,
                event_type="STEP_NEEDS_INPUT",
                event_payload={"missing": outcome.needs_user_input},
            )
            await self._store.update_run_status(
                run.id, run.version, RunStatus.WAITING_USER,
                current_step=step.step_key, lease_owner=self._owner,
                result_json={"missing_fields": outcome.needs_user_input},
                event_type="WAITING_USER_INPUT",
                event_payload={"missing": outcome.needs_user_input},
            )
            context["_finished"] = True
            return
        # 正常完成：持久化输出与证据（检查点）
        await self._store.update_step_status(
            run.id, step.step_key, StepStatus.RUNNING, StepStatus.SUCCEEDED,
            output_json=outcome.output, evidence_json=outcome.evidence,
            event_type="STEP_SUCCEEDED", event_payload={"step": step.step_key},
        )
        if outcome.answer is not None:
            run = await self._store.update_run_status(
                run.id, run.version, RunStatus.SUCCEEDED,
                current_step=step.step_key, lease_owner=self._owner,
                result_json={"collected": context.get("collected", {})},
                response_json={"response": outcome.answer},
                event_type="RUN_SUCCEEDED",
            )
            await self._commit_transcript(run, outcome.answer)
            context["_finished"] = True

    async def _finalize(self, run_id: str, run: Run) -> None:
        """没有 READY Step 且未写终态：全部完成但缺最终回答，按计划耗尽失败。"""
        steps = await self._store.get_steps(run_id)
        if all(s.status in (StepStatus.SUCCEEDED, StepStatus.SKIPPED) for s in steps) \
                and not (run.response_json or {}).get("response"):
            await self._store.update_run_status(
                run_id, run.version, RunStatus.FAILED,
                lease_owner=self._owner, error_code="plan_exhausted", retryable=False,
                event_type="RUN_FAILED", event_payload={"reason": "plan_exhausted"},
            )

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
        """收集订单信息；缺少订单号/金额/支付时间时进入 WAITING_USER（8.1 节）。"""
        message = (run.input_json or {}).get("message", "")
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
            await self._store.update_step_status(
                run.id, "SEARCH_REFUND_POLICY", StepStatus.PENDING, StepStatus.READY)
        else:
            context["duplicate"] = False
            for key in DUP_BRANCH:  # 不检索退款执行工具，不创建审批
                await self._store.update_step_status(run.id, key, StepStatus.PENDING, StepStatus.SKIPPED)
            await self._store.update_step_status(
                run.id, "BUILD_NO_REFUND_ANSWER", StepStatus.PENDING, StepStatus.READY)
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
        await self._store.update_step_status(
            run.id, "PREPARE_REFUND_PROPOSAL", StepStatus.PENDING, StepStatus.READY)
        return StepOutcome(output={"policy_docs": docs[:3]},
                           evidence={"source": "search_knowledge_base", "hits": len(docs)})

    async def _step_prepare_proposal(self, run: Run, context: dict, step: RunStep) -> StepOutcome:
        """生成退款提案（确定性模板）：退还重复的一笔。审批与执行在 PR5。"""
        order = context["order"]
        charges = context["charges"]
        proposal = {
            "order_id": order["order_id"],
            "amount_cents": charges[0]["amount_cents"],
            "reason": f"疑似重复扣款：共 {len(charges)} 笔金额相同的扣款，申请退还多余的一笔",
            "charge_ids": [charge["charge_id"] for charge in charges],
        }
        context["proposal"] = proposal
        await self._store.update_step_status(
            run.id, "APPROVAL_GATE", StepStatus.PENDING, StepStatus.READY)
        return StepOutcome(output={"proposal": proposal})

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
