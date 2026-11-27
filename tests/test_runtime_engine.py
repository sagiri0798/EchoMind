"""TaskEngine 回归：租约互斥、Step 检查点、两条 DAG 分支与取消安全检查点（PR4 验收）。

依赖真实 SandboxBillingClient（临时 SQLite）与假知识库工具，不需要 Redis/模型/外网。
"""
import asyncio
import json
import sqlite3
from types import SimpleNamespace

import pytest
import pytest_asyncio

from runtime.billing import SandboxBillingClient, demo_seed
from runtime.engine import TaskEngine
from runtime.models import EventSpec, RunStatus, StepStatus
from runtime.plans import plan_for
from runtime.store import RunStore


def run_fields(message="订单 #12345 这个月好像被扣了两次款，金额 99 元，昨天支付的", **overrides):
    base = dict(
        user_id="u1",
        conv_id="c1",
        client_request_id="msg-001",
        message_hash="h",
        task_type="duplicate_charge_refund",
        plan_version="v1",
        input_json={"message": message},
        context_json={},
    )
    base.update(overrides)
    return base


@pytest_asyncio.fixture
async def env(tmp_path):
    store = RunStore(tmp_path / "runtime.db")
    await store.bootstrap()
    billing = SandboxBillingClient(tmp_path / "billing.db")
    await billing.bootstrap()
    await billing.seed(*demo_seed())
    search_calls = []

    async def search(params, ctx):
        search_calls.append(params)
        return [{"title": "退款政策", "content": "重复扣款可在 7 日内申请原路退回"}]

    committed = []

    async def commit(run, answer):
        committed.append((run.id, answer))

    engine = TaskEngine(store, billing,
                        tools={"search_knowledge_base": search},
                        transcript_commit=commit)
    return SimpleNamespace(store=store, billing=billing, engine=engine,
                           search_calls=search_calls, committed=committed)


async def create_run(store, **overrides):
    return await store.create_run(**run_fields(**overrides))


# ── 重复扣款分支 ─────────────────────────────────────


@pytest.mark.asyncio
async def test_duplicate_charge_runs_to_waiting_approval(env):
    run = await create_run(env.store)
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.WAITING_APPROVAL
    assert final.current_step == "APPROVAL_GATE"
    steps = {s.step_key: s for s in await env.store.get_steps(run.id)}
    for key in ("COLLECT_CONTEXT", "LOAD_ORDER", "LOAD_CHARGES", "EVALUATE_DUPLICATE",
                "SEARCH_REFUND_POLICY", "PREPARE_REFUND_PROPOSAL"):
        assert steps[key].status == StepStatus.SUCCEEDED, key
    assert steps["APPROVAL_GATE"].status == StepStatus.WAITING_APPROVAL
    # 只读工具调用一次；不触碰退款写工具
    assert len(env.search_calls) == 1
    # 每个执行过的 Step 都留有检查点
    assert steps["LOAD_ORDER"].output_json["order"]["order_id"] == "12345"
    assert steps["EVALUATE_DUPLICATE"].output_json["duplicate"] is True
    assert steps["PREPARE_REFUND_PROPOSAL"].output_json["proposal"]["amount_cents"] == 9900
    event_types = [e.event_type for e in await env.store.get_events(run.id)]
    assert "RUN_STARTED" in event_types and event_types.count("STEP_STARTED") == 7
    assert "WAITING_APPROVAL" in event_types
    # 未到最终回答，不提交会话
    assert env.committed == [] and final.transcript_committed is False


@pytest.mark.asyncio
async def test_no_duplicate_branch_succeeds_without_refund_tools(env):
    run = await create_run(env.store, message="订单 #67890 昨天支付了 199 元，帮我看看扣款记录")
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED
    assert "未发现超过订单金额的多收" in final.response_json["response"]
    steps = {s.step_key: s for s in await env.store.get_steps(run.id)}
    # 不检索退款执行工具、不创建审批
    for key in ("SEARCH_REFUND_POLICY", "PREPARE_REFUND_PROPOSAL", "APPROVAL_GATE",
                "SUBMIT_REFUND", "POLL_REFUND_STATUS", "BUILD_FINAL_ANSWER"):
        assert steps[key].status == StepStatus.SKIPPED, key
    assert steps["BUILD_NO_REFUND_ANSWER"].status == StepStatus.SUCCEEDED
    assert env.search_calls == []
    # 最终回答一次性提交会话并打标记
    assert len(env.committed) == 1 and final.transcript_committed is True


# ── 等待用户与失败路径 ────────────────────────────────


@pytest.mark.asyncio
async def test_missing_order_number_waits_for_user(env):
    run = await create_run(env.store, message="我这个月好像被扣了两次款")
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.WAITING_USER
    assert final.current_step == "COLLECT_CONTEXT"
    assert "订单号" in final.result_json["missing_fields"]
    assert env.search_calls == []  # 没有订单，不进查询
    # Step 回到 READY，等待补充后重试
    step = await env.store.get_step(run.id, "COLLECT_CONTEXT")
    assert step.status == StepStatus.READY


@pytest.mark.asyncio
async def test_unknown_order_fails_with_explanation(env):
    run = await create_run(env.store, message="订单 #00000 被扣了两次款，99 元，昨天")
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.FAILED
    assert final.error_code == "order_not_found"
    assert "核对订单号" in final.response_json["response"]


@pytest.mark.asyncio
async def test_knowledge_failure_fails_without_refund(env):
    from dataclasses import replace

    async def broken(params, ctx):
        raise RuntimeError("知识库超时")

    registry = dict(env.engine._tool_registry)
    spec = registry["search_knowledge_base"]
    registry["search_knowledge_base"] = replace(spec, handler=broken)
    env.engine._tool_registry = registry

    run = await create_run(env.store)
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.FAILED
    assert final.error_code == "knowledge_unavailable"
    step = await env.store.get_step(run.id, "SEARCH_REFUND_POLICY")
    assert step.status == StepStatus.FAILED
    # 保留失败原因，不进入自动退款
    assert (await env.store.get_step(run.id, "APPROVAL_GATE")).status == StepStatus.PENDING


# ── 取消与租约 ────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_requested_stops_run_at_checkpoint(env):
    run = await create_run(env.store)
    await env.store.create_steps(run.id, plan_for(run.task_type))
    run = await env.store.update_run_status(run.id, 0, RunStatus.RUNNING)
    await env.store.request_cancel(run.id, expected_version=run.version)

    await env.engine.run(run.id)  # RUNNING 的 Run 也允许引擎接管（调度重试场景）

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.CANCELLED
    assert env.search_calls == []  # 取消后不启动新的工作
    assert (await env.store.get_events(run.id))[-1].event_type == "RUN_CANCELLED"


@pytest.mark.asyncio
async def test_two_engines_only_one_executes(env):
    run = await create_run(env.store)
    engine2 = TaskEngine(env.store, env.billing,
                         tools={"search_knowledge_base": env.engine._tools["search_knowledge_base"]})
    await asyncio.gather(env.engine.run(run.id), engine2.run(run.id))

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.WAITING_APPROVAL
    # 只有获得租约的 runner 执行了知识检索
    assert len(env.search_calls) == 1


# ── 审批与退款全流程（PR5）─────────────────────────────


@pytest.mark.asyncio
async def test_approved_refund_flow_submits_once_and_succeeds(env):
    from runtime.models import ApprovalDecision
    run = await create_run(env.store)
    await env.engine.run(run.id)
    run = await env.store.get_run(run.id)
    assert run.status == RunStatus.WAITING_APPROVAL

    # 审批通过（API 在 PR5 里做的事：决定 + 转回 RUNNING + 调度）
    await env.store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.APPROVED,
                                    decided_by="admin")
    await env.store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                      event=EventSpec("RUN_RESUMED"))
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED
    assert "已原路退回" in final.response_json["response"]
    assert final.transcript_committed is True and len(env.committed) == 1
    # 审批通过 → 退款只提交一次
    import sqlite3
    conn = sqlite3.connect(env.billing.path)
    count = conn.execute("SELECT COUNT(*) FROM refunds").fetchone()[0]
    conn.close()
    assert count == 1
    steps = {s.step_key: s for s in await env.store.get_steps(run.id)}
    assert steps["SUBMIT_REFUND"].status == StepStatus.SUCCEEDED
    assert steps["POLL_REFUND_STATUS"].status == StepStatus.SUCCEEDED
    assert steps["BUILD_FINAL_ANSWER"].status == StepStatus.SUCCEEDED
    assert steps["SUBMIT_REFUND"].idempotency_key  # 幂等键已持久化
    # 12.1：写工具检查点必须带幂等键哈希（不暴露明文）
    checkpoints = {e.step_key: e.payload_json for e in await env.store.get_events(run.id)
                   if e.event_type == "CHECKPOINT_COMMITTED"}
    assert checkpoints["SUBMIT_REFUND"]["idempotency_key_hash"]
    assert checkpoints["SUBMIT_REFUND"]["tool"] == "submit_refund"


@pytest.mark.asyncio
async def test_approval_required_before_any_refund_submit(env):
    run = await create_run(env.store)
    await env.engine.run(run.id)  # 停在 WAITING_APPROVAL
    import sqlite3
    conn = sqlite3.connect(env.billing.path)
    count = conn.execute("SELECT COUNT(*) FROM refunds").fetchone()[0]
    conn.close()
    assert count == 0  # 审批前绝不执行退款


@pytest.mark.asyncio
async def test_tool_timeout_pauses_and_recovers(env):
    """P1：RuntimeToolSpec.timeout_s 真正生效——挂起工具 → PAUSED 可重试；恢复后继续。"""
    from dataclasses import replace

    async def hung(params, ctx):
        await asyncio.sleep(30)
        return []

    registry = dict(env.engine._tool_registry)
    spec = registry["search_knowledge_base"]
    registry["search_knowledge_base"] = replace(spec, timeout_s=0.1, handler=hung)
    env.engine._tool_registry = registry

    run = await create_run(env.store)
    await env.engine.run(run.id)

    paused = await env.store.get_run(run.id)
    assert paused.status == RunStatus.PAUSED
    assert paused.error_code == "knowledge_timeout" and paused.retryable is True
    assert (await env.store.get_step(run.id, "SEARCH_REFUND_POLICY")).status == StepStatus.READY

    # 恢复：换回快速 handler 并 resume
    async def fast(params, ctx):
        return [{"title": "退款政策", "content": "重复扣款可在 7 日内申请原路退回"}]

    registry["search_knowledge_base"] = replace(spec, timeout_s=10.0, handler=fast)
    env.engine._tool_registry = registry
    paused = await env.store.update_run_status(paused.id, paused.version, RunStatus.RUNNING,
                                               event=EventSpec("RUN_RESUMED"))
    await env.engine.run(run.id)
    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.WAITING_APPROVAL  # 恢复后正常推进到审批


# ── 13.2 测试缺口补强（S11）────────────────────────────


@pytest.mark.asyncio
async def test_billing_unavailable_pauses_and_recovers(env):
    """FAIL_BEFORE_EFFECT 端到端：瞬时账单故障 → PAUSED 可恢复，重试后成功（P1-2）。"""
    from runtime.billing import FAIL_BEFORE_EFFECT
    from runtime.models import ApprovalDecision
    run = await create_run(env.store)
    await env.engine.run(run.id)
    run = await env.store.get_run(run.id)
    await env.store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.APPROVED,
                                    decided_by="admin")
    await env.store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                      event=EventSpec("RUN_RESUMED"))
    env.billing.enable_fault(FAIL_BEFORE_EFFECT)
    await env.engine.run(run.id)

    paused = await env.store.get_run(run.id)
    assert paused.status == RunStatus.PAUSED
    assert paused.error_code == "billing_unavailable" and paused.retryable is True
    assert (await env.store.get_step(run.id, "SUBMIT_REFUND")).status == StepStatus.READY
    # 副作用未发生
    import sqlite3
    conn = sqlite3.connect(env.billing.path)
    assert conn.execute("SELECT COUNT(*) FROM refunds").fetchone()[0] == 0
    conn.close()

    # resume 恢复（API 语义：PAUSED → RUNNING + 调度）
    paused = await env.store.update_run_status(paused.id, paused.version, RunStatus.RUNNING,
                                               event=EventSpec("RUN_RESUMED"))
    await env.engine.run(run.id)
    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED
    conn = sqlite3.connect(env.billing.path)
    assert conn.execute("SELECT COUNT(*) FROM refunds").fetchone()[0] == 1
    conn.close()


@pytest.mark.asyncio
async def test_engine_rerun_after_success_keeps_single_external_write(env):
    from runtime.models import ApprovalDecision
    run = await create_run(env.store)
    await env.engine.run(run.id)
    run = await env.store.get_run(run.id)
    await env.store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.APPROVED,
                                    decided_by="admin")
    await env.store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                      event=EventSpec("RUN_RESUMED"))
    await env.engine.run(run.id)
    # 13.2 #5：完成后再次调度引擎，不产生第二次外部写
    await env.engine.run(run.id)
    import sqlite3
    conn = sqlite3.connect(env.billing.path)
    count = conn.execute("SELECT COUNT(*) FROM refunds").fetchone()[0]
    conn.close()
    assert count == 1


@pytest.mark.asyncio
async def test_two_concurrent_resumes_only_one_executes(env):
    run = await create_run(env.store, message="我这个月好像被扣了两次款")  # 缺订单号
    await env.engine.run(run.id)
    assert (await env.store.get_run(run.id)).status == RunStatus.WAITING_USER

    # 模拟 API resume：合并补充输入并转回 RUNNING（仍缺金额/支付时间，会再次 WAITING_USER）
    run = await env.store.get_run(run.id)
    run = await env.store.append_run_input(run.id, run.version, "还是没有订单号")
    run = await env.store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                            event=EventSpec("RUN_RESUMED"))

    engine2 = TaskEngine(env.store, env.billing,
                         tools={"search_knowledge_base": env.engine._tools["search_knowledge_base"]})
    # 13.2 #6：两个并发 resume（重新调度引擎），只有一个获得租约并执行
    await asyncio.gather(env.engine.run(run.id), engine2.run(run.id))

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.WAITING_USER  # 仍缺订单号
    events = [e for e in await env.store.get_events(run.id)
              if e.event_type == "STEP_STARTED" and e.step_key == "COLLECT_CONTEXT"]
    assert len(events) == 2  # 首次 1 次 + 并发 resume 中只有一个真正执行


# ── S1：取消与写工具对账 ─────────────────────────────


def refund_count(billing) -> int:
    conn = sqlite3.connect(billing.path)
    try:
        return conn.execute("SELECT COUNT(*) FROM refunds").fetchone()[0]
    finally:
        conn.close()


def refund_amount(billing) -> int:
    conn = sqlite3.connect(billing.path)
    try:
        row = conn.execute("SELECT amount_cents FROM refunds").fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


async def seed_order(billing, order_id: str, amount_cents: int, charge_amounts) -> None:
    await billing.seed(
        [{"order_id": order_id, "user_id": "u1", "amount_cents": amount_cents,
          "status": "paid", "created_at": "2026-11-01T10:00:00"}],
        [{"charge_id": f"{order_id}-chg-{i}", "order_id": order_id, "user_id": "u1",
          "amount_cents": amount, "paid_at": f"2026-11-01T10:0{i}:00",
          "channel": "wechat"} for i, amount in enumerate(charge_amounts)])


def force_cancel_flag(store) -> None:
    """直接置位 cancel_requested，模拟用户已在 RUNNING 时取消（不递增版本）。"""
    conn = sqlite3.connect(store.path)
    try:
        conn.execute("UPDATE runs SET cancel_requested=1")
        conn.commit()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_cancel_after_refund_succeeded_completes_run(env):
    """S1：退款已成功时 cancel 不能宣称已取消，必须继续完成最终回答并提交会话。"""
    from runtime.models import ApprovalDecision
    run = await create_run(env.store)
    await env.engine.run(run.id)
    run = await env.store.get_run(run.id)
    await env.store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.APPROVED,
                                    decided_by="admin")
    run = await env.store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                            event=EventSpec("RUN_RESUMED"))
    original_execute = env.engine._execute_step
    fired = []

    async def execute_then_cancel(run_obj, context, step):
        await original_execute(run_obj, context, step)
        if step.step_key == "SUBMIT_REFUND" and not fired:
            submit = await env.store.get_step(run.id, "SUBMIT_REFUND")
            if submit.status == StepStatus.SUCCEEDED:
                fired.append(True)
                force_cancel_flag(env.store)

    env.engine._execute_step = execute_then_cancel
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert fired == [True]
    assert final.status == RunStatus.SUCCEEDED  # 外部退款已发生，绝不宣称取消
    assert "已原路退回" in final.response_json["response"]
    assert refund_count(env.billing) == 1
    assert final.transcript_committed is True


@pytest.mark.asyncio
async def test_cancel_with_refund_in_flight_holds_outcome_unknown(env):
    """S1/7.5：退款仍在 submitted/processing 时不能取消，Run 收敛为 OUTCOME_UNKNOWN。"""
    from runtime.billing import FAIL_AFTER_EFFECT_BEFORE_RESPONSE
    from runtime.models import ApprovalDecision
    run = await create_run(env.store)
    await env.engine.run(run.id)
    run = await env.store.get_run(run.id)
    await env.store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.APPROVED,
                                    decided_by="admin")
    run = await env.store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                            event=EventSpec("RUN_RESUMED"))
    env.billing.enable_fault(FAIL_AFTER_EFFECT_BEFORE_RESPONSE)
    await env.engine.run(run.id)  # 副作用已发生、响应丢失 → OUTCOME_UNKNOWN
    assert (await env.store.get_run(run.id)).status == RunStatus.OUTCOME_UNKNOWN

    async def still_processing(operation_id, user_id):
        return {"operation_id": operation_id, "status": "processing"}

    env.billing.get_refund_status = still_processing  # 远端长期处理中
    force_cancel_flag(env.store)
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.OUTCOME_UNKNOWN
    assert final.cancel_requested is True
    events = await env.store.get_events(run.id)
    assert "RUN_CANCELLED" not in [e.event_type for e in events]
    assert any(e.event_type == "OUTCOME_UNKNOWN"
               and e.payload_json.get("reason") == "cancel_reconcile_in_flight" for e in events)
    assert (await env.store.get_step(run.id, "SUBMIT_REFUND")).status != StepStatus.SKIPPED
    assert refund_count(env.billing) == 1


# ── S3：resume 补充输入走现有意图实体提取 ──────────────


@pytest.mark.asyncio
async def test_resume_uses_injected_entity_extractor(tmp_path):
    """S3：补充输入必须交给注入的意图实体提取器，而不是只靠引擎自带正则。"""
    store = RunStore(tmp_path / "runtime.db")
    await store.bootstrap()
    billing = SandboxBillingClient(tmp_path / "billing.db")
    await billing.bootstrap()
    await billing.seed(*demo_seed())
    calls = []

    def extractor(message):
        calls.append(message)
        if "补充" in message:
            return {"order_id": ["12345"], "amount": ["99元"], "date": ["昨天"]}
        return {}

    async def search(params, ctx):
        return [{"title": "退款政策", "content": "重复扣款可在 7 日内申请原路退回"}]

    engine = TaskEngine(store, billing, tools={"search_knowledge_base": search},
                        entity_extractor=extractor)
    run = await store.create_run(**run_fields(message="我这个月好像被扣了两次款"))
    await engine.run(run.id)
    assert (await store.get_run(run.id)).status == RunStatus.WAITING_USER

    run = await store.get_run(run.id)
    run = await store.append_run_input(run.id, run.version,
                                       "补充：订单号 #12345，99 元，昨天支付")
    run = await store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                        event=EventSpec("RUN_RESUMED"))
    await engine.run(run.id)

    final = await store.get_run(run.id)
    assert final.status == RunStatus.WAITING_APPROVAL
    assert any("补充" in message for message in calls)  # 注入的提取器收到补充输入
    order_step = await store.get_step(run.id, "LOAD_ORDER")
    assert order_step.output_json["order"]["order_id"] == "12345"


# ── S6：CHECKPOINT_COMMITTED 事件字段 ─────────────────


@pytest.mark.asyncio
async def test_checkpoint_events_carry_input_evidence_and_duration(env):
    """S6/12.1：检查点必须记录输入摘要、证据 ID、耗时、尝试次数与幂等键哈希。"""
    run = await create_run(env.store)
    await env.engine.run(run.id)

    events = await env.store.get_events(run.id)
    checkpoints = {e.step_key: e.payload_json
                   for e in events if e.event_type == "CHECKPOINT_COMMITTED"}
    for key in ("COLLECT_CONTEXT", "LOAD_ORDER", "LOAD_CHARGES",
                "EVALUATE_DUPLICATE", "SEARCH_REFUND_POLICY", "PREPARE_REFUND_PROPOSAL"):
        assert key in checkpoints, key
    payload = checkpoints["LOAD_ORDER"]
    assert payload["tool"] == "get_order"
    assert payload["status"] == "SUCCEEDED"
    assert payload["attempt"] == 1
    assert payload["duration_ms"] >= 0
    assert payload["evidence_keys"] == ["order_id"]
    assert payload["input_summary"]["keys"] == ["order_id"]
    # 输入摘要只存字段名 + 哈希，不泄露订单号明文（9.2/12.2）
    assert "12345" not in json.dumps(payload, ensure_ascii=False)


# ── S7：RuntimeToolSpec 与四级风险策略 ────────────────


def test_runtime_tool_registry_and_risk_policy():
    """S7/9.1/9.3：白名单工具元数据齐全；写工具必须审批；BLOCKED 不可执行。"""
    from dataclasses import replace
    from runtime.policy import RiskLevel, can_execute, requires_approval
    from runtime.tools import build_runtime_tools, write_tool_names

    class FakeBilling:
        pass

    registry = build_runtime_tools(FakeBilling())
    assert set(registry) == {"get_order", "get_charges", "evaluate_duplicate",
                             "search_knowledge_base", "submit_refund", "get_refund_status"}
    for name, spec in registry.items():
        for field in ("name", "description", "input_schema", "handler", "read_only",
                      "side_effect", "requires_approval", "retry_safe", "timeout_s",
                      "idempotency_scope", "risk_level"):
            assert hasattr(spec, field), f"{name}.{field}"

    submit = registry["submit_refund"]
    assert submit.read_only is False and submit.side_effect is True
    assert submit.requires_approval is True and submit.retry_safe is False
    assert submit.risk_level == RiskLevel.HIGH_RISK_WRITE
    assert submit.idempotency_scope == "run_step"
    assert write_tool_names(registry) == {"submit_refund"}

    low = replace(registry["get_charges"], risk_level=RiskLevel.LOW_RISK_WRITE,
                  read_only=False, side_effect=True)
    assert requires_approval(low) is True   # 9.3：LOW_RISK_WRITE v1 也不自动执行
    blocked = replace(registry["get_charges"], risk_level=RiskLevel.BLOCKED)
    assert can_execute(blocked) is False    # 9.3：BLOCKED 一律拒绝
    assert can_execute(registry["get_charges"]) is True


# ── 跨 Run 重复退款闸门 ────────────────────────────────


@pytest.mark.asyncio
async def test_second_run_on_same_order_cannot_refund_again(env):
    """跨 Run：第一笔全额退款成功后，第二个 Run 对同一订单再退全额被订单级闸门拦截。"""
    from runtime.models import ApprovalDecision

    async def run_refund(client_request_id: str):
        run = await create_run(env.store, client_request_id=client_request_id)
        await env.engine.run(run.id)
        current = await env.store.get_run(run.id)
        assert current.status == RunStatus.WAITING_APPROVAL
        await env.store.decide_approval(run.id, "APPROVAL_GATE",
                                        ApprovalDecision.APPROVED, decided_by="admin")
        current = await env.store.update_run_status(
            run.id, current.version, RunStatus.RUNNING,
            event=EventSpec("RUN_RESUMED"))
        await env.engine.run(run.id)
        return await env.store.get_run(run.id)

    first = await run_refund("msg-first")
    assert first.status == RunStatus.SUCCEEDED
    assert refund_count(env.billing) == 1

    second = await run_refund("msg-second")
    assert second.status == RunStatus.FAILED
    assert second.error_code == "refund_limit_exceeded"
    assert "不重复退款" in second.response_json["response"]
    assert refund_count(env.billing) == 1  # 没有第二笔外部退款


# ── 可退上限 = 多收金额（总扣款 - 订单金额）──────────────


async def approve_and_run(env, run):
    from runtime.models import ApprovalDecision
    current = await env.store.get_run(run.id)
    await env.store.decide_approval(run.id, "APPROVAL_GATE",
                                    ApprovalDecision.APPROVED, decided_by="admin")
    current = await env.store.update_run_status(
        run.id, current.version, RunStatus.RUNNING,
        event=EventSpec("RUN_RESUMED"))
    await env.engine.run(run.id)
    return await env.store.get_run(run.id)


@pytest.mark.asyncio
async def test_refund_amount_capped_by_overcharge_full(env):
    """订单 5000 分 + 两笔 5000 分（多收 5000 分）→ 提案/实际退款都是 5000 分。"""
    await seed_order(env.billing, "OC5000", 5000, [5000, 5000])
    run = await create_run(env.store, message="订单 #OC5000 被扣了两次款，每次 50 元，昨天支付")
    await env.engine.run(run.id)
    assert (await env.store.get_run(run.id)).status == RunStatus.WAITING_APPROVAL
    proposal = await env.store.get_step(run.id, "PREPARE_REFUND_PROPOSAL")
    assert proposal.output_json["proposal"]["amount_cents"] == 5000
    final = await approve_and_run(env, run)
    assert final.status == RunStatus.SUCCEEDED
    assert refund_amount(env.billing) == 5000


@pytest.mark.asyncio
async def test_refund_amount_capped_by_partial_overcharge(env):
    """订单 10000 分 + 5000+5000+3000 分（多收 3000 分）→ 提案/实际退款都是 3000 分。"""
    await seed_order(env.billing, "OC13000", 10000, [5000, 5000, 3000])
    run = await create_run(env.store, message="订单 #OC13000 被扣了三次款，每次 50 元，昨天支付")
    await env.engine.run(run.id)
    assert (await env.store.get_run(run.id)).status == RunStatus.WAITING_APPROVAL
    proposal = await env.store.get_step(run.id, "PREPARE_REFUND_PROPOSAL")
    assert proposal.output_json["proposal"]["amount_cents"] == 3000
    final = await approve_and_run(env, run)
    assert final.status == RunStatus.SUCCEEDED
    assert refund_amount(env.billing) == 3000


@pytest.mark.asyncio
async def test_refund_can_exceed_order_amount_within_overcharge(env):
    """订单 1000 分 + 两笔 1500 分（多收 2000）：退 1500（> 订单额但 ≤ 多收）应成功。"""
    await seed_order(env.billing, "OC1500", 1000, [1500, 1500])
    run = await create_run(env.store, message="订单 #OC1500 被扣了两次款，每次 15 元，昨天支付")
    await env.engine.run(run.id)
    assert (await env.store.get_run(run.id)).status == RunStatus.WAITING_APPROVAL
    proposal = await env.store.get_step(run.id, "PREPARE_REFUND_PROPOSAL")
    assert proposal.output_json["proposal"]["amount_cents"] == 1500
    final = await approve_and_run(env, run)
    assert final.status == RunStatus.SUCCEEDED
    assert refund_amount(env.billing) == 1500


@pytest.mark.asyncio
async def test_no_overcharge_skips_refund_branch(env):
    """订单 10000 分 + 3000+3000 分（未超收）→ 不进入退款分支、不产生退款。"""
    await seed_order(env.billing, "OC6000", 10000, [3000, 3000])
    run = await create_run(env.store, message="订单 #OC6000 被扣了两次款，每次 30 元，昨天支付")
    await env.engine.run(run.id)
    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED
    steps = {s.step_key: s for s in await env.store.get_steps(run.id)}
    assert steps["BUILD_NO_REFUND_ANSWER"].status == StepStatus.SUCCEEDED
    assert steps["APPROVAL_GATE"].status == StepStatus.SKIPPED
    assert refund_count(env.billing) == 0
    assert "未发现超过订单金额的多收" in final.response_json["response"]


# ── 第五轮：执行中 cancel/resume → PAUSED 必须补跑 ─────────


def _blocking_search_spec(env, handler):
    from dataclasses import replace
    registry = dict(env.engine._tool_registry)
    spec = registry["search_knowledge_base"]
    env.engine._tool_registry = {
        **registry, "search_knowledge_base": replace(spec, timeout_s=0.5, handler=handler)}


@pytest.mark.asyncio
async def test_cancel_during_blocking_tool_replays_from_paused(env):
    """MEDIUM：搜索阻塞期间 cancel → 版本冲突落 PAUSED 后必须补跑，202 取消真正生效。"""
    started = asyncio.Event()

    async def hung(params, ctx):
        started.set()
        await asyncio.Event().wait()
        return []

    _blocking_search_spec(env, hung)
    run = await create_run(env.store)
    env.engine.schedule(run.id)
    await asyncio.wait_for(started.wait(), timeout=2)

    current = await env.store.get_run(run.id)
    await env.store.request_cancel(run.id, expected_version=current.version)
    env.engine.schedule(run.id)  # in-flight → 登记待重调度

    for _ in range(500):
        final = await env.store.get_run(run.id)
        if final.status in (RunStatus.CANCELLED, RunStatus.FAILED, RunStatus.SUCCEEDED):
            break
        await asyncio.sleep(0.01)
    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.CANCELLED
    assert "RUN_CANCELLED" in [e.event_type for e in await env.store.get_events(run.id)]


@pytest.mark.asyncio
async def test_resume_during_blocking_tool_replays_and_keeps_input(env):
    """MEDIUM：执行中 resume 补充输入 → PAUSED 补跑后输入不丢失，任务继续到审批。"""
    started = asyncio.Event()
    calls = {"n": 0}

    async def flaky(params, ctx):
        calls["n"] += 1
        if calls["n"] == 1:
            started.set()
            await asyncio.Event().wait()
        return [{"title": "退款政策", "content": "重复扣款可在 7 日内申请原路退回"}]

    _blocking_search_spec(env, flaky)
    run = await create_run(env.store)
    env.engine.schedule(run.id)
    await asyncio.wait_for(started.wait(), timeout=2)

    current = await env.store.get_run(run.id)
    await env.store.append_run_input(run.id, current.version, "补充：订单号 #12345")
    env.engine.schedule(run.id)  # in-flight → 登记待重调度

    for _ in range(500):
        final = await env.store.get_run(run.id)
        if final.status == RunStatus.WAITING_APPROVAL:
            break
        await asyncio.sleep(0.01)
    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.WAITING_APPROVAL
    assert final.input_json["supplements"] == ["补充：订单号 #12345"]


# ── 第五轮：执行层 fail-safe 拒绝历史自批记录 ─────────────


async def _forge_approval(store, run_id, decided_by):
    from datetime import datetime
    conn = sqlite3.connect(store.path)
    try:
        conn.execute(
            "UPDATE run_approvals SET decision='approved', decided_by=?, decided_at=? "
            "WHERE run_id=?", (decided_by, datetime.now().isoformat(), run_id))
        conn.commit()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_engine_refuses_legacy_self_approved_record(env):
    """MEDIUM：绕过 API 写入的自批记录，引擎执行层必须 fail-safe 拒绝退款。"""
    run = await create_run(env.store)  # user_id=u1
    await env.engine.run(run.id)
    assert (await env.store.get_run(run.id)).status == RunStatus.WAITING_APPROVAL
    await _forge_approval(env.store, run.id, "u1")  # 历史自批记录

    current = await env.store.get_run(run.id)
    current = await env.store.update_run_status(
        run.id, current.version, RunStatus.RUNNING, event=EventSpec("RUN_RESUMED"))
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.FAILED
    assert final.error_code == "approval_self_denied"
    assert refund_count(env.billing) == 0


@pytest.mark.asyncio
async def test_engine_refuses_legacy_approval_without_identity(env):
    """MEDIUM：审批人身份缺失的历史 APPROVED 记录同样拒绝执行。"""
    run = await create_run(env.store)
    await env.engine.run(run.id)
    assert (await env.store.get_run(run.id)).status == RunStatus.WAITING_APPROVAL
    await _forge_approval(env.store, run.id, None)

    current = await env.store.get_run(run.id)
    current = await env.store.update_run_status(
        run.id, current.version, RunStatus.RUNNING, event=EventSpec("RUN_RESUMED"))
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.FAILED
    assert final.error_code == "approval_identity_missing"
    assert refund_count(env.billing) == 0


@pytest.mark.asyncio
async def test_engine_refuses_legacy_anonymous_approval_on_other_users_run(env):
    """MEDIUM：decided_by=anonymous 不能审批非 anonymous 用户的 Run。"""
    run = await create_run(env.store)  # user_id=u1
    await env.engine.run(run.id)
    assert (await env.store.get_run(run.id)).status == RunStatus.WAITING_APPROVAL
    await _forge_approval(env.store, run.id, "anonymous")

    current = await env.store.get_run(run.id)
    current = await env.store.update_run_status(
        run.id, current.version, RunStatus.RUNNING, event=EventSpec("RUN_RESUMED"))
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.FAILED
    assert final.error_code == "approval_anonymous_denied"
    assert refund_count(env.billing) == 0
