"""TaskEngine 回归：租约互斥、Step 检查点、两条 DAG 分支与取消安全检查点（PR4 验收）。

依赖真实 SandboxBillingClient（临时 SQLite）与假知识库工具，不需要 Redis/模型/外网。
"""
import asyncio
from types import SimpleNamespace

import pytest
import pytest_asyncio

from runtime.billing import SandboxBillingClient, demo_seed
from runtime.engine import TaskEngine
from runtime.models import RunStatus, StepStatus
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
    assert "没有发现重复扣款" in final.response_json["response"]
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
    async def broken(params, ctx):
        raise RuntimeError("知识库超时")

    env.engine._tools["search_knowledge_base"] = broken
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
