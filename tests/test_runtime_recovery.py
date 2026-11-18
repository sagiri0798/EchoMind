"""TaskRecovery 回归：进程重启后的扫描、对账与不重复退款（13.2 节 #7-#10）。"""
import sqlite3
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio

from runtime.billing import (
    FAIL_AFTER_EFFECT_BEFORE_RESPONSE,
    SandboxBillingClient,
    demo_seed,
)
from runtime.engine import TaskEngine
from runtime.models import ApprovalDecision, RunStatus, StepStatus
from runtime.recovery import TaskRecovery
from runtime.store import RunStore


def run_fields(message="订单 #12345 这个月好像被扣了两次款，金额 99 元，昨天支付的"):
    return dict(user_id="u1", conv_id="c1", client_request_id="msg-001", message_hash="h",
                task_type="duplicate_charge_refund", plan_version="v1",
                input_json={"message": message}, context_json={})


def expire_lease(db_path, run_id):
    """直接 SQL 把租约改成已过期，模拟进程崩溃/长时间挂起。"""
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE runs SET lease_until=? WHERE id=?",
                 ((datetime.now() - timedelta(seconds=1)).isoformat(), run_id))
    conn.commit()
    conn.close()


def refund_count(billing):
    conn = sqlite3.connect(billing.path)
    count = conn.execute("SELECT COUNT(*) FROM refunds").fetchone()[0]
    conn.close()
    return count


async def make_env(tmp_path, fault_mode=None):
    store = RunStore(tmp_path / "runtime.db")
    await store.bootstrap()
    billing = SandboxBillingClient(tmp_path / "billing.db", fault_injection=fault_mode)
    await billing.bootstrap()
    await billing.seed(*demo_seed())
    search_calls = []

    async def search(params, ctx):
        search_calls.append(params)
        return [{"title": "退款政策", "content": "重复扣款可在 7 日内申请原路退回"}]

    committed = []

    async def commit(run, answer):
        committed.append((run.id, answer))

    engine = TaskEngine(store, billing, tools={"search_knowledge_base": search},
                        transcript_commit=commit)
    return SimpleNamespace(store=store, billing=billing, engine=engine,
                           search_calls=search_calls, committed=committed)


async def advance_to_waiting_approval(env):
    run = await env.store.create_run(**run_fields())
    await env.engine.run(run.id)
    return await env.store.get_run(run.id)


async def approve(env, run, decision=ApprovalDecision.APPROVED):
    await env.store.decide_approval(run.id, "APPROVAL_GATE", decision, decided_by="admin")
    return await env.store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                             event_type="RUN_RESUMED")


# ── 写工具结果未知 → 重启对账，不重复退款（#8/#9）────────


@pytest.mark.asyncio
async def test_write_effect_unknown_then_restart_reconciles_without_duplicate(tmp_path):
    env = await make_env(tmp_path)
    run = await advance_to_waiting_approval(env)
    await approve(env, run)
    # 注入：退款已提交但响应丢失（本地 checkpoint 前"崩溃"）
    env.billing.enable_fault(FAIL_AFTER_EFFECT_BEFORE_RESPONSE)
    await env.engine.run(run.id)
    run = await env.store.get_run(run.id)
    assert run.status == RunStatus.OUTCOME_UNKNOWN
    assert (await env.store.get_step(run.id, "SUBMIT_REFUND")).status == StepStatus.OUTCOME_UNKNOWN
    assert refund_count(env.billing) == 1  # 外部已执行

    # 模拟进程重启：新实例（同一数据库），先扫租约过期再恢复扫描
    expire_lease(env.store.path, run.id)
    store2 = RunStore(tmp_path / "runtime.db")
    await store2.bootstrap()
    billing2 = SandboxBillingClient(tmp_path / "billing.db")
    engine2 = TaskEngine(store2, billing2, tools={"search_knowledge_base": env.engine._tools["search_knowledge_base"]},
                         transcript_commit=env.engine._transcript_commit)
    recovered = await TaskRecovery(store2, billing2).scan()
    assert run.id in recovered
    await engine2.run(run.id)

    final = await store2.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED
    assert "退款" in final.response_json["response"]
    # 对账发现远端成功 → 不重复退款；最终只有一笔退款
    assert refund_count(billing2) == 1
    step = await store2.get_step(run.id, "SUBMIT_REFUND")
    assert step.status == StepStatus.SUCCEEDED and step.idempotency_key
    assert final.transcript_committed is True


# ── 对账发现远端未执行 → 复用同一幂等键重新提交（#10）────


@pytest.mark.asyncio
async def test_reconcile_not_found_resubmits_with_same_idempotency_key(tmp_path):
    env = await make_env(tmp_path)
    run = await advance_to_waiting_approval(env)
    await approve(env, run)
    # 模拟：写工具 RUNNING 时崩溃且请求根本没到远端；恢复扫描会标记 OUTCOME_UNKNOWN
    await env.store.update_step_status(run.id, "SUBMIT_REFUND", StepStatus.PENDING, StepStatus.READY)
    await env.store.update_step_status(run.id, "SUBMIT_REFUND", StepStatus.READY, StepStatus.RUNNING)
    # 现实流程中幂等键在执行前已持久化（9.2 节），这里一并模拟
    await env.store.set_step_idempotency_key(run.id, "SUBMIT_REFUND", "simulated-key")
    await env.store.update_step_status(run.id, "SUBMIT_REFUND", StepStatus.RUNNING, StepStatus.OUTCOME_UNKNOWN)
    expire_lease(env.store.path, run.id)
    assert refund_count(env.billing) == 0

    recovered = await TaskRecovery(env.store, env.billing).scan()
    assert run.id in recovered
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED
    assert refund_count(env.billing) == 1
    step = await env.store.get_step(run.id, "SUBMIT_REFUND")
    # 重新提交使用的 operation_id 就是 Step 的幂等键（同一输入永远算出同一个键）
    conn = sqlite3.connect(env.billing.path)
    op_id = conn.execute("SELECT operation_id FROM refunds").fetchone()[0]
    conn.close()
    assert op_id == step.idempotency_key


# ── 只读 Step 过期 RUNNING → READY 重试（#7）────────────


@pytest.mark.asyncio
async def test_stale_read_step_running_returns_to_ready(tmp_path):
    env = await make_env(tmp_path)
    run = await advance_to_waiting_approval(env)
    # 模拟崩溃：LOAD_ORDER 卡在 RUNNING（直接 SQL 模拟脏状态），租约过期
    conn = sqlite3.connect(env.store.path)
    conn.execute("UPDATE run_steps SET status='RUNNING' WHERE run_id=? AND step_key='LOAD_ORDER'",
                 (run.id,))
    conn.commit()
    conn.close()
    expire_lease(env.store.path, run.id)

    recovered = await TaskRecovery(env.store, env.billing).scan()
    assert run.id in recovered
    step = await env.store.get_step(run.id, "LOAD_ORDER")
    assert step.status == StepStatus.READY  # 只读步骤允许重试

    await env.engine.run(run.id)
    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.WAITING_APPROVAL  # 重试后回到审批等待


# ── 审批拒绝 → 不执行退款（安全失败路径）───────────────


@pytest.mark.asyncio
async def test_rejected_approval_never_submits_refund(tmp_path):
    env = await make_env(tmp_path)
    run = await advance_to_waiting_approval(env)
    await approve(env, run, decision=ApprovalDecision.REJECTED)

    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.FAILED
    assert final.error_code == "approval_rejected"
    assert "不执行退款" in final.response_json["response"]
    assert refund_count(env.billing) == 0
    for key in ("SUBMIT_REFUND", "POLL_REFUND_STATUS", "BUILD_FINAL_ANSWER"):
        assert (await env.store.get_step(run.id, key)).status == StepStatus.SKIPPED
