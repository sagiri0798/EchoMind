"""TaskRecovery 回归：进程重启后的扫描、对账与不重复退款（13.2 节 #7-#10）。"""
import asyncio
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


# ── 写工具「收到响应但检查点写入前崩溃」（S8/#8）──────────


@pytest.mark.asyncio
async def test_checkpoint_loss_after_response_recovers_without_duplicate(tmp_path):
    """S8 引擎侧模拟：submit_refund 响应成功、但 Step 检查点写入崩溃。"""
    from runtime.billing import FAIL_AFTER_RESPONSE_BEFORE_CHECKPOINT
    env = await make_env(tmp_path, fault_mode=FAIL_AFTER_RESPONSE_BEFORE_CHECKPOINT)
    run = await advance_to_waiting_approval(env)
    await approve(env, run)
    # 账单侧：记录已写入（响应后崩溃的注入同样抛 BillingOutcomeUnknown）
    await env.engine.run(run.id)
    run = await env.store.get_run(run.id)
    assert run.status == RunStatus.OUTCOME_UNKNOWN
    assert refund_count(env.billing) == 1

    expire_lease(env.store.path, run.id)
    recovered = await TaskRecovery(env.store, env.billing).scan()
    assert run.id in recovered
    await env.engine.run(run.id)
    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED
    assert refund_count(env.billing) == 1  # 对账后不重复退款


# ── 审批崩溃窗口（P0-1：审批创建后、Step/Run 切换前崩溃）──


@pytest.mark.asyncio
async def test_crash_after_approval_created_never_executes_refund(tmp_path):
    """审批记录已创建、审批步尚未进入 WAITING_APPROVAL 时崩溃：恢复后必须回到等待态。"""
    env = await make_env(tmp_path)
    run = await advance_to_waiting_approval(env)  # 正常路径已到等待审批（含完整 DAG）
    # 构造崩溃窗口：审批改回 pending、审批步改回 RUNNING（直接 SQL 模拟脏状态）
    conn = sqlite3.connect(env.store.path)
    conn.execute("UPDATE run_approvals SET decision='pending', decided_at=NULL "
                 "WHERE run_id=?", (run.id,))
    conn.execute("UPDATE run_steps SET status='RUNNING' WHERE run_id=? AND step_key='APPROVAL_GATE'",
                 (run.id,))
    conn.execute("UPDATE runs SET status='RUNNING' WHERE id=?", (run.id,))
    conn.commit()
    conn.close()
    expire_lease(env.store.path, run.id)

    recovered = await TaskRecovery(env.store, env.billing).scan()
    assert run.id in recovered
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    # P0-1：pending 绝不能被当作 approved——必须回到等待审批，退款零执行
    assert final.status == RunStatus.WAITING_APPROVAL
    assert refund_count(env.billing) == 0
    assert (await env.store.get_step(run.id, "SUBMIT_REFUND")).status == StepStatus.PENDING
    approval = await env.store.get_approval(run.id, "APPROVAL_GATE")
    assert approval.decision == ApprovalDecision.PENDING


# ── 审批悬空收敛（P0-2：决定提交后、Run 转换前崩溃）───────


@pytest.mark.asyncio
async def test_decided_approval_with_stale_waiting_run_converges(tmp_path):
    """审批已决定但 Run 仍停在 WAITING_APPROVAL（旧崩溃窗口）：恢复扫描必须收敛。"""
    env = await make_env(tmp_path)
    run = await advance_to_waiting_approval(env)
    # 模拟旧实现崩溃窗口：只写审批决定，不动 Run 状态
    await env.store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.APPROVED,
                                    decided_by="admin")
    expire_lease(env.store.path, run.id)

    recovered = await TaskRecovery(env.store, env.billing).scan()
    assert run.id in recovered  # 悬空状态被重新调度
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED
    assert refund_count(env.billing) == 1


# ── 写 Step 卡在 RUNNING 且幂等键尚未写入 ────────────────


@pytest.mark.asyncio
async def test_stuck_write_step_without_idempotency_key_retries_safely(tmp_path):
    env = await make_env(tmp_path)
    run = await advance_to_waiting_approval(env)
    await approve(env, run)
    # 模拟：写工具 RUNNING 时崩溃，且幂等键尚未落库（执行前崩溃）
    await env.store.update_step_status(run.id, "SUBMIT_REFUND", StepStatus.PENDING, StepStatus.READY)
    await env.store.update_step_status(run.id, "SUBMIT_REFUND", StepStatus.READY, StepStatus.RUNNING)
    expire_lease(env.store.path, run.id)

    recovered = await TaskRecovery(env.store, env.billing).scan()
    assert run.id in recovered
    step = await env.store.get_step(run.id, "SUBMIT_REFUND")
    assert step.status == StepStatus.OUTCOME_UNKNOWN  # 写 Step 先对账

    await env.engine.run(run.id)
    final = await env.store.get_run(run.id)
    # 无幂等键 ⇒ 外部调用从未发出 ⇒ 安全重试成功
    assert final.status == RunStatus.SUCCEEDED
    assert refund_count(env.billing) == 1


# ── Run 终态与 transcript 标记分阶段恢复 ─────────────────


@pytest.mark.asyncio
async def test_outcome_unknown_reconcile_success_commits_transcript_once(tmp_path):
    """真实路径：OUTCOME_UNKNOWN → 对账远端 succeeded → SUCCEEDED，会话仅提交一次、退款仅 1 笔。"""
    env = await make_env(tmp_path)
    run = await advance_to_waiting_approval(env)
    await approve(env, run)
    env.billing.enable_fault(FAIL_AFTER_EFFECT_BEFORE_RESPONSE)
    await env.engine.run(run.id)
    crashed = await env.store.get_run(run.id)
    assert crashed.status == RunStatus.OUTCOME_UNKNOWN
    assert env.committed == []  # 未完成前不提交会话

    expire_lease(env.store.path, run.id)
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED
    assert final.transcript_committed is True
    assert len(env.committed) == 1  # 最终回答只提交一次
    assert refund_count(env.billing) == 1  # 对账成功不重复退款


@pytest.mark.asyncio
async def test_succeeded_but_uncommitted_transcript_is_recovered_once(tmp_path):
    env = await make_env(tmp_path)
    run = await advance_to_waiting_approval(env)
    await approve(env, run)
    await env.engine.run(run.id)
    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED and final.transcript_committed is True

    # 模拟 Redis 提交成功、SQLite 标记写失败：回退标记
    conn = sqlite3.connect(env.store.path)
    conn.execute("UPDATE runs SET transcript_committed=0 WHERE id=?", (run.id,))
    conn.commit()
    conn.close()

    recovered = await TaskRecovery(env.store, env.billing).scan()
    assert run.id in recovered  # SUCCEEDED 但未提交的 Run 进入恢复清单
    await env.engine.run(run.id)
    after = await env.store.get_run(run.id)
    assert after.transcript_committed is True
    assert len(env.committed) == 2  # 首次提交 + 补提交各一次
    await env.engine.run(run.id)   # 标记已写回：引擎侧不再触发第三次提交
    assert len(env.committed) == 2


# ── P0：最终回答 Step 已提交、Run 终态未提交时崩溃 ─────────


@pytest.mark.asyncio
async def test_crash_between_step_and_run_success_rebuilds_answer(tmp_path, monkeypatch):
    """P0：回答持久化在 BUILD_* Step 输出；Run 终态提交前崩溃，恢复重建回答而非 plan_exhausted。"""
    env = await make_env(tmp_path)
    run = await env.store.create_run(
        **run_fields(message="订单 #67890 昨天支付了 199 元，帮我看看扣款记录"))
    original = env.store.update_run_status

    async def flaky(run_id, expected_version, next_status, **kwargs):
        if next_status == RunStatus.SUCCEEDED:
            raise RuntimeError("模拟崩溃：Run 终态提交前进程挂掉")
        return await original(run_id, expected_version, next_status, **kwargs)

    monkeypatch.setattr(env.store, "update_run_status", flaky)
    with pytest.raises(RuntimeError):
        await env.engine.run(run.id)

    run = await env.store.get_run(run.id)
    assert run.status == RunStatus.RUNNING  # 终态未写
    step = await env.store.get_step(run.id, "BUILD_NO_REFUND_ANSWER")
    assert step.status == StepStatus.SUCCEEDED
    assert "未发现超过订单金额的多收" in step.output_json["answer"]  # 回答已持久化到 Step 输出

    monkeypatch.setattr(env.store, "update_run_status", original)
    expire_lease(env.store.path, run.id)
    recovered = await TaskRecovery(env.store, env.billing).scan()
    assert run.id in recovered
    await env.engine.run(run.id)

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED
    assert "未发现超过订单金额的多收" in final.response_json["response"]
    assert final.error_code != "plan_exhausted"


# ── P1：非重复分支 SKIP 循环中途崩溃 ─────────────────────


@pytest.mark.asyncio
async def test_partial_skip_recovery_reruns_tolerantly(tmp_path):
    """P1：SKIP 半提交后重跑 EVALUATE_DUPLICATE，不得对已 SKIPPED 的 Step 提交非法转换。"""
    env = await make_env(tmp_path)
    run = await env.store.create_run(
        **run_fields(message="订单 #67890 昨天支付了 199 元，帮我看看扣款记录"))
    await env.engine.run(run.id)  # 正常完成

    # 模拟 SKIP 循环中途崩溃：EVALUATE 回 READY、部分步骤回 PENDING、清空 response
    conn = sqlite3.connect(env.store.path)
    conn.execute("UPDATE run_steps SET status='READY' WHERE run_id=? AND step_key='EVALUATE_DUPLICATE'",
                 (run.id,))
    conn.execute("UPDATE run_steps SET status='PENDING' WHERE run_id=? AND step_key='SUBMIT_REFUND'",
                 (run.id,))
    conn.execute("UPDATE run_steps SET status='PENDING' WHERE run_id=? AND step_key='BUILD_NO_REFUND_ANSWER'",
                 (run.id,))
    conn.execute("UPDATE runs SET status='RUNNING', response_json=NULL WHERE id=?", (run.id,))
    conn.commit()
    conn.close()
    expire_lease(env.store.path, run.id)

    recovered = await TaskRecovery(env.store, env.billing).scan()
    assert run.id in recovered
    await env.engine.run(run.id)  # 不允许抛 IllegalTransitionError

    final = await env.store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED
    assert "未发现超过订单金额的多收" in final.response_json["response"]


# ── cancel 与 resume 并发（13.2 #15）────────────────────


@pytest.mark.asyncio
async def test_cancel_resume_race_has_explainable_outcome(tmp_path):
    from runtime.store import IllegalTransitionError, VersionConflictError
    env = await make_env(tmp_path)
    run = await env.store.create_run(**run_fields(message="我这个月被扣了两次款"))
    await env.engine.run(run.id)
    run = await env.store.get_run(run.id)
    assert run.status == RunStatus.WAITING_USER

    async def do_cancel():
        try:
            current = await env.store.get_run(run.id)
            await env.store.update_run_status(current.id, current.version, RunStatus.CANCELLED,
                                              event_type="RUN_CANCELLED", event_payload={"by": "user"})
        except VersionConflictError:
            pass  # 并发下输掉 CAS 的一方按 409 处理（API 层语义）

    async def do_resume():
        try:
            current = await env.store.get_run(run.id)
            await env.store.append_run_input(current.id, current.version, "订单号是 #12345")
            current = await env.store.get_run(run.id)
            if current.status != RunStatus.RUNNING:
                await env.store.update_run_status(current.id, current.version, RunStatus.RUNNING,
                                                  event_type="RUN_RESUMED")
        except (VersionConflictError, IllegalTransitionError):
            pass  # 并发下输掉 CAS 或状态已被取消的一方按 409 处理（API 层语义）

    await asyncio.gather(do_cancel(), do_resume())
    final = await env.store.get_run(run.id)
    # 无论谁赢，最终状态都可解释：取消或继续运行，绝无中间态残留
    assert final.status in (RunStatus.CANCELLED, RunStatus.RUNNING, RunStatus.WAITING_USER)
    events = [e.event_type for e in await env.store.get_events(run.id)]
    if final.status == RunStatus.CANCELLED:
        assert "RUN_CANCELLED" in events
    else:
        assert "RUN_RESUMED" in events
