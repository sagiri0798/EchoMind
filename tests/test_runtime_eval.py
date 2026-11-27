"""退款评测集运行器（实施计划 13.3 节）。

从 runtime-eval/refund-v1.json 参数化执行，每个用例都有确定性预期：
最终状态、关键 Step、工具调用、审批决定、退款记录数与回答关键事实。
所有依赖为沙箱（临时 SQLite 账单、假知识库），不调用真实模型/支付/外网。
"""
import json
import pathlib
import sqlite3
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio

from runtime.billing import SandboxBillingClient, demo_seed
from runtime.engine import TaskEngine
from runtime.models import ApprovalDecision, EventSpec, RunStatus, StepStatus
from runtime.recovery import TaskRecovery
from runtime.store import RunStore

_CASES = json.loads(
    (pathlib.Path(__file__).parent.parent / "runtime-eval" / "refund-v1.json")
    .read_text(encoding="utf-8"))["cases"]


def refund_count(billing: SandboxBillingClient) -> int:
    conn = sqlite3.connect(billing.path)
    count = conn.execute("SELECT COUNT(*) FROM refunds").fetchone()[0]
    conn.close()
    return count


def refund_amount(billing: SandboxBillingClient) -> int:
    conn = sqlite3.connect(billing.path)
    row = conn.execute("SELECT amount_cents FROM refunds").fetchone()
    conn.close()
    return row[0] if row else 0


def expire_lease(store: RunStore, run_id: str) -> None:
    conn = sqlite3.connect(store.path)
    conn.execute("UPDATE runs SET lease_until=? WHERE id=?",
                 ((datetime.now() - timedelta(seconds=1)).isoformat(), run_id))
    conn.commit()
    conn.close()


async def collected_tool_events(store: RunStore, run_id: str) -> list:
    return [e.payload_json.get("tool") for e in await store.get_events(run_id)
            if e.event_type == "TOOL_ATTEMPTED"]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _CASES, ids=[case["id"] for case in _CASES])
async def test_eval_case(case, tmp_path):
    store = RunStore(tmp_path / "runtime.db")
    await store.bootstrap()
    billing = SandboxBillingClient(tmp_path / "billing.db")
    await billing.bootstrap()
    await billing.seed(*demo_seed())
    if case.get("fault"):
        billing.enable_fault(case["fault"])

    async def search(params, ctx):
        if case["kb"] == "fail":
            raise RuntimeError("知识库超时")
        return [{"title": "退款政策", "content": "重复扣款可在 7 日内申请原路退回"}]

    engine = TaskEngine(store, billing, tools={"search_knowledge_base": search})

    run = await store.create_run(
        user_id=case["user_id"], conv_id="c1", client_request_id="msg-001",
        message_hash="h", task_type="duplicate_charge_refund", plan_version="v1",
        input_json={"message": case["message"]}, context_json={})

    for action in case["actions"]:
        if action == "run":
            await engine.run(run.id)
            run = await store.get_run(run.id)
        elif action == "approve":
            # 只决定并转回 RUNNING；引擎由后续 run/recover 驱动
            await store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.APPROVED,
                                        decided_by="admin")
            run = await store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                                event=EventSpec("RUN_RESUMED"))
        elif action == "reject":
            await store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.REJECTED,
                                        decided_by="admin")
            run = await store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                                event=EventSpec("RUN_RESUMED"))
            await engine.run(run.id)
            run = await store.get_run(run.id)
        elif action == "cancel":
            run = await store.update_run_status(run.id, run.version, RunStatus.CANCELLED,
                                                event=EventSpec("RUN_CANCELLED", {"by": "user"}))
        elif action == "recover":
            expire_lease(store, run.id)
            recovered = await TaskRecovery(store, billing).scan()
            assert run.id in recovered
            await engine.run(run.id)
            run = await store.get_run(run.id)
        elif action == "simulate_unknown_no_effect":
            # 模拟：写工具 RUNNING 时崩溃、请求未到远端（幂等键已先行落库）
            await store.update_step_status(run.id, "SUBMIT_REFUND",
                                           StepStatus.PENDING, StepStatus.READY)
            await store.update_step_status(run.id, "SUBMIT_REFUND",
                                           StepStatus.READY, StepStatus.RUNNING)
            await store.set_step_idempotency_key(run.id, "SUBMIT_REFUND", "simulated-key")
            await store.update_step_status(run.id, "SUBMIT_REFUND",
                                           StepStatus.RUNNING, StepStatus.OUTCOME_UNKNOWN)
            run = await store.get_run(run.id)
        else:
            raise AssertionError(f"未知 action: {action}")

    expect = case["expect"]
    # 最终状态与错误码
    assert run.status.value == expect["final_status"], case["id"]
    if "error_code" in expect:
        assert run.error_code == expect["error_code"], case["id"]
    # 关键 Step 达成
    steps = {s.step_key: s for s in await store.get_steps(run.id)}
    for key in expect.get("key_steps", []):
        assert steps[key].status == StepStatus.SUCCEEDED, f"{case['id']}: {key}"
    # 工具调用与不调用
    tools = await collected_tool_events(store, run.id)
    for tool in expect.get("tools_called", []):
        assert tool in tools, f"{case['id']}: 期望调用 {tool}，实际 {tools}"
    for tool in expect.get("tools_not_called", []):
        assert tool not in tools, f"{case['id']}: 不应调用 {tool}"
    # 审批决定
    if "approval_decision" in expect:
        approval = await store.get_approval(run.id, "APPROVAL_GATE")
        assert approval is not None and approval.decision.value == expect["approval_decision"]
    # 退款记录数量与金额
    assert refund_count(billing) == expect.get("refund_count", 0), case["id"]
    if "refund_amount_cents" in expect:
        assert refund_amount(billing) == expect["refund_amount_cents"], case["id"]
    # 回答中的关键事实
    response = (run.response_json or {}).get("response", "")
    for phrase in expect.get("answer_contains", []):
        assert phrase in response, f"{case['id']}: 回答缺少「{phrase}」"
