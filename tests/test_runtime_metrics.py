"""Runtime 指标回归（实施计划 12.2 节）。

指标注册在 prometheus_client 默认 REGISTRY；测试通过标签采样差量验证
引擎与 API 埋点，不依赖真实模型/支付/外网。
"""
import pytest
import pytest_asyncio

from runtime import metrics
from runtime.billing import (
    FAIL_AFTER_EFFECT_BEFORE_RESPONSE,
    SandboxBillingClient,
    demo_seed,
)
from runtime.engine import TaskEngine
from runtime.models import ApprovalDecision, RunStatus
from runtime.store import RunStore


def value(counter, **labels):
    return counter.labels(**labels)._value.get()


def count(histogram, **labels):
    """通过公共 collect() 接口取指定标签的观察次数。"""
    for metric in histogram.collect():
        for sample in metric.samples:
            if sample.name.endswith("_count") \
                    and all(sample.labels.get(key) == val for key, val in labels.items()):
                return sample.value
    return 0


@pytest_asyncio.fixture
async def env(tmp_path):
    store = RunStore(tmp_path / "runtime.db")
    await store.bootstrap()
    billing = SandboxBillingClient(tmp_path / "billing.db")
    await billing.bootstrap()
    await billing.seed(*demo_seed())

    async def search(params, ctx):
        return [{"title": "退款政策", "content": "重复扣款可在 7 日内申请原路退回"}]

    engine = TaskEngine(store, billing, tools={"search_knowledge_base": search})
    return store, billing, engine


async def create_run(store, message="订单 #12345 被扣了两次款，99 元，昨天支付的",
                     client_request_id="msg-001"):
    return await store.create_run(
        user_id="u1", conv_id="c1", client_request_id=client_request_id, message_hash="h",
        task_type="duplicate_charge_refund", plan_version="v1",
        input_json={"message": message}, context_json={})


@pytest.mark.asyncio
async def test_success_flow_increments_runs_and_steps(env):
    store, billing, engine = env
    before_runs = value(metrics.runs_total, task_type="duplicate_charge_refund",
                        status="SUCCEEDED")
    before_steps = count(metrics.step_duration, step="LOAD_ORDER", status="SUCCEEDED")

    run = await create_run(store)
    await engine.run(run.id)
    run = await store.get_run(run.id)
    await store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.APPROVED,
                                decided_by="admin")
    await store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                  event_type="RUN_RESUMED")
    await engine.run(run.id)

    assert value(metrics.runs_total, task_type="duplicate_charge_refund",
                 status="SUCCEEDED") == before_runs + 1
    assert count(metrics.step_duration, step="LOAD_ORDER", status="SUCCEEDED") \
        == before_steps + 1


@pytest.mark.asyncio
async def test_outcome_unknown_and_duplicate_prevented_metrics(env):
    store, billing, engine = env
    before_unknown = value(metrics.outcome_unknown_total, task_type="duplicate_charge_refund")
    before_dup = value(metrics.duplicate_prevented_total, tool="submit_refund")

    run = await create_run(store)
    await engine.run(run.id)
    run = await store.get_run(run.id)
    await store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.APPROVED,
                                decided_by="admin")
    await store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                  event_type="RUN_RESUMED")
    billing.enable_fault(FAIL_AFTER_EFFECT_BEFORE_RESPONSE)
    await engine.run(run.id)
    assert value(metrics.outcome_unknown_total,
                 task_type="duplicate_charge_refund") == before_unknown + 1

    # 重新调度：对账后幂等重提被拦截（不产生新退款）
    run = await store.get_run(run.id)
    await engine.run(run.id)
    assert value(metrics.duplicate_prevented_total, tool="submit_refund") == before_dup + 1


@pytest.mark.asyncio
async def test_failed_run_and_approval_metrics(env):
    store, billing, engine = env
    before_failed = value(metrics.runs_total, task_type="duplicate_charge_refund",
                          status="FAILED")
    run = await create_run(store, message="订单 #00000 被扣了两次款，99 元，昨天")
    await engine.run(run.id)
    assert value(metrics.runs_total, task_type="duplicate_charge_refund",
                 status="FAILED") == before_failed + 1

    # 审批拒绝：引擎拒绝路径计数 + 审批决定计数
    run = await create_run(store, client_request_id="msg-002")
    await engine.run(run.id)
    run = await store.get_run(run.id)
    await store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.REJECTED,
                                decided_by="admin")
    before_reject = value(metrics.runs_total, task_type="duplicate_charge_refund",
                          status="FAILED")
    run = await store.update_run_status(run.id, run.version, RunStatus.RUNNING,
                                        event_type="RUN_RESUMED")
    await engine.run(run.id)
    assert value(metrics.runs_total, task_type="duplicate_charge_refund",
                 status="FAILED") == before_reject + 1
