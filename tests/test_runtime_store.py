"""Runtime 存储层单元测试：Run/Step 的创建、读取、版本更新、租约、状态转换与事件记录。

本文件是 PR1 的验收测试：非法状态转换和旧 owner 写入必须被拒绝。
只依赖临时 SQLite 文件，不依赖 Redis、模型或外网。
"""
import sqlite3
import uuid
from datetime import datetime, timedelta

import pytest
import pytest_asyncio

from runtime.models import Run, RunStatus, RunStep, StepStatus
from runtime.store import (
    DuplicateRunError,
    IllegalTransitionError,
    LeaseConflictError,
    LeaseLostError,
    RunNotFoundError,
    RunStore,
    StepExistsError,
    StepNotFoundError,
    VersionConflictError,
)


def make_run_fields(**overrides):
    """构造一个合法 Run 的字段字典，测试里按需覆盖。"""
    fields = dict(
        user_id="u1",
        conv_id="c1",
        client_request_id="msg-001",
        message_hash="hash-001",
        task_type="duplicate_charge_refund",
        plan_version="v1",
        status=RunStatus.CREATED,
        context_json={"entities": {}},
        input_json={"message": "订单 #12345 被扣了两次款"},
    )
    fields.update(overrides)
    return fields


def make_step_fields(step_key, ordinal, **overrides):
    """构造一个合法 Step 的 RunStep 对象，run_id 由 create_steps 统一填充。"""
    fields = dict(
        run_id="",
        step_key=step_key,
        ordinal=ordinal,
        tool_name=None,
        status=StepStatus.PENDING,
    )
    fields.update(overrides)
    return RunStep(**fields)


@pytest_asyncio.fixture
async def store(tmp_path):
    """每个测试使用独立的临时 SQLite 文件。"""
    db = RunStore(tmp_path / "runtime.db")
    await db.bootstrap()
    return db


# ── 创建与读取 ────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_and_get_run_roundtrip(store):
    run = await store.create_run(**make_run_fields())
    assert run.id and run.version == 0
    assert run.status == RunStatus.CREATED
    assert run.created_at and run.updated_at
    assert run.finished_at is None

    loaded = await store.get_run(run.id)
    assert loaded == run
    # JSON 字段完整往返
    assert loaded.context_json == {"entities": {}}
    assert loaded.input_json["message"] == "订单 #12345 被扣了两次款"
    # RUN_CREATED 事件随创建一并落库
    events = await store.get_events(run.id)
    assert [e.event_type for e in events] == ["RUN_CREATED"]


@pytest.mark.asyncio
async def test_get_missing_run_raises(store):
    with pytest.raises(RunNotFoundError):
        await store.get_run("missing-run-id")


@pytest.mark.asyncio
async def test_duplicate_request_key_is_rejected_and_findable(store):
    first = await store.create_run(**make_run_fields())
    with pytest.raises(DuplicateRunError) as exc:
        await store.create_run(**make_run_fields())
    assert exc.value.run_id == first.id

    found = await store.find_run("u1", "c1", "msg-001")
    assert found is not None and found.id == first.id
    assert await store.find_run("u1", "c1", "other-msg") is None


# ── 状态转换与版本 ─────────────────────────────────────


@pytest.mark.asyncio
async def test_legal_transition_updates_status_and_version(store):
    run = await store.create_run(**make_run_fields())
    updated = await store.update_run_status(run.id, expected_version=0, next_status=RunStatus.RUNNING)
    assert updated.status == RunStatus.RUNNING
    assert updated.version == 1
    assert (await store.get_run(run.id)).version == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("next_status", list(RunStatus))
async def test_created_only_transitions_to_running(store, next_status):
    run = await store.create_run(**make_run_fields())
    if next_status == RunStatus.RUNNING:
        await store.update_run_status(run.id, 0, next_status)  # 唯一合法转换
    else:
        with pytest.raises(IllegalTransitionError):
            await store.update_run_status(run.id, 0, next_status)
        assert (await store.get_run(run.id)).status == RunStatus.CREATED


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", [RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED])
async def test_terminal_status_cannot_transition_back(store, terminal):
    run = await store.create_run(**make_run_fields())
    run = await store.update_run_status(run.id, 0, RunStatus.RUNNING)
    run = await store.update_run_status(run.id, 1, terminal)
    assert run.finished_at is not None
    for next_status in RunStatus:
        with pytest.raises(IllegalTransitionError):
            await store.update_run_status(run.id, run.version, next_status)


@pytest.mark.asyncio
async def test_stale_version_write_is_rejected(store):
    run = await store.create_run(**make_run_fields())
    await store.update_run_status(run.id, 0, RunStatus.RUNNING)
    # 用旧版本再提交一次，必须被拒绝，且状态不变
    with pytest.raises(VersionConflictError):
        await store.update_run_status(run.id, expected_version=0, next_status=RunStatus.WAITING_USER)
    assert (await store.get_run(run.id)).status == RunStatus.RUNNING


@pytest.mark.asyncio
async def test_transition_can_record_event_in_same_transaction(store):
    run = await store.create_run(**make_run_fields())
    await store.update_run_status(
        run.id, 0, RunStatus.RUNNING,
        event_type="RUN_RESUMED", event_payload={"reason": "测试"},
    )
    events = await store.get_events(run.id)
    assert [e.event_type for e in events] == ["RUN_CREATED", "RUN_RESUMED"]
    assert events[-1].payload_json == {"reason": "测试"}


# ── 租约 ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_lease_acquire_renew_and_foreign_rejection(store):
    run = await store.create_run(**make_run_fields())
    leased = await store.try_acquire_lease(run.id, owner="worker-1", expected_version=0, lease_s=30)
    assert leased.lease_owner == "worker-1"
    assert leased.version == 1

    # 同一 owner 续租：租期延长、版本递增
    renewed = await store.try_acquire_lease(run.id, owner="worker-1", expected_version=1, lease_s=30)
    assert renewed.lease_until >= leased.lease_until

    # 其他 owner 在有效租期内不能抢占
    with pytest.raises(LeaseConflictError):
        await store.try_acquire_lease(run.id, owner="worker-2", expected_version=2, lease_s=30)


@pytest.mark.asyncio
async def test_expired_lease_blocks_stale_owner_writes_but_allows_takeover(store):
    run = await store.create_run(**make_run_fields())
    await store.try_acquire_lease(run.id, owner="worker-1", expected_version=0, lease_s=30)

    # 直接用 SQL 把租约改成已过期，模拟 worker 卡死超过租期
    conn = sqlite3.connect(store.path)
    conn.execute(
        "UPDATE runs SET lease_until=? WHERE id=?",
        ((datetime.now() - timedelta(seconds=1)).isoformat(), run.id),
    )
    conn.commit()
    conn.close()
    stale = await store.get_run(run.id)

    # 过期持有者不能再提交状态
    with pytest.raises(LeaseLostError):
        await store.update_run_status(
            run.id, expected_version=stale.version, next_status=RunStatus.RUNNING, lease_owner="worker-1",
        )
    assert (await store.get_run(run.id)).status == RunStatus.CREATED

    # 新 worker 可以接管过期租约
    taken = await store.try_acquire_lease(run.id, owner="worker-2", expected_version=stale.version, lease_s=30)
    assert taken.lease_owner == "worker-2"


@pytest.mark.asyncio
async def test_leased_run_write_requires_matching_owner(store):
    run = await store.create_run(**make_run_fields())
    run = await store.try_acquire_lease(run.id, owner="worker-1", expected_version=0, lease_s=30)
    # 不是租约持有者（或 API 路径未传 owner）时，有效租期内的写入被拒绝
    with pytest.raises(LeaseConflictError):
        await store.update_run_status(run.id, run.version, RunStatus.RUNNING, lease_owner="worker-2")
    # 持有者本人可以正常提交
    ok = await store.update_run_status(run.id, run.version, RunStatus.RUNNING, lease_owner="worker-1")
    assert ok.status == RunStatus.RUNNING


# ── 取消与迁移 ────────────────────────────────────────


@pytest.mark.asyncio
async def test_request_cancel_sets_flag_and_event_and_is_idempotent(store):
    run = await store.create_run(**make_run_fields())
    run = await store.update_run_status(run.id, 0, RunStatus.RUNNING)
    updated = await store.request_cancel(run.id, expected_version=run.version)
    assert updated.cancel_requested and updated.status == RunStatus.RUNNING
    assert (await store.get_events(run.id))[-1].event_type == "CANCEL_REQUESTED"
    # 幂等：重复请求不递增版本
    again = await store.request_cancel(run.id, expected_version=updated.version)
    assert again.version == updated.version


@pytest.mark.asyncio
async def test_request_cancel_requires_running_status(store):
    run = await store.create_run(**make_run_fields())
    with pytest.raises(IllegalTransitionError):
        await store.request_cancel(run.id, expected_version=0)


@pytest.mark.asyncio
async def test_migration_adds_cancel_requested_column(tmp_path):
    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE runs (id TEXT PRIMARY KEY)")  # v1 简化模拟
    conn.execute("PRAGMA user_version=1")
    conn.commit()
    conn.close()
    store = RunStore(path)
    await store.bootstrap()
    conn = sqlite3.connect(path)
    columns = [row[1] for row in conn.execute("PRAGMA table_info(runs)")]
    assert "cancel_requested" in columns
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 2
    conn.close()


# ── 审批、幂等键与补充输入（PR5）──────────────────────


@pytest.mark.asyncio
async def test_approval_create_is_idempotent_and_decide_once(store):
    from runtime.models import ApprovalDecision
    from runtime.store import ApprovalAlreadyDecidedError
    run = await store.create_run(**make_run_fields())
    first = await store.create_approval(run.id, "APPROVAL_GATE", "风险摘要")
    second = await store.create_approval(run.id, "APPROVAL_GATE", "别的摘要")
    assert first.id == second.id  # 幂等：不覆盖已存在的审批
    assert (await store.get_approval(run.id, "APPROVAL_GATE")).risk_summary == "风险摘要"

    decided = await store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.APPROVED,
                                          decided_by="admin", comment="金额一致")
    assert decided.decision == ApprovalDecision.APPROVED and decided.decided_at
    # 审批只能决定一次
    with pytest.raises(ApprovalAlreadyDecidedError):
        await store.decide_approval(run.id, "APPROVAL_GATE", ApprovalDecision.REJECTED)
    events = await store.get_events(run.id)
    assert any(e.event_type == "APPROVAL_DECIDED" for e in events)


@pytest.mark.asyncio
async def test_append_run_input_and_version(store):
    run = await store.create_run(**make_run_fields())
    updated = await store.append_run_input(run.id, run.version, "订单号是 #12345")
    assert updated.input_json["supplements"] == ["订单号是 #12345"]
    assert updated.version == run.version + 1
    with pytest.raises(VersionConflictError):
        await store.append_run_input(run.id, run.version, "旧版本写入")


@pytest.mark.asyncio
async def test_set_step_idempotency_key_persists_without_status_change(store):
    run = await store.create_run(**make_run_fields())
    await store.create_steps(run.id, [make_step_fields("SUBMIT_REFUND", 0)])
    step = await store.set_step_idempotency_key(run.id, "SUBMIT_REFUND", "key-abc")
    assert step.idempotency_key == "key-abc"
    loaded = await store.get_step(run.id, "SUBMIT_REFUND")
    assert loaded.idempotency_key == "key-abc" and loaded.status == StepStatus.PENDING


@pytest.mark.asyncio
async def test_list_active_runs_filters_terminal(store):
    run = await store.create_run(**make_run_fields())
    assert [r.id for r in await store.list_active_runs()] == [run.id]
    await store.update_run_status(run.id, 0, RunStatus.RUNNING)
    await store.update_run_status(run.id, 1, RunStatus.SUCCEEDED)
    assert await store.list_active_runs() == []


# ── Step ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_and_list_steps_in_ordinal_order(store):
    run = await store.create_run(**make_run_fields())
    steps = [
        make_step_fields("LOAD_ORDER", 2),
        make_step_fields("COLLECT_CONTEXT", 0),
        make_step_fields("LOAD_CHARGES", 1),
    ]
    await store.create_steps(run.id, steps)
    loaded = await store.get_steps(run.id)
    assert [s.step_key for s in loaded] == ["COLLECT_CONTEXT", "LOAD_CHARGES", "LOAD_ORDER"]
    assert all(s.id for s in loaded)


@pytest.mark.asyncio
async def test_duplicate_step_key_is_rejected(store):
    run = await store.create_run(**make_run_fields())
    await store.create_steps(run.id, [make_step_fields("LOAD_ORDER", 0)])
    with pytest.raises(StepExistsError):
        await store.create_steps(run.id, [make_step_fields("LOAD_ORDER", 1)])


@pytest.mark.asyncio
async def test_step_transition_and_illegal_transition(store):
    run = await store.create_run(**make_run_fields())
    await store.create_steps(run.id, [make_step_fields("LOAD_ORDER", 0)])
    step = await store.update_step_status(run.id, "LOAD_ORDER", StepStatus.PENDING, StepStatus.READY)
    assert step.status == StepStatus.READY
    with pytest.raises(IllegalTransitionError):
        await store.update_step_status(run.id, "LOAD_ORDER", StepStatus.READY, StepStatus.SUCCEEDED)
    assert (await store.get_step(run.id, "LOAD_ORDER")).status == StepStatus.READY


@pytest.mark.asyncio
async def test_step_cas_and_missing_step(store):
    run = await store.create_run(**make_run_fields())
    await store.create_steps(run.id, [make_step_fields("LOAD_ORDER", 0)])
    # 期望状态不匹配时拒绝（CAS）
    with pytest.raises(IllegalTransitionError):
        await store.update_step_status(run.id, "LOAD_ORDER", StepStatus.RUNNING, StepStatus.SUCCEEDED)
    with pytest.raises(StepNotFoundError):
        await store.update_step_status(run.id, "NO_SUCH_STEP", StepStatus.PENDING, StepStatus.READY)


@pytest.mark.asyncio
async def test_bootstrap_is_idempotent(store, tmp_path):
    run = await store.create_run(**make_run_fields())
    db2 = RunStore(tmp_path / "runtime.db")
    await db2.bootstrap()  # 第二次启动不报错，且原数据仍在
    assert (await db2.get_run(run.id)).id == run.id
