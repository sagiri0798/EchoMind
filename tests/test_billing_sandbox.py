"""沙箱账单客户端测试：属主隔离、查询稳定性、退款幂等与故障注入（PR3/PR5 验收）。

不依赖真实外部服务、Redis 或模型；每次测试使用独立临时 SQLite 文件。
"""
import pytest
import pytest_asyncio

from runtime.billing import (
    BillingOutcomeUnknown,
    BillingUnavailable,
    FAIL_AFTER_EFFECT_BEFORE_RESPONSE,
    FAIL_BEFORE_EFFECT,
    RefundConflictError,
    SandboxBillingClient,
    demo_seed,
)


@pytest_asyncio.fixture
async def billing(tmp_path):
    client = SandboxBillingClient(tmp_path / "billing.db")
    await client.bootstrap()
    return client


@pytest_asyncio.fixture
async def seeded(billing):
    orders, charges = demo_seed()
    await billing.seed(orders, charges)
    return billing


# ── 查询与属主隔离 ─────────────────────────────────────


@pytest.mark.asyncio
async def test_get_order_returns_full_fields(billing):
    await billing.seed([{"order_id": "o1", "user_id": "u1", "amount_cents": 9900,
                         "status": "paid", "created_at": "2026-11-01T10:00:00"}], [])
    order = await billing.get_order("o1", "u1")
    assert order["order_id"] == "o1"
    assert order["amount_cents"] == 9900 and order["status"] == "paid"
    assert await billing.get_order("no-such-order", "u1") is None


@pytest.mark.asyncio
async def test_cross_user_order_is_invisible(seeded):
    # u2 查询 u1 的订单：按不存在处理，不暴露资源存在性
    assert await seeded.get_order("12345", "u2") is None
    assert await seeded.get_charges("12345", "u2") == []
    # 拥有者正常可见
    assert (await seeded.get_order("12345", "u1"))["order_id"] == "12345"


@pytest.mark.asyncio
async def test_get_charges_ordered_by_paid_at(seeded):
    charges = await seeded.get_charges("12345", "u1")
    assert [c["charge_id"] for c in charges] == ["chg-001", "chg-002"]
    assert all(c["amount_cents"] == 9900 for c in charges)
    assert [c["channel"] for c in charges] == ["wechat", "wechat"]


@pytest.mark.asyncio
async def test_repeated_queries_are_stable(seeded):
    first = await seeded.get_charges("12345", "u1")
    second = await seeded.get_charges("12345", "u1")
    assert first == second
    order = await seeded.get_order("12345", "u1")
    assert order == await seeded.get_order("12345", "u1")


@pytest.mark.asyncio
async def test_demo_seed_contains_duplicate_charge_scenario(seeded):
    # 旗舰场景：u1/12345 有两笔相同金额扣款（重复扣款）
    assert len(await seeded.get_charges("12345", "u1")) == 2
    # u2 的订单正常存在且彼此不可见
    assert (await seeded.get_order("99999", "u2"))["user_id"] == "u2"
    assert await seeded.get_order("99999", "u1") is None


@pytest.mark.asyncio
async def test_bootstrap_is_idempotent_and_preserves_data(seeded, tmp_path):
    second = SandboxBillingClient(tmp_path / "billing.db")
    await second.bootstrap()  # 第二次启动不报错，且原数据仍在
    assert (await second.get_order("12345", "u1"))["order_id"] == "12345"


@pytest.mark.asyncio
async def test_seed_is_replace_idempotent(billing):
    orders, charges = demo_seed()
    await billing.seed(orders, charges)
    await billing.seed(orders, charges)  # 重复播种不产生重复记录
    assert len(await billing.get_charges("12345", "u1")) == 2


# ── 退款写入：幂等与冲突（PR5）──────────────────────────


@pytest.mark.asyncio
async def test_submit_refund_creates_record_and_is_idempotent(seeded):
    record = await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-1")
    assert record["status"] == "submitted" and record["operation_id"] == "op-1"
    # 相同 operation_id 重复提交：返回相同退款记录，不产生第二条
    again = await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-1")
    assert again["created_at"] == record["created_at"]


@pytest.mark.asyncio
async def test_submit_refund_conflicts_on_same_operation_id_different_params(seeded):
    await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-1")
    with pytest.raises(RefundConflictError):
        await seeded.submit_refund("12345", "u1", 5000, "别的理由", operation_id="op-1")


@pytest.mark.asyncio
async def test_submit_refund_requires_order_ownership(seeded):
    with pytest.raises(RefundConflictError):
        await seeded.submit_refund("12345", "u2", 9900, "重复扣款", operation_id="op-x")
    with pytest.raises(RefundConflictError):
        await seeded.submit_refund("不存在", "u1", 9900, "重复扣款", operation_id="op-y")


@pytest.mark.asyncio
async def test_refund_status_ladder_and_isolation(seeded):
    await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-1")
    assert (await seeded.get_refund_status("op-1", "u1"))["status"] == "processing"
    assert (await seeded.get_refund_status("op-1", "u1"))["status"] == "succeeded"
    # 未执行或跨用户查询均按不存在处理
    assert await seeded.get_refund_status("op-none", "u1") is None
    assert await seeded.get_refund_status("op-1", "u2") is None


# ── 故障注入（仅测试）───────────────────────────────────


@pytest.mark.asyncio
async def test_fault_before_effect_leaves_no_record(seeded):
    seeded.enable_fault(FAIL_BEFORE_EFFECT)
    with pytest.raises(BillingUnavailable):
        await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-f1")
    assert await seeded.get_refund_status("op-f1", "u1") is None  # 副作用未发生


@pytest.mark.asyncio
async def test_fault_after_effect_before_response_leaves_record(seeded):
    seeded.enable_fault(FAIL_AFTER_EFFECT_BEFORE_RESPONSE)
    with pytest.raises(BillingOutcomeUnknown):
        await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-f2")
    # 副作用已发生：记录存在，可通过查询对账
    record = await seeded.get_refund_status("op-f2", "u1")
    assert record is not None and record["order_id"] == "12345"
