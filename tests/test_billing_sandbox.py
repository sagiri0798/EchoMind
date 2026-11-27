"""沙箱账单客户端测试：属主隔离、查询稳定性、退款幂等与故障注入（PR3/PR5 验收）。

不依赖真实外部服务、Redis 或模型；每次测试使用独立临时 SQLite 文件。
"""
import asyncio
import sqlite3

import pytest
import pytest_asyncio

from runtime.billing import (
    BillingOutcomeUnknown,
    BillingUnavailable,
    FAIL_AFTER_EFFECT_BEFORE_RESPONSE,
    FAIL_BEFORE_EFFECT,
    RefundConflictError,
    RefundLimitExceeded,
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


@pytest.mark.asyncio
async def test_refund_over_order_amount_reaches_failed_terminal(seeded):
    """S10/10：状态阶梯必须能到达 failed 终态，且终态不再上升。"""
    record = await seeded.submit_refund("12345", "u1", 10000, "超额退款", operation_id="op-over")
    assert record["status"] == "failed"  # 超过订单金额 9900 → 业务失败
    again = await seeded.get_refund_status("op-over", "u1")
    assert again is not None and again["status"] == "failed"  # failed 是终态，不再升级
    # 合法金额仍走正常阶梯
    ok = await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-ok")
    assert ok["status"] == "submitted"


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


# ── 订单级累计退款闸门（跨 Run）────────────────────────


def refund_rows(billing) -> list:
    conn = sqlite3.connect(billing.path)
    try:
        return conn.execute(
            "SELECT operation_id, amount_cents, status FROM refunds ORDER BY created_at"
        ).fetchall()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_second_full_refund_on_same_order_is_blocked(seeded):
    """两个 Run 对同一订单各退全额：第二笔被订单级闸门拦截，不产生记录。"""
    await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-r1")
    with pytest.raises(RefundLimitExceeded):
        await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-r2")
    assert len(refund_rows(seeded)) == 1


@pytest.mark.asyncio
async def test_partial_refunds_over_order_total_are_blocked(seeded):
    """两笔部分退款累计超过订单金额：第二笔被拦；未超的第三笔允许。"""
    await seeded.submit_refund("12345", "u1", 6000, "部分退款", operation_id="op-p1")
    with pytest.raises(RefundLimitExceeded):
        await seeded.submit_refund("12345", "u1", 6000, "部分退款", operation_id="op-p2")
    assert len(refund_rows(seeded)) == 1
    # 6000 + 3000 = 9000 ≤ 9900，允许
    record = await seeded.submit_refund("12345", "u1", 3000, "部分退款", operation_id="op-p3")
    assert record["status"] == "submitted"
    assert len(refund_rows(seeded)) == 2
    # 6000 + 3000 + 1000 = 10000 > 9900，拒绝
    with pytest.raises(RefundLimitExceeded):
        await seeded.submit_refund("12345", "u1", 1000, "部分退款", operation_id="op-p4")


@pytest.mark.asyncio
async def test_multiple_partial_refunds_within_order_total_allowed(seeded):
    """多笔合法部分退款只要累计不超订单额就允许（不能简单按同订单拦截）。"""
    first = await seeded.submit_refund("12345", "u1", 3000, "部分退款", operation_id="op-m1")
    second = await seeded.submit_refund("12345", "u1", 5000, "部分退款", operation_id="op-m2")
    assert first["status"] == second["status"] == "submitted"
    assert len(refund_rows(seeded)) == 2


@pytest.mark.asyncio
async def test_failed_refund_does_not_consume_order_limit(seeded):
    """failed 终态不计入累计额度，失败后允许重新发起合法退款。"""
    failed = await seeded.submit_refund("12345", "u1", 10000, "超额", operation_id="op-fail")
    assert failed["status"] == "failed"
    ok = await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-retry")
    assert ok["status"] == "submitted"
    assert len(refund_rows(seeded)) == 2


@pytest.mark.asyncio
async def test_same_operation_id_replay_remains_idempotent_after_full_refund(seeded):
    """幂等重放优先于累计校验：同一 operation_id 重复提交返回原记录，不会被自己拦住。"""
    first = await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-idem")
    again = await seeded.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-idem")
    assert again["created_at"] == first["created_at"]
    assert len(refund_rows(seeded)) == 1


@pytest.mark.asyncio
async def test_concurrent_full_refunds_do_not_oversell(seeded):
    """BEGIN IMMEDIATE 串行化并发写入：两笔同订单全额退款只有一笔成功。"""
    results = await asyncio.gather(
        seeded.submit_refund("12345", "u1", 9900, "并发退款", operation_id="op-c1"),
        seeded.submit_refund("12345", "u1", 9900, "并发退款", operation_id="op-c2"),
        return_exceptions=True,
    )
    successes = [r for r in results if isinstance(r, dict)]
    failures = [r for r in results if isinstance(r, RefundLimitExceeded)]
    assert len(successes) == 1 and len(failures) == 1
    assert len(refund_rows(seeded)) == 1


# ── 可退上限 = 多收金额（总扣款 - 订单金额）──────────────


async def seed_custom_order(billing, order_id, order_amount, charge_amounts):
    await billing.seed(
        [{"order_id": order_id, "user_id": "u1", "amount_cents": order_amount,
          "status": "paid", "created_at": "2026-11-01T10:00:00"}],
        [{"charge_id": f"{order_id}-chg-{i}", "order_id": order_id, "user_id": "u1",
          "amount_cents": amount, "paid_at": f"2026-11-01T10:0{i}:00",
          "channel": "wechat"} for i, amount in enumerate(charge_amounts)])


@pytest.mark.asyncio
async def test_refund_cap_uses_overcharge_not_order_amount(seeded):
    """订单 5000 + 两笔 5000（多收 5000）→ 退 5000；再退 1 分被拦。"""
    await seed_custom_order(seeded, "OC5000", 5000, [5000, 5000])
    record = await seeded.submit_refund("OC5000", "u1", 5000, "退还多收", operation_id="op-oc1")
    assert record["status"] == "submitted"
    with pytest.raises(RefundLimitExceeded):
        await seeded.submit_refund("OC5000", "u1", 1, "再退", operation_id="op-oc2")


@pytest.mark.asyncio
async def test_refund_cap_partial_overcharge(seeded):
    """订单 10000 + 5000+5000+3000（多收 3000）→ 最多退 3000。"""
    await seed_custom_order(seeded, "OC13000", 10000, [5000, 5000, 3000])
    record = await seeded.submit_refund("OC13000", "u1", 3000, "退还多收", operation_id="op-oc3")
    assert record["status"] == "submitted"
    with pytest.raises(RefundLimitExceeded):
        await seeded.submit_refund("OC13000", "u1", 1, "再退", operation_id="op-oc4")
    with pytest.raises(RefundLimitExceeded):
        await seeded.submit_refund("OC13000", "u1", 3000, "再退", operation_id="op-oc5")


@pytest.mark.asyncio
async def test_no_overcharge_means_no_refund(seeded):
    """订单 10000 + 3000+3000（未超收）→ 单笔超可退上限，落 failed 终态，不产生退款。"""
    await seed_custom_order(seeded, "OC6000", 10000, [3000, 3000])
    record = await seeded.submit_refund("OC6000", "u1", 3000, "退款", operation_id="op-oc6")
    assert record["status"] == "failed"
    assert [row[2] for row in refund_rows(seeded) if row[2] != "failed"] == []


@pytest.mark.asyncio
async def test_refund_may_exceed_order_amount_up_to_overcharge(seeded):
    """订单 1000 + 两笔 1500（多收 2000）：1500 > 订单额但 ≤ overcharge，应允许。"""
    await seed_custom_order(seeded, "OC1500", 1000, [1500, 1500])
    record = await seeded.submit_refund("OC1500", "u1", 1500, "退还多收", operation_id="op-oc7")
    assert record["status"] == "submitted"  # 不再因「超过订单金额」误落 failed


@pytest.mark.asyncio
async def test_non_positive_refund_amount_is_rejected(seeded):
    """amount_cents <= 0 直接拒绝，不产生退款记录。"""
    for index, amount in enumerate((0, -100)):
        with pytest.raises(RefundLimitExceeded):
            await seeded.submit_refund("12345", "u1", amount, "非法金额",
                                       operation_id=f"op-bad-{index}")
    assert refund_rows(seeded) == []
