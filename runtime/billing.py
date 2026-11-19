"""沙箱账单/退款系统：模拟外部业务系统的独立 SQLite 数据库（实施计划第 10 节）。

边界：与 RunStore 使用不同的数据库文件，明确"外部系统"不属于任务状态；
查询一律校验 user_id 与订单归属，跨用户数据按不存在处理（不可见）。

退款写入保证：
- operation_id 唯一（幂等键 = 业务幂等键）；
- 相同 operation_id 重复提交返回相同退款记录；参数不同则冲突；
- 状态阶梯 submitted → processing → succeeded/failed；
- 故障注入只通过客户端构造参数/方法开关，生产代码路径不读取用户可控参数。
"""
import asyncio
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Iterator, List, Optional

# 故障注入模式（仅测试使用，10 节）
FAIL_BEFORE_EFFECT = "FAIL_BEFORE_EFFECT"
FAIL_AFTER_EFFECT_BEFORE_RESPONSE = "FAIL_AFTER_EFFECT_BEFORE_RESPONSE"
FAIL_AFTER_RESPONSE_BEFORE_CHECKPOINT = "FAIL_AFTER_RESPONSE_BEFORE_CHECKPOINT"
DELAYED_SUCCESS = "DELAYED_SUCCESS"
# 说明：从外部系统视角，AFTER_EFFECT_BEFORE_RESPONSE 与 AFTER_RESPONSE_BEFORE_CHECKPOINT
# 留下同一状态（记录已写入、客户端未确认）；后者用于引擎侧"收到响应但检查点写入前崩溃"的模拟测试。

_SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
  order_id     TEXT PRIMARY KEY,
  user_id      TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  status       TEXT NOT NULL,
  created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS charges (
  charge_id    TEXT PRIMARY KEY,
  order_id     TEXT NOT NULL REFERENCES orders(order_id),
  user_id      TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  paid_at      TEXT NOT NULL,
  channel      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS refunds (
  operation_id TEXT PRIMARY KEY,
  order_id     TEXT NOT NULL,
  user_id      TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  reason       TEXT NOT NULL,
  status       TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_charges_order ON charges(order_id);
CREATE INDEX IF NOT EXISTS idx_refunds_order ON refunds(order_id);
"""


def _row_dict(row: sqlite3.Row) -> dict:
    return {key: row[key] for key in row.keys()}


class BillingUnavailable(RuntimeError):
    """外部账单系统不可用（副作用未发生）。"""


class BillingOutcomeUnknown(RuntimeError):
    """外部系统可能已执行但响应丢失（副作用可能已发生）。"""

    def __init__(self, operation_id: str):
        super().__init__(f"退款提交结果未知: {operation_id}")
        self.operation_id = operation_id


class RefundConflictError(RuntimeError):
    """同一 operation_id 携带不同参数重复提交。"""


class SandboxBillingClient:
    """沙箱账单客户端。只读查询属主隔离；写操作以 operation_id 幂等。"""

    def __init__(self, path, fault_injection: Optional[str] = None):
        self._path = str(path)
        self._fault = fault_injection

    @property
    def path(self) -> str:
        """数据库文件路径，便于排查与测试。"""
        return self._path

    def enable_fault(self, mode: str) -> None:
        """启用一次性故障注入（仅测试）。"""
        self._fault = mode

    def disable_fault(self) -> None:
        self._fault = None

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._path, timeout=5.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            yield conn
        finally:
            conn.close()

    async def bootstrap(self) -> None:
        """建表。幂等：重复调用不报错、不影响已有数据。"""
        await asyncio.to_thread(self._bootstrap)

    def _bootstrap(self) -> None:
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    # ── 种子数据 ──────────────────────────────────────

    async def seed(self, orders: List[dict], charges: List[dict]) -> None:
        """写入种子订单与扣款记录（INSERT OR REPLACE，重复调用结果一致）。"""
        await asyncio.to_thread(self._seed, orders, charges)

    def _seed(self, orders: List[dict], charges: List[dict]) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for order in orders:
                conn.execute(
                    "INSERT OR REPLACE INTO orders (order_id, user_id, amount_cents, status, created_at) "
                    "VALUES (:order_id, :user_id, :amount_cents, :status, :created_at)",
                    order,
                )
            for charge in charges:
                conn.execute(
                    "INSERT OR REPLACE INTO charges "
                    "(charge_id, order_id, user_id, amount_cents, paid_at, channel) "
                    "VALUES (:charge_id, :order_id, :user_id, :amount_cents, :paid_at, :channel)",
                    charge,
                )
            conn.execute("COMMIT")

    # ── 只读查询（属主隔离）────────────────────────────

    async def get_order(self, order_id: str, user_id: str) -> Optional[dict]:
        """按订单号查询。不属于该用户的订单按不存在处理。"""
        return await asyncio.to_thread(self._get_order, order_id, user_id)

    def _get_order(self, order_id: str, user_id: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM orders WHERE order_id=? AND user_id=?", (order_id, user_id)
            ).fetchone()
            return _row_dict(row) if row else None

    async def get_charges(self, order_id: str, user_id: str) -> List[dict]:
        """按订单号返回扣款记录（按支付时间升序）。订单不属于该用户时返回空列表。"""
        return await asyncio.to_thread(self._get_charges, order_id, user_id)

    def _get_charges(self, order_id: str, user_id: str) -> List[dict]:
        with self._connect() as conn:
            owner = conn.execute(
                "SELECT user_id FROM orders WHERE order_id=?", (order_id,)
            ).fetchone()
            if owner is None or owner["user_id"] != user_id:
                return []  # 跨用户订单不可见
            rows = conn.execute(
                "SELECT * FROM charges WHERE order_id=? AND user_id=? ORDER BY paid_at ASC",
                (order_id, user_id),
            ).fetchall()
            return [_row_dict(row) for row in rows]

    # ── 退款写入（幂等）────────────────────────────────

    async def submit_refund(self, order_id: str, user_id: str, amount_cents: int,
                            reason: str, operation_id: str) -> dict:
        """提交退款申请。同一 operation_id 重复提交返回相同记录；参数不同则冲突。"""
        return await asyncio.to_thread(
            self._submit_refund, order_id, user_id, amount_cents, reason, operation_id)

    def _submit_refund(self, order_id, user_id, amount_cents, reason, operation_id) -> dict:
        # 故障注入：副作用发生前失败（外部系统根本没收到）
        if self._fault == FAIL_BEFORE_EFFECT:
            self._fault = None
            raise BillingUnavailable("故障注入：提交前失败（副作用未发生）")
        with self._connect() as conn:
            order = conn.execute(
                "SELECT * FROM orders WHERE order_id=? AND user_id=?", (order_id, user_id)
            ).fetchone()
            if order is None:
                raise RefundConflictError(f"订单不存在或不属于用户: {order_id}")
            existing = conn.execute(
                "SELECT * FROM refunds WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if existing is not None:
                if (existing["order_id"], existing["user_id"], existing["amount_cents"]) \
                        != (order_id, user_id, amount_cents):
                    raise RefundConflictError("同一 operation_id 不能用于不同参数")
                return _row_dict(existing)  # 幂等：返回相同退款记录
            conn.execute("BEGIN IMMEDIATE")
            now = datetime.now().isoformat()
            # 业务校验（确定性）：退款金额超过订单金额 → 直接落为 failed 终态
            if amount_cents > order["amount_cents"]:
                status = "failed"
            elif self._fault == DELAYED_SUCCESS:
                status = "processing"
                self._fault = None  # 一次性注入：代表外部系统处理较慢
            else:
                status = "submitted"
            conn.execute(
                "INSERT INTO refunds (operation_id, order_id, user_id, amount_cents, reason, "
                "status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (operation_id, order_id, user_id, amount_cents, reason, status, now, now),
            )
            conn.execute("COMMIT")
            record = dict(operation_id=operation_id, order_id=order_id, user_id=user_id,
                          amount_cents=amount_cents, reason=reason, status=status,
                          created_at=now, updated_at=now)
        # 故障注入：副作用已发生但响应丢失（本地 checkpoint 前崩溃）
        if self._fault in (FAIL_AFTER_EFFECT_BEFORE_RESPONSE,
                           FAIL_AFTER_RESPONSE_BEFORE_CHECKPOINT):
            self._fault = None
            raise BillingOutcomeUnknown(operation_id)
        return record

    async def get_refund_status(self, operation_id: str, user_id: str) -> Optional[dict]:
        """查询退款状态（状态阶梯：submitted → processing → succeeded）。"""
        return await asyncio.to_thread(self._get_refund_status, operation_id, user_id)

    def _get_refund_status(self, operation_id: str, user_id: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM refunds WHERE operation_id=? AND user_id=?", (operation_id, user_id)
            ).fetchone()
            if row is None:
                return None  # 未执行或不属于该用户
            status = row["status"]
            if status == "submitted":
                status = "processing"
                conn.execute(
                    "UPDATE refunds SET status='processing', updated_at=? WHERE operation_id=?",
                    (datetime.now().isoformat(), operation_id),
                )
            elif status == "processing":
                status = "succeeded"
                conn.execute(
                    "UPDATE refunds SET status='succeeded', updated_at=? WHERE operation_id=?",
                    (datetime.now().isoformat(), operation_id),
                )
            return dict(row) | {"status": status}


def demo_seed() -> tuple[List[dict], List[dict]]:
    """旗舰场景的演示数据：u1 的订单 #12345 有两笔相同金额扣款（重复扣款）。

    时间固定，保证演示与断言可重复；u2 拥有独立订单用于属主隔离验证。
    """
    orders = [
        {"order_id": "12345", "user_id": "u1", "amount_cents": 9900,
         "status": "paid", "created_at": "2026-11-01T10:00:00"},
        {"order_id": "67890", "user_id": "u1", "amount_cents": 19900,
         "status": "paid", "created_at": "2026-11-10T10:00:00"},
        {"order_id": "99999", "user_id": "u2", "amount_cents": 5000,
         "status": "paid", "created_at": "2026-11-11T10:00:00"},
    ]
    charges = [
        {"charge_id": "chg-001", "order_id": "12345", "user_id": "u1",
         "amount_cents": 9900, "paid_at": "2026-11-01T10:01:00", "channel": "wechat"},
        {"charge_id": "chg-002", "order_id": "12345", "user_id": "u1",
         "amount_cents": 9900, "paid_at": "2026-11-01T10:02:00", "channel": "wechat"},
        {"charge_id": "chg-003", "order_id": "67890", "user_id": "u1",
         "amount_cents": 19900, "paid_at": "2026-11-10T10:01:00", "channel": "alipay"},
        {"charge_id": "chg-004", "order_id": "99999", "user_id": "u2",
         "amount_cents": 5000, "paid_at": "2026-11-11T10:01:00", "channel": "wechat"},
    ]
    return orders, charges
