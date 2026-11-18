"""RunStore：任务运行状态的唯一事实源（SQLite）。

亮点：所有状态变更都是「校验 + 更新 + 事件」的原子事务；
版本号与租约双重校验，防止过期 runner 覆盖新状态（实施计划 11.2 节）。

SQLite 打开参数：WAL、foreign_keys、busy_timeout。每个操作使用独立连接
并通过 asyncio.to_thread 在线程池执行，避免跨线程共用连接；
多语句写操作在 BEGIN IMMEDIATE 事务内完成。
"""
import asyncio
import dataclasses
import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Iterator, Optional

from runtime.models import (
    ApprovalDecision,
    Run,
    RunApproval,
    RunEvent,
    RunStatus,
    RunStep,
    StepStatus,
    now_iso,
)
from runtime.state_machine import (
    RUN_TERMINAL,
    STEP_TERMINAL,
    can_transition_run,
    can_transition_step,
)

logger = logging.getLogger(__name__)

DEFAULT_LEASE_SECONDS = 30
SCHEMA_VERSION = 2

# 版本迁移：键为目标版本号；_MIGRATIONS[2] 是把 v1 数据库升级到 v2 的语句
_MIGRATIONS = {
    2: ["ALTER TABLE runs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0"],
}


# ── 异常 ──────────────────────────────────────────────


class StoreError(RuntimeError):
    """Runtime 存储层错误基类。"""


class RunNotFoundError(StoreError):
    """Run 不存在。"""


class DuplicateRunError(StoreError):
    """同一 (user_id, conv_id, client_request_id) 已存在 Run。"""

    def __init__(self, run_id: str):
        super().__init__(f"同一请求键已存在 Run: {run_id}")
        self.run_id = run_id


class VersionConflictError(StoreError):
    """版本号不匹配，写入被拒绝。"""


class IllegalTransitionError(StoreError):
    """状态转换不在合法转换表内（含 Step 的期望状态不匹配）。"""


class LeaseConflictError(StoreError):
    """有效租约由其他 owner 持有。"""


class LeaseLostError(StoreError):
    """持有者租约已过期，禁止继续提交。"""


class StepNotFoundError(StoreError):
    """Step 不存在。"""


class StepExistsError(StoreError):
    """同一 (run_id, step_key) 的 Step 已存在。"""


class ApprovalNotFoundError(StoreError):
    """审批记录不存在。"""


class ApprovalAlreadyDecidedError(StoreError):
    """审批已经决定过，不允许覆盖。"""


REJECTED_ANSWER = "退款申请未获批准，本次不执行退款。如有疑问请联系人工客服。"


# ── 表结构 ────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  id                   TEXT PRIMARY KEY,
  user_id              TEXT NOT NULL,
  conv_id              TEXT NOT NULL,
  client_request_id    TEXT NOT NULL,
  message_hash         TEXT NOT NULL,
  task_type            TEXT NOT NULL,
  plan_version         TEXT NOT NULL,
  status               TEXT NOT NULL,
  current_step         TEXT,
  context_json         TEXT,
  input_json           TEXT,
  result_json          TEXT,
  response_json        TEXT,
  error_code           TEXT,
  retryable            INTEGER NOT NULL DEFAULT 0,
  version              INTEGER NOT NULL DEFAULT 0,
  lease_owner          TEXT,
  lease_until          TEXT,
  cancel_requested     INTEGER NOT NULL DEFAULT 0,
  transcript_committed INTEGER NOT NULL DEFAULT 0,
  created_at           TEXT NOT NULL,
  updated_at           TEXT NOT NULL,
  finished_at          TEXT,
  UNIQUE(user_id, conv_id, client_request_id)
);

CREATE TABLE IF NOT EXISTS run_steps (
  id              TEXT PRIMARY KEY,
  run_id          TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_key        TEXT NOT NULL,
  ordinal         INTEGER NOT NULL,
  tool_name       TEXT,
  status          TEXT NOT NULL,
  attempt         INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT,
  input_json      TEXT,
  output_json     TEXT,
  evidence_json   TEXT,
  error_code      TEXT,
  started_at      TEXT,
  finished_at     TEXT,
  UNIQUE(run_id, step_key)
);

CREATE TABLE IF NOT EXISTS run_approvals (
  id           TEXT PRIMARY KEY,
  run_id       TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_key     TEXT NOT NULL,
  risk_summary TEXT NOT NULL,
  decision     TEXT NOT NULL,
  decided_by   TEXT,
  comment      TEXT,
  created_at   TEXT NOT NULL,
  decided_at   TEXT
);

CREATE TABLE IF NOT EXISTS run_events (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id       TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  event_type   TEXT NOT NULL,
  step_key     TEXT,
  payload_json TEXT NOT NULL,
  created_at   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_steps_run ON run_steps(run_id);
CREATE INDEX IF NOT EXISTS idx_run_events_run ON run_events(run_id);
CREATE INDEX IF NOT EXISTS idx_run_approvals_run ON run_approvals(run_id);
"""


def _dumps(value: Optional[dict]) -> Optional[str]:
    """dict → JSON 文本；None 保持 NULL。"""
    return json.dumps(value, ensure_ascii=False) if value is not None else None


def _loads(text: Optional[str]) -> Optional[dict]:
    """JSON 文本 → dict；NULL 保持 None。"""
    return json.loads(text) if text is not None else None


def _run_to_row(run: Run) -> dict:
    return {
        "id": run.id,
        "user_id": run.user_id,
        "conv_id": run.conv_id,
        "client_request_id": run.client_request_id,
        "message_hash": run.message_hash,
        "task_type": run.task_type,
        "plan_version": run.plan_version,
        "status": run.status.value,
        "current_step": run.current_step,
        "context_json": _dumps(run.context_json),
        "input_json": _dumps(run.input_json),
        "result_json": _dumps(run.result_json),
        "response_json": _dumps(run.response_json),
        "error_code": run.error_code,
        "retryable": int(run.retryable),
        "version": run.version,
        "lease_owner": run.lease_owner,
        "lease_until": run.lease_until,
        "cancel_requested": int(run.cancel_requested),
        "transcript_committed": int(run.transcript_committed),
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "finished_at": run.finished_at,
    }


def _run_from_row(row: sqlite3.Row) -> Run:
    return Run(
        id=row["id"],
        user_id=row["user_id"],
        conv_id=row["conv_id"],
        client_request_id=row["client_request_id"],
        message_hash=row["message_hash"],
        task_type=row["task_type"],
        plan_version=row["plan_version"],
        status=RunStatus(row["status"]),
        current_step=row["current_step"],
        context_json=_loads(row["context_json"]),
        input_json=_loads(row["input_json"]),
        result_json=_loads(row["result_json"]),
        response_json=_loads(row["response_json"]),
        error_code=row["error_code"],
        retryable=bool(row["retryable"]),
        version=row["version"],
        lease_owner=row["lease_owner"],
        lease_until=row["lease_until"],
        cancel_requested=bool(row["cancel_requested"]),
        transcript_committed=bool(row["transcript_committed"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        finished_at=row["finished_at"],
    )


def _step_to_row(step: RunStep) -> dict:
    return {
        "id": step.id,
        "run_id": step.run_id,
        "step_key": step.step_key,
        "ordinal": step.ordinal,
        "tool_name": step.tool_name,
        "status": step.status.value,
        "attempt": step.attempt,
        "idempotency_key": step.idempotency_key,
        "input_json": _dumps(step.input_json),
        "output_json": _dumps(step.output_json),
        "evidence_json": _dumps(step.evidence_json),
        "error_code": step.error_code,
        "started_at": step.started_at,
        "finished_at": step.finished_at,
    }


def _step_from_row(row: sqlite3.Row) -> RunStep:
    return RunStep(
        id=row["id"],
        run_id=row["run_id"],
        step_key=row["step_key"],
        ordinal=row["ordinal"],
        tool_name=row["tool_name"],
        status=StepStatus(row["status"]),
        attempt=row["attempt"],
        idempotency_key=row["idempotency_key"],
        input_json=_loads(row["input_json"]),
        output_json=_loads(row["output_json"]),
        evidence_json=_loads(row["evidence_json"]),
        error_code=row["error_code"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


def _event_from_row(row: sqlite3.Row) -> RunEvent:
    return RunEvent(
        seq=row["seq"],
        run_id=row["run_id"],
        event_type=row["event_type"],
        step_key=row["step_key"],
        payload_json=_loads(row["payload_json"]) or {},
        created_at=row["created_at"],
    )


def _approval_from_row(row: sqlite3.Row) -> RunApproval:
    return RunApproval(
        id=row["id"],
        run_id=row["run_id"],
        step_key=row["step_key"],
        risk_summary=row["risk_summary"],
        decision=ApprovalDecision(row["decision"]),
        decided_by=row["decided_by"],
        comment=row["comment"],
        created_at=row["created_at"],
        decided_at=row["decided_at"],
    )


class RunStore:
    """SQLite 持久化的任务状态存储。所有写操作带版本/租约校验。

    接口是 async 的（与仓库其余模块一致），内部同步 sqlite3
    操作统一通过 asyncio.to_thread 调度到线程池。
    """

    def __init__(self, path):
        self._path = str(path)

    @property
    def path(self) -> str:
        """数据库文件路径，便于排查与测试。"""
        return self._path

    # ── 连接管理 ──────────────────────────────────────

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        """每次操作使用独立连接；isolation_level=None 后由显式 BEGIN 控制事务。"""
        conn = sqlite3.connect(self._path, timeout=5.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            yield conn
        finally:
            conn.close()

    def _insert_event(self, conn: sqlite3.Connection, run_id: str, event_type: str,
                      step_key: Optional[str], payload: Optional[dict]) -> None:
        conn.execute(
            "INSERT INTO run_events (run_id, event_type, step_key, payload_json, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (run_id, event_type, step_key, _dumps(payload or {}), now_iso()),
        )

    def _load_run_for_write(self, conn: sqlite3.Connection, run_id: str) -> Run:
        """写事务样板：加载 Run；不存在则回滚并抛错（A2 提取共享）。"""
        row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        if row is None:
            conn.execute("ROLLBACK")
            raise RunNotFoundError(f"Run 不存在: {run_id}")
        return _run_from_row(row)

    def _require_version(self, conn: sqlite3.Connection, run_id: str, run: Run,
                         expected_version: int, target: str) -> None:
        """写事务样板：版本校验；不匹配则回滚、写拒绝事件并抛错（A2 提取共享）。"""
        if run.version != expected_version:
            conn.execute("ROLLBACK")
            self._record_rejection(conn, run_id, None, {
                "from": run.status.value, "to": target,
                "reason": "version_conflict",
                "expected_version": expected_version, "current_version": run.version})
            raise VersionConflictError(f"版本冲突: 期望 {expected_version}，当前 {run.version}")

    def _record_rejection(self, conn: sqlite3.Connection, run_id: str, step_key: Optional[str],
                          payload: dict) -> None:
        """5.3 节：非法转换/版本冲突被拒绝时也写入 Event（ROLLBACK 后独立落库，审计可追溯）。"""
        try:
            self._insert_event(conn, run_id, "TRANSITION_REJECTED", step_key, payload)
        except Exception:
            logger.warning("拒绝事件写入失败: %s", run_id, exc_info=True)

    # ── 初始化 ────────────────────────────────────────

    async def bootstrap(self) -> None:
        """建表与索引。幂等：重复调用不报错、不影响已有数据。"""
        await asyncio.to_thread(self._bootstrap)

    def _bootstrap(self) -> None:
        with self._connect() as conn:
            existed = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='runs'"
            ).fetchone() is not None
            conn.executescript(_SCHEMA)
            if not existed:
                # 全新数据库：直接标记为最新版本，跳过历史迁移
                conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                return
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            for target in range(version + 1, SCHEMA_VERSION + 1):
                for stmt in _MIGRATIONS.get(target, []):
                    conn.execute(stmt)
            conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    # ── Run 创建与读取 ────────────────────────────────

    async def create_run(self, **fields: Any) -> Run:
        """插入一个新 Run 并写入 RUN_CREATED 事件（同一事务）。"""
        return await asyncio.to_thread(self._insert_run, Run(**fields))

    def _insert_run(self, run: Run) -> Run:
        row_values = _run_to_row(run)  # A3：单次构造行值
        with self._connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO runs ("
                    + ", ".join(row_values.keys()) + ") VALUES ("
                    + ", ".join("?" for _ in row_values) + ")",
                    tuple(row_values.values()),
                )
                self._insert_event(conn, run.id, "RUN_CREATED", None,
                                   {"task_type": run.task_type, "plan_version": run.plan_version})
                conn.execute("COMMIT")
            except sqlite3.IntegrityError:
                conn.execute("ROLLBACK")
                existing = conn.execute(
                    "SELECT id FROM runs WHERE user_id=? AND conv_id=? AND client_request_id=?",
                    (run.user_id, run.conv_id, run.client_request_id),
                ).fetchone()
                if existing:
                    raise DuplicateRunError(existing["id"]) from None
                raise
        return run

    async def get_run(self, run_id: str) -> Run:
        return await asyncio.to_thread(self._get_run, run_id)

    def _get_run(self, run_id: str) -> Run:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise RunNotFoundError(f"Run 不存在: {run_id}")
            return _run_from_row(row)

    async def find_run(self, user_id: str, conv_id: str, client_request_id: str) -> Optional[Run]:
        """按请求键查找 Run；幂等创建由 API 层用本方法配合 create_run 实现。"""
        return await asyncio.to_thread(self._find_run, user_id, conv_id, client_request_id)

    def _find_run(self, user_id: str, conv_id: str, client_request_id: str) -> Optional[Run]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE user_id=? AND conv_id=? AND client_request_id=?",
                (user_id, conv_id, client_request_id),
            ).fetchone()
            return _run_from_row(row) if row else None

    # ── Run 状态转换 ──────────────────────────────────

    async def update_run_status(
        self,
        run_id: str,
        expected_version: int,
        next_status: RunStatus,
        *,
        current_step: Optional[str] = None,
        lease_owner: Optional[str] = None,
        result_json: Optional[dict] = None,
        response_json: Optional[dict] = None,
        error_code: Optional[str] = None,
        retryable: Optional[bool] = None,
        event_type: Optional[str] = None,
        event_payload: Optional[dict] = None,
    ) -> Run:
        """原子状态转换：校验版本、合法转换与租约后，更新状态并（可选）写事件。

        进入终态时自动写 finished_at。任一校验失败整个事务回滚。
        """
        return await asyncio.to_thread(
            self._update_run_status, run_id, expected_version, next_status,
            current_step, lease_owner, result_json, response_json, error_code, retryable,
            event_type, event_payload,
        )

    def _update_run_status(self, run_id, expected_version, next_status, current_step,
                           lease_owner, result_json, response_json, error_code, retryable,
                           event_type, event_payload) -> Run:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = self._load_run_for_write(conn, run_id)
            self._require_version(conn, run_id, run, expected_version, next_status.value)
            if not can_transition_run(run.status, next_status):
                conn.execute("ROLLBACK")
                self._record_rejection(conn, run_id, None, {
                    "from": run.status.value, "to": next_status.value,
                    "reason": "illegal_transition"})
                raise IllegalTransitionError(
                    f"非法状态转换: {run.status.value} → {next_status.value}"
                )
            self._check_lease(run, lease_owner)
            finished_at = now_iso() if next_status in RUN_TERMINAL else None
            conn.execute(
                "UPDATE runs SET status=?, current_step=?, result_json=?, response_json=?, "
                "error_code=?, retryable=?, version=version+1, updated_at=?, finished_at=? "
                "WHERE id=?",
                (
                    next_status.value,
                    current_step if current_step is not None else run.current_step,
                    _dumps(result_json) if result_json is not None else run.result_json and _dumps(run.result_json),
                    _dumps(response_json) if response_json is not None else run.response_json and _dumps(run.response_json),
                    error_code if error_code is not None else run.error_code,
                    int(retryable) if retryable is not None else run.retryable,
                    now_iso(),
                    finished_at if finished_at is not None else run.finished_at,
                    run_id,
                ),
            )
            if event_type:
                self._insert_event(conn, run_id, event_type, current_step, event_payload)
            conn.execute("COMMIT")
        return dataclasses.replace(
            run,
            status=next_status,
            version=run.version + 1,
            updated_at=now_iso(),
            finished_at=finished_at or run.finished_at,
            current_step=current_step if current_step is not None else run.current_step,
            result_json=result_json if result_json is not None else run.result_json,
            response_json=response_json if response_json is not None else run.response_json,
            error_code=error_code if error_code is not None else run.error_code,
            retryable=retryable if retryable is not None else run.retryable,
        )

    def _check_lease(self, run: Run, lease_owner: Optional[str]) -> None:
        """租约校验：有效租约只能由持有者提交；过期持有者提交被拒绝。"""
        if run.lease_owner is None:
            return
        active = bool(run.lease_until) and datetime.fromisoformat(run.lease_until) > datetime.now()
        if active:
            if lease_owner != run.lease_owner:
                raise LeaseConflictError(f"租约由 {run.lease_owner} 持有")
            return
        # 租约已过期：旧持有者不能再提交；其他调用方视为无有效租约
        if lease_owner is not None and lease_owner == run.lease_owner:
            raise LeaseLostError(f"租约已过期，持有者 {run.lease_owner} 不能继续提交")

    # ── 租约 ──────────────────────────────────────────

    async def try_acquire_lease(self, run_id: str, owner: str, expected_version: int,
                                lease_s: int = DEFAULT_LEASE_SECONDS) -> Run:
        """获取或续租。有效租约只允许持有者续租；过期/空闲租约任何 owner 可接管。"""
        return await asyncio.to_thread(self._try_acquire_lease, run_id, owner, expected_version, lease_s)

    def _try_acquire_lease(self, run_id: str, owner: str, expected_version: int, lease_s: int) -> Run:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = self._load_run_for_write(conn, run_id)
            self._require_version(conn, run_id, run, expected_version, "lease")
            active = bool(run.lease_until) and datetime.fromisoformat(run.lease_until) > datetime.now()
            if active and run.lease_owner != owner:
                conn.execute("ROLLBACK")
                raise LeaseConflictError(f"租约由 {run.lease_owner} 持有")
            lease_until = (datetime.now() + timedelta(seconds=lease_s)).isoformat()
            conn.execute(
                "UPDATE runs SET lease_owner=?, lease_until=?, version=version+1, updated_at=? "
                "WHERE id=?",
                (owner, lease_until, now_iso(), run_id),
            )
            conn.execute("COMMIT")
        return dataclasses.replace(run, lease_owner=owner, lease_until=lease_until,
                                   version=run.version + 1, updated_at=now_iso())

    # ── 补充输入与活动列表 ────────────────────────────

    async def append_run_input(self, run_id: str, expected_version: int, message: str) -> Run:
        """把用户补充输入追加到 input_json.supplements（resume 用，7.3 节）。"""
        return await asyncio.to_thread(self._append_run_input, run_id, expected_version, message)

    def _append_run_input(self, run_id: str, expected_version: int, message: str) -> Run:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = self._load_run_for_write(conn, run_id)
            self._require_version(conn, run_id, run, expected_version, "resume")
            updated_input = dict(run.input_json or {})
            updated_input.setdefault("supplements", []).append(message)
            conn.execute(
                "UPDATE runs SET input_json=?, version=version+1, updated_at=? WHERE id=?",
                (_dumps(updated_input), now_iso(), run_id),
            )
            self._insert_event(conn, run_id, "RUN_RESUMED", None, {"reason": "user_input"})
            conn.execute("COMMIT")
        return dataclasses.replace(run, input_json=updated_input,
                                   version=run.version + 1, updated_at=now_iso())

    async def list_active_runs(self) -> list[Run]:
        """返回所有非终态 Run（恢复扫描用，11.3 节）。"""
        return await asyncio.to_thread(self._list_active_runs)

    def _list_active_runs(self) -> list[Run]:
        terminal = ", ".join(f"'{status.value}'" for status in RUN_TERMINAL)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM runs WHERE status NOT IN ({terminal}) ORDER BY created_at ASC"
            ).fetchall()
            return [_run_from_row(row) for row in rows]

    async def list_uncommitted_succeeded(self) -> list[Run]:
        """返回 SUCCEEDED 但会话尚未提交的 Run（11.4 节恢复检查）。"""
        return await asyncio.to_thread(self._list_uncommitted_succeeded)

    def _list_uncommitted_succeeded(self) -> list[Run]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runs WHERE status='SUCCEEDED' AND transcript_committed=0"
            ).fetchall()
            return [_run_from_row(row) for row in rows]

    # ── 会话提交标记 ──────────────────────────────────

    async def mark_transcript_committed(self, run_id: str, expected_version: int) -> Run:
        """最终回答已一次性提交会话（11.4 节）。只标记，不改变状态机。"""
        return await asyncio.to_thread(self._mark_transcript_committed, run_id, expected_version)

    def _mark_transcript_committed(self, run_id: str, expected_version: int) -> Run:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = self._load_run_for_write(conn, run_id)
            self._require_version(conn, run_id, run, expected_version, "transcript_committed")
            conn.execute(
                "UPDATE runs SET transcript_committed=1, version=version+1, updated_at=? WHERE id=?",
                (now_iso(), run_id),
            )
            self._insert_event(conn, run_id, "TRANSCRIPT_COMMITTED", None, {})
            conn.execute("COMMIT")
        return dataclasses.replace(run, transcript_committed=True,
                                   version=run.version + 1, updated_at=now_iso())

    # ── 取消 ──────────────────────────────────────────

    async def request_cancel(self, run_id: str, expected_version: int) -> Run:
        """对 RUNNING 状态的 Run 设置取消标记；引擎在安全检查点响应。

        重复设置幂等：已标记时直接返回当前 Run，不递增版本。
        """
        return await asyncio.to_thread(self._request_cancel, run_id, expected_version)

    def _request_cancel(self, run_id: str, expected_version: int) -> Run:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = self._load_run_for_write(conn, run_id)
            self._require_version(conn, run_id, run, expected_version, "cancel_requested")
            if run.status != RunStatus.RUNNING:
                conn.execute("ROLLBACK")
                self._record_rejection(conn, run_id, None, {
                    "from": run.status.value, "to": "cancel_requested",
                    "reason": "illegal_transition"})
                raise IllegalTransitionError(
                    f"仅 RUNNING 状态支持取消标记，当前 {run.status.value}"
                )
            if run.cancel_requested:
                conn.execute("COMMIT")  # 已标记，幂等返回
                return run
            conn.execute(
                "UPDATE runs SET cancel_requested=1, version=version+1, updated_at=? WHERE id=?",
                (now_iso(), run_id),
            )
            self._insert_event(conn, run_id, "CANCEL_REQUESTED", None, {"by": "user"})
            conn.execute("COMMIT")
        return dataclasses.replace(run, cancel_requested=True, version=run.version + 1,
                                   updated_at=now_iso())

    async def release_lease(self, run_id: str, expected_version: int) -> Run:
        """释放租约（引擎停靠到等待态/终态时调用），用户操作不受租约阻塞。"""
        return await asyncio.to_thread(self._release_lease, run_id, expected_version)

    def _release_lease(self, run_id: str, expected_version: int) -> Run:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = self._load_run_for_write(conn, run_id)
            self._require_version(conn, run_id, run, expected_version, "lease_release")
            conn.execute(
                "UPDATE runs SET lease_owner=NULL, lease_until=NULL, version=version+1, "
                "updated_at=? WHERE id=?",
                (now_iso(), run_id),
            )
            conn.execute("COMMIT")
        return dataclasses.replace(run, lease_owner=None, lease_until=None,
                                   version=run.version + 1, updated_at=now_iso())

    # ── Step ──────────────────────────────────────────

    async def create_steps(self, run_id: str, steps: list[RunStep]) -> None:
        """批量插入 Plan 生成的 Step。任一重复 step_key 则整批回滚。"""
        await asyncio.to_thread(self._insert_steps, run_id, steps)

    def _insert_steps(self, run_id: str, steps: list[RunStep]) -> None:
        with self._connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                for step in steps:
                    step = dataclasses.replace(step, run_id=run_id)  # 统一填充归属 Run
                    conn.execute(
                        "INSERT INTO run_steps (" + ", ".join(_step_to_row(step).keys())
                        + ") VALUES (" + ", ".join("?" for _ in _step_to_row(step)) + ")",
                        tuple(_step_to_row(step).values()),
                    )
                conn.execute("COMMIT")
            except sqlite3.IntegrityError:
                conn.execute("ROLLBACK")
                raise StepExistsError(f"Run {run_id} 中 Step 已存在") from None

    async def get_steps(self, run_id: str) -> list[RunStep]:
        """按 ordinal 升序返回全部 Step。"""
        return await asyncio.to_thread(self._get_steps, run_id)

    def _get_steps(self, run_id: str) -> list[RunStep]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM run_steps WHERE run_id=? ORDER BY ordinal ASC", (run_id,)
            ).fetchall()
            return [_step_from_row(row) for row in rows]

    async def get_step(self, run_id: str, step_key: str) -> RunStep:
        return await asyncio.to_thread(self._get_step, run_id, step_key)

    def _get_step(self, run_id: str, step_key: str) -> RunStep:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM run_steps WHERE run_id=? AND step_key=?", (run_id, step_key)
            ).fetchone()
            if row is None:
                raise StepNotFoundError(f"Step 不存在: {run_id}/{step_key}")
            return _step_from_row(row)

    async def update_step_status(
        self,
        run_id: str,
        step_key: str,
        expected_status: StepStatus,
        next_status: StepStatus,
        *,
        attempt: Optional[int] = None,
        idempotency_key: Optional[str] = None,
        output_json: Optional[dict] = None,
        evidence_json: Optional[dict] = None,
        error_code: Optional[str] = None,
        event_type: Optional[str] = None,
        event_payload: Optional[dict] = None,
    ) -> RunStep:
        """Step 状态转换：期望状态不匹配（CAS）或转换非法时拒绝，事务回滚。

        idempotency_key 在执行写工具前持久化（9.2 节），重试复用相同键。
        """
        return await asyncio.to_thread(
            self._update_step_status, run_id, step_key, expected_status, next_status,
            attempt, idempotency_key, output_json, evidence_json, error_code,
            event_type, event_payload,
        )

    def _update_step_status(self, run_id, step_key, expected_status, next_status, attempt,
                            idempotency_key, output_json, evidence_json, error_code,
                            event_type, event_payload) -> RunStep:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM run_steps WHERE run_id=? AND step_key=?", (run_id, step_key)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise StepNotFoundError(f"Step 不存在: {run_id}/{step_key}")
            step = _step_from_row(row)
            if step.status != expected_status:
                conn.execute("ROLLBACK")
                self._record_rejection(conn, run_id, step_key, {
                    "from": step.status.value, "to": next_status.value,
                    "reason": "status_conflict", "expected": expected_status.value})
                raise IllegalTransitionError(
                    f"Step 状态冲突: 期望 {expected_status.value}，当前 {step.status.value}"
                )
            if not can_transition_step(expected_status, next_status):
                conn.execute("ROLLBACK")
                self._record_rejection(conn, run_id, step_key, {
                    "from": expected_status.value, "to": next_status.value,
                    "reason": "illegal_transition"})
                raise IllegalTransitionError(
                    f"非法 Step 转换: {expected_status.value} → {next_status.value}"
                )
            # 进入 RUNNING 时打开始时间；此后保持不变
            started_at = now_iso() if next_status == StepStatus.RUNNING and step.started_at is None \
                else step.started_at
            finished_at = now_iso() if next_status in STEP_TERMINAL else None
            conn.execute(
                "UPDATE run_steps SET status=?, attempt=?, idempotency_key=?, output_json=?, "
                "evidence_json=?, error_code=?, started_at=?, finished_at=? "
                "WHERE run_id=? AND step_key=?",
                (
                    next_status.value,
                    attempt if attempt is not None else step.attempt,
                    idempotency_key if idempotency_key is not None else step.idempotency_key,
                    _dumps(output_json) if output_json is not None else step.output_json and _dumps(step.output_json),
                    _dumps(evidence_json) if evidence_json is not None else step.evidence_json and _dumps(step.evidence_json),
                    error_code if error_code is not None else step.error_code,
                    started_at,
                    finished_at if finished_at is not None else step.finished_at,
                    run_id,
                    step_key,
                ),
            )
            if event_type:
                self._insert_event(conn, run_id, event_type, step_key, event_payload)
            conn.execute("COMMIT")
        return dataclasses.replace(
            step,
            status=next_status,
            attempt=attempt if attempt is not None else step.attempt,
            idempotency_key=idempotency_key if idempotency_key is not None else step.idempotency_key,
            output_json=output_json if output_json is not None else step.output_json,
            evidence_json=evidence_json if evidence_json is not None else step.evidence_json,
            error_code=error_code if error_code is not None else step.error_code,
            started_at=started_at,
            finished_at=finished_at or step.finished_at,
        )

    async def set_step_idempotency_key(self, run_id: str, step_key: str, key: str, *,
                                       event_type: Optional[str] = None,
                                       event_payload: Optional[dict] = None) -> RunStep:
        """执行写工具前持久化幂等键（9.2 节），不改变 Step 状态。"""
        return await asyncio.to_thread(
            self._set_step_idempotency_key, run_id, step_key, key, event_type, event_payload)

    def _set_step_idempotency_key(self, run_id, step_key, key, event_type, event_payload) -> RunStep:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM run_steps WHERE run_id=? AND step_key=?", (run_id, step_key)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise StepNotFoundError(f"Step 不存在: {run_id}/{step_key}")
            conn.execute(
                "UPDATE run_steps SET idempotency_key=? WHERE run_id=? AND step_key=?",
                (key, run_id, step_key),
            )
            if event_type:
                self._insert_event(conn, run_id, event_type, step_key, event_payload)
            conn.execute("COMMIT")
        return dataclasses.replace(_step_from_row(row), idempotency_key=key)

    # ── 审批 ──────────────────────────────────────────

    async def create_approval(self, run_id: str, step_key: str, risk_summary: str) -> RunApproval:
        """创建待审批记录。幂等：同 (run_id, step_key) 已有记录时返回原记录。"""
        return await asyncio.to_thread(self._create_approval, run_id, step_key, risk_summary)

    def _create_approval(self, run_id: str, step_key: str, risk_summary: str) -> RunApproval:
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM run_approvals WHERE run_id=? AND step_key=?",
                (run_id, step_key),
            ).fetchone()
            if existing is not None:
                return _approval_from_row(existing)
            approval = RunApproval(run_id=run_id, step_key=step_key, risk_summary=risk_summary)
            conn.execute(
                "INSERT INTO run_approvals (id, run_id, step_key, risk_summary, decision, "
                "decided_by, comment, created_at, decided_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (approval.id, approval.run_id, approval.step_key, approval.risk_summary,
                 approval.decision.value, approval.decided_by, approval.comment,
                 approval.created_at, approval.decided_at),
            )
            return approval

    async def get_approval(self, run_id: str, step_key: str) -> Optional[RunApproval]:
        """按 (run_id, step_key) 查审批记录；不存在返回 None。"""
        return await asyncio.to_thread(self._get_approval, run_id, step_key)

    def _get_approval(self, run_id: str, step_key: str) -> Optional[RunApproval]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM run_approvals WHERE run_id=? AND step_key=?",
                (run_id, step_key),
            ).fetchone()
            return _approval_from_row(row) if row else None

    async def decide_approval(self, run_id: str, step_key: str, decision: ApprovalDecision,
                              decided_by: Optional[str] = None,
                              comment: Optional[str] = None) -> RunApproval:
        """审批决定只允许一次：重复决定抛 ApprovalAlreadyDecidedError（不可覆盖）。

        注意：本方法只写审批记录；需要同时转换 Run 状态的调用方
        应使用 decide_approval_with_consequences（P0-2 原子性）。
        """
        return await asyncio.to_thread(
            self._decide_approval, run_id, step_key, decision, decided_by, comment)

    async def decide_approval_with_consequences(
        self, run_id: str, step_key: str, decision: ApprovalDecision,
        decided_by: Optional[str], comment: Optional[str],
        expected_version: int, lease_owner: Optional[str] = None,
    ) -> Run:
        """单个事务：审批决定 + Run 状态转换 + 事件（P0-2）。

        approve：Run WAITING_APPROVAL → RUNNING（引擎继续执行审批门）；
        reject：审批步 FAILED、退款步骤 SKIPPED、Run 经 RUNNING 到 FAILED，
        全部在同一 BEGIN IMMEDIATE 事务内完成，不存在"决定已提交但状态悬空"的中间态。
        """
        return await asyncio.to_thread(
            self._decide_approval_with_consequences, run_id, step_key, decision,
            decided_by, comment, expected_version, lease_owner)

    def _decide_approval_with_consequences(self, run_id, step_key, decision, decided_by,
                                           comment, expected_version, lease_owner) -> Run:
        from runtime.plans import StepKey  # 延迟导入避免循环依赖
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = self._load_run_for_write(conn, run_id)
            self._require_version(conn, run_id, run, expected_version,
                                  f"approval_{decision.value}")
            if run.status != RunStatus.WAITING_APPROVAL:
                conn.execute("ROLLBACK")
                self._record_rejection(conn, run_id, step_key, {
                    "from": run.status.value, "to": f"approval_{decision.value}",
                    "reason": "illegal_transition"})
                raise IllegalTransitionError(
                    f"审批决定需要 WAITING_APPROVAL，当前 {run.status.value}")
            self._check_lease(run, lease_owner)
            row = conn.execute(
                "SELECT * FROM run_approvals WHERE run_id=? AND step_key=?",
                (run_id, step_key)).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise ApprovalNotFoundError(f"审批不存在: {run_id}/{step_key}")
            approval = _approval_from_row(row)
            if approval.decision != ApprovalDecision.PENDING:
                conn.execute("ROLLBACK")
                raise ApprovalAlreadyDecidedError(
                    f"审批已决定为 {approval.decision.value}，不可覆盖")
            decided_at = now_iso()
            conn.execute(
                "UPDATE run_approvals SET decision=?, decided_by=?, comment=?, decided_at=? "
                "WHERE id=?", (decision.value, decided_by, comment, decided_at, approval.id))
            self._insert_event(conn, run_id, "APPROVAL_DECIDED", step_key,
                               {"decision": decision.value, "decided_by": decided_by})
            if decision == ApprovalDecision.REJECTED:
                # 拒绝后果同事务完成（与 approval.apply_rejected 语义一致）
                gate = conn.execute(
                    "SELECT * FROM run_steps WHERE run_id=? AND step_key=?",
                    (run_id, step_key)).fetchone()
                if gate is not None and _step_from_row(gate).status == StepStatus.WAITING_APPROVAL:
                    conn.execute(
                        "UPDATE run_steps SET status='FAILED', error_code='approval_rejected', "
                        "finished_at=? WHERE run_id=? AND step_key=?",
                        (now_iso(), run_id, step_key))
                for key in (StepKey.SUBMIT_REFUND, StepKey.POLL_REFUND_STATUS,
                            StepKey.BUILD_FINAL_ANSWER):
                    conn.execute(
                        "UPDATE run_steps SET status='SKIPPED' WHERE run_id=? AND step_key=? "
                        "AND status='PENDING'", (run_id, key.value))
                # 镜像状态机两步走（WAITING_APPROVAL → RUNNING → FAILED），但在同一事务内
                conn.execute(
                    "UPDATE runs SET status='RUNNING', version=version+1, updated_at=? "
                    "WHERE id=?", (now_iso(), run_id))
                conn.execute(
                    "UPDATE runs SET status='FAILED', error_code='approval_rejected', "
                    "retryable=0, response_json=?, version=version+1, updated_at=?, finished_at=? "
                    "WHERE id=?",
                    (_dumps({"response": REJECTED_ANSWER}), now_iso(), now_iso(), run_id))
                self._insert_event(conn, run_id, "RUN_FAILED", None,
                                   {"reason": "approval_rejected"})
                conn.execute("COMMIT")
                return _run_from_row(
                    conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone())
            # approved：WAITING_APPROVAL → RUNNING，引擎继续执行审批门
            conn.execute(
                "UPDATE runs SET status='RUNNING', version=version+1, updated_at=? WHERE id=?",
                (now_iso(), run_id))
            self._insert_event(conn, run_id, "RUN_RESUMED", None,
                               {"reason": "approval_approved"})
            conn.execute("COMMIT")
            return _run_from_row(
                conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone())

    def _decide_approval(self, run_id, step_key, decision, decided_by, comment) -> RunApproval:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM run_approvals WHERE run_id=? AND step_key=?",
                (run_id, step_key),
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise ApprovalNotFoundError(f"审批不存在: {run_id}/{step_key}")
            approval = _approval_from_row(row)
            if approval.decision != ApprovalDecision.PENDING:
                conn.execute("ROLLBACK")
                raise ApprovalAlreadyDecidedError(
                    f"审批已决定为 {approval.decision.value}，不可覆盖")
            decided_at = now_iso()
            conn.execute(
                "UPDATE run_approvals SET decision=?, decided_by=?, comment=?, decided_at=? "
                "WHERE id=?",
                (decision.value, decided_by, comment, decided_at, approval.id),
            )
            self._insert_event(conn, run_id, "APPROVAL_DECIDED", step_key,
                               {"decision": decision.value, "decided_by": decided_by})
            conn.execute("COMMIT")
        return dataclasses.replace(approval, decision=decision, decided_by=decided_by,
                                   comment=comment, decided_at=decided_at)

    async def request_approval_atomic(self, run_id: str, step_key: str, risk_summary: str,
                                      expected_version: int,
                                      lease_owner: Optional[str] = None) -> Run:
        """单个事务：创建审批（幂等）+ 审批步 → WAITING_APPROVAL + Run → WAITING_APPROVAL + 事件。

        P0-1：审批创建、Step 等待态与 Run 等待态原子提交，不存在
        "审批已创建但 Step/Run 未切换"的崩溃窗口。
        """
        return await asyncio.to_thread(
            self._request_approval_atomic, run_id, step_key, risk_summary,
            expected_version, lease_owner)

    def _request_approval_atomic(self, run_id, step_key, risk_summary, expected_version,
                                 lease_owner) -> Run:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            run = self._load_run_for_write(conn, run_id)
            self._require_version(conn, run_id, run, expected_version, "waiting_approval")
            if not can_transition_run(run.status, RunStatus.WAITING_APPROVAL):
                conn.execute("ROLLBACK")
                self._record_rejection(conn, run_id, step_key, {
                    "from": run.status.value, "to": RunStatus.WAITING_APPROVAL.value,
                    "reason": "illegal_transition"})
                raise IllegalTransitionError(
                    f"非法状态转换: {run.status.value} → {RunStatus.WAITING_APPROVAL.value}")
            self._check_lease(run, lease_owner)
            existing = conn.execute(
                "SELECT id FROM run_approvals WHERE run_id=? AND step_key=?",
                (run_id, step_key)).fetchone()
            if existing is None:
                approval = RunApproval(run_id=run_id, step_key=step_key,
                                       risk_summary=risk_summary)
                conn.execute(
                    "INSERT INTO run_approvals (id, run_id, step_key, risk_summary, decision, "
                    "decided_by, comment, created_at, decided_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (approval.id, approval.run_id, approval.step_key, approval.risk_summary,
                     approval.decision.value, approval.decided_by, approval.comment,
                     approval.created_at, approval.decided_at))
            step_row = conn.execute(
                "SELECT * FROM run_steps WHERE run_id=? AND step_key=?", (run_id, step_key)
            ).fetchone()
            if step_row is None:
                conn.execute("ROLLBACK")
                raise StepNotFoundError(f"Step 不存在: {run_id}/{step_key}")
            step_status = _step_from_row(step_row).status
            if step_status == StepStatus.RUNNING:
                conn.execute(
                    "UPDATE run_steps SET status='WAITING_APPROVAL', finished_at=NULL "
                    "WHERE run_id=? AND step_key=?", (run_id, step_key))
            elif step_status != StepStatus.WAITING_APPROVAL:
                conn.execute("ROLLBACK")
                self._record_rejection(conn, run_id, step_key, {
                    "from": step_status.value, "to": StepStatus.WAITING_APPROVAL.value,
                    "reason": "illegal_transition"})
                raise IllegalTransitionError(
                    f"非法 Step 转换: {step_status.value} → {StepStatus.WAITING_APPROVAL.value}")
            conn.execute(
                "UPDATE runs SET status='WAITING_APPROVAL', current_step=?, "
                "version=version+1, updated_at=? WHERE id=?",
                (step_key, now_iso(), run_id))
            self._insert_event(conn, run_id, "APPROVAL_REQUESTED", step_key, {})
            self._insert_event(conn, run_id, "WAITING_APPROVAL", None,
                               {"step": step_key})
            conn.execute("COMMIT")
            return _run_from_row(
                conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone())

    async def get_approvals(self, run_id: str) -> list[RunApproval]:
        """按创建顺序返回全部审批记录。创建/决定方法在 PR5 实现。"""
        return await asyncio.to_thread(self._get_approvals, run_id)

    def _get_approvals(self, run_id: str) -> list[RunApproval]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM run_approvals WHERE run_id=? ORDER BY created_at ASC", (run_id,)
            ).fetchall()
            return [_approval_from_row(row) for row in rows]

    # ── 事件 ──────────────────────────────────────────

    async def record_event(self, run_id: str, event_type: str, *,
                           step_key: Optional[str] = None,
                           payload: Optional[dict] = None) -> RunEvent:
        """追加一条审计事件。seq 由 SQLite 自增分配。"""
        return await asyncio.to_thread(self._record_event, run_id, event_type, step_key, payload)

    def _record_event(self, run_id: str, event_type: str, step_key: Optional[str],
                      payload: Optional[dict]) -> RunEvent:
        created_at = now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                "INSERT INTO run_events (run_id, event_type, step_key, payload_json, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (run_id, event_type, step_key, _dumps(payload or {}), created_at),
            )
            return RunEvent(
                seq=cursor.lastrowid, run_id=run_id, event_type=event_type,
                step_key=step_key, payload_json=payload or {}, created_at=created_at,
            )

    async def get_events(self, run_id: str, *, limit: Optional[int] = None,
                         offset: int = 0) -> list[RunEvent]:
        """按 seq 升序返回事件（追加式日志的天然顺序）。"""
        return await asyncio.to_thread(self._get_events, run_id, limit, offset)

    def _get_events(self, run_id: str, limit: Optional[int], offset: int) -> list[RunEvent]:
        sql = "SELECT * FROM run_events WHERE run_id=? ORDER BY seq ASC"
        params: list = [run_id]
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params += [limit, offset]
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [_event_from_row(row) for row in rows]
