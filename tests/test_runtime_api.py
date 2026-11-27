"""Runs API 回归：创建幂等、消息冲突、属主隔离、取消、审批与鉴权覆盖。

使用 ASGI 直连（不经过真实网络），依赖临时 SQLite，不需要 Redis/模型/外网。
"""
import asyncio
import json

import httpx
import pytest
import pytest_asyncio

import api.main as api
import runtime.api as runs_api
from runtime.models import RunStatus
from runtime.store import RunStore


def body(message="订单 #12345 好像被扣了两次款", user_id="u1", conv_id="c1",
         client_request_id="msg-001", task_type="duplicate_charge_refund"):
    return {"message": message, "user_id": user_id, "conv_id": conv_id,
            "client_request_id": client_request_id, "task_type": task_type}


@pytest_asyncio.fixture
async def services(monkeypatch, tmp_path):
    """无鉴权（development）环境：直接注入临时 SQLite 的 RunStore。"""
    monkeypatch.setenv("ECHOMIND_ENV", "development")
    monkeypatch.delenv("ECHOMIND_API_TOKENS", raising=False)
    store = RunStore(tmp_path / "runtime.db")
    await store.bootstrap()
    monkeypatch.setattr(runs_api, "_store", store)
    monkeypatch.setattr(runs_api, "_dispatch", None)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api.app), base_url="http://test") as http:
        yield store, http


@pytest_asyncio.fixture
async def authed(monkeypatch, tmp_path):
    """开启 Bearer 鉴权：admin + u1 + u2 三个身份。"""
    tokens = {
        "token-admin-0000000000000000": {"user_id": "admin", "role": "admin"},
        "token-user1-0000000000000000": {"user_id": "u1", "role": "user"},
        "token-user2-0000000000000000": {"user_id": "u2", "role": "user"},
    }
    monkeypatch.setenv("ECHOMIND_ENV", "production")
    monkeypatch.setenv("ECHOMIND_API_TOKENS", json.dumps(tokens))
    store = RunStore(tmp_path / "runtime.db")
    await store.bootstrap()
    monkeypatch.setattr(runs_api, "_store", store)
    monkeypatch.setattr(runs_api, "_dispatch", None)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api.app), base_url="http://test") as http:
        yield store, http, tokens


def auth(token):
    return {"Authorization": f"Bearer {token}"}


# ── 创建 ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_run_returns_202_and_persists(services):
    store, http = services
    resp = await http.post("/runs", json=body())
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "CREATED" and data["poll_after_ms"] == 500
    run = await store.get_run(data["run_id"])
    assert run.user_id == "u1" and run.task_type == "duplicate_charge_refund"
    assert run.input_json == {"message": "订单 #12345 好像被扣了两次款"}


@pytest.mark.asyncio
async def test_create_is_idempotent_for_same_message(services):
    store, http = services
    first = (await http.post("/runs", json=body())).json()
    second = (await http.post("/runs", json=body())).json()
    assert first["run_id"] == second["run_id"]
    assert (await store.find_run("u1", "c1", "msg-001")).id == first["run_id"]


@pytest.mark.asyncio
async def test_create_conflicts_when_same_request_id_has_different_message(services):
    _, http = services
    await http.post("/runs", json=body())
    conflict = await http.post("/runs", json=body(message="另一条完全不同的消息"))
    assert conflict.status_code == 409


@pytest.mark.asyncio
async def test_missing_conv_id_derives_stable_conv(services):
    _, http = services
    req = {k: v for k, v in body().items() if k != "conv_id"}
    first = (await http.post("/runs", json=req)).json()
    second = (await http.post("/runs", json=req)).json()
    assert first["run_id"] == second["run_id"]  # 同一 client_request_id 推导出同一 conv


@pytest.mark.asyncio
async def test_store_uninitialized_returns_503(services, monkeypatch):
    _, http = services
    monkeypatch.setattr(runs_api, "_store", None)
    resp = await http.post("/runs", json=body())
    assert resp.status_code == 503


# ── 查询与脱敏 ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_run_returns_view_with_steps_and_events(services):
    store, http = services
    run_id = (await http.post("/runs", json=body())).json()["run_id"]
    # 补充一个 Step 与一条含敏感字段的事件，验证视图与脱敏
    from runtime.models import RunStep, StepStatus
    await store.create_steps(run_id, [RunStep(run_id="", step_key="LOAD_ORDER", ordinal=0,
                                              status=StepStatus.PENDING)])
    await store.record_event(run_id, "TOOL_ATTEMPTED", step_key="LOAD_ORDER",
                             payload={"api_key": "sk-secret-abcdefghijklmnop", "ok": "yes"})
    resp = await http.get(f"/runs/{run_id}")
    assert resp.status_code == 200
    view = resp.json()
    assert view["run_id"] == run_id and view["status"] == "CREATED"
    assert [s["step_key"] for s in view["steps"]] == ["LOAD_ORDER"]
    types = [e["event_type"] for e in view["events"]]
    assert types == ["RUN_CREATED", "TOOL_ATTEMPTED"]
    assert view["events"][1]["payload_json"]["api_key"] == "[REDACTED]"
    assert view["events"][1]["payload_json"]["ok"] == "yes"


@pytest.mark.asyncio
async def test_get_missing_run_returns_404(services):
    _, http = services
    assert (await http.get("/runs/no-such-run")).status_code == 404


# ── 取消 ────────────────────────────────────────────────


async def setup_run_at(store, status, run_fields=None):
    fields = dict(user_id="u1", conv_id="c1", client_request_id="msg-001",
                  message_hash="h", task_type="duplicate_charge_refund",
                  plan_version="v1")
    fields.update(run_fields or {})
    run = await store.create_run(**fields)
    if status == RunStatus.CREATED:
        return run
    run = await store.update_run_status(run.id, 0, RunStatus.RUNNING)
    if status == RunStatus.RUNNING:
        return run
    return await store.update_run_status(run.id, run.version, status)


@pytest.mark.asyncio
async def test_cancel_waiting_run_transitions_to_cancelled(services):
    store, http = services
    run = await setup_run_at(store, RunStatus.WAITING_USER)
    resp = await http.post(f"/runs/{run.id}/cancel")
    assert resp.status_code == 202 and resp.json()["status"] == "CANCELLED"
    view = (await http.get(f"/runs/{run.id}")).json()
    assert view["status"] == "CANCELLED"
    assert view["events"][-1]["event_type"] == "RUN_CANCELLED"


@pytest.mark.asyncio
async def test_cancel_running_run_only_sets_flag(services):
    store, http = services
    run = await setup_run_at(store, RunStatus.RUNNING)
    resp = await http.post(f"/runs/{run.id}/cancel")
    assert resp.status_code == 202 and resp.json()["cancel_requested"] is True
    view = (await http.get(f"/runs/{run.id}")).json()
    assert view["status"] == "RUNNING" and view["cancel_requested"] is True


@pytest.mark.asyncio
async def test_cancel_terminal_run_conflicts(services):
    store, http = services
    run = await setup_run_at(store, RunStatus.SUCCEEDED)
    assert (await http.post(f"/runs/{run.id}/cancel")).status_code == 409


# ── 鉴权与隔离 ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_user_cannot_see_others_runs_but_admin_can(authed):
    store, http, tokens = authed
    run_id = (await http.post("/runs", json=body(), headers=auth("token-user1-0000000000000000"))).json()["run_id"]
    assert (await http.get(f"/runs/{run_id}", headers=auth("token-user2-0000000000000000"))).status_code == 404
    assert (await http.get(f"/runs/{run_id}", headers=auth("token-user1-0000000000000000"))).status_code == 200
    assert (await http.get(f"/runs/{run_id}", headers=auth("token-admin-0000000000000000"))).status_code == 200


@pytest.mark.asyncio
async def test_token_identity_overrides_user_id(authed):
    store, http, tokens = authed
    payload = body(user_id="evil-user")
    resp = await http.post("/runs", json=payload, headers=auth("token-user1-0000000000000000"))
    assert resp.status_code == 202
    assert await store.find_run("u1", "c1", "msg-001") is not None
    assert await store.find_run("evil-user", "c1", "msg-001") is None


@pytest.mark.asyncio
async def test_missing_token_returns_401(authed):
    _, http, _ = authed
    assert (await http.post("/runs", json=body())).status_code == 401
    assert (await http.get("/runs/whatever")).status_code == 401


@pytest.mark.asyncio
async def test_non_owner_cancel_is_hidden_and_admin_is_read_only(authed):
    store, http, tokens = authed
    run_id = (await http.post("/runs", json=body(), headers=auth("token-user1-0000000000000000"))).json()["run_id"]
    # 其他用户取消：按 404 隐藏资源存在性
    assert (await http.post(f"/runs/{run_id}/cancel", headers=auth("token-user2-0000000000000000"))).status_code == 404
    # 管理员对他人 Run 只读，不允许取消
    assert (await http.post(f"/runs/{run_id}/cancel", headers=auth("token-admin-0000000000000000"))).status_code == 403
    # 所有者本人可以取消
    run = await store.get_run(run_id)
    run = await store.update_run_status(run_id, run.version, RunStatus.RUNNING)
    await store.update_run_status(run_id, run.version, RunStatus.WAITING_USER)
    assert (await http.post(f"/runs/{run_id}/cancel", headers=auth("token-user1-0000000000000000"))).status_code == 202


@pytest.mark.asyncio
async def test_concurrent_create_same_request_key_is_idempotent(services):
    """P1：两个并发 POST /runs 同一请求键——TOCTOU 竞争也必须幂等返回同一 Run。"""
    store, http = services
    first, second = await asyncio.gather(
        http.post("/runs", json=body()),
        http.post("/runs", json=body()),
    )
    assert first.status_code == second.status_code == 202
    assert first.json()["run_id"] == second.json()["run_id"]
    assert len(await store.list_active_runs()) == 1  # 只有一行 Run


# ── 会话提交幂等与命名空间隔离（P0-3）──────────────────


@pytest.mark.asyncio
async def test_transcript_commit_is_idempotent_and_isolated_from_chat(monkeypatch):
    """Redis 提交成功、SQLite 标记失败后的恢复重试不得重复 LPUSH；
    run 专用回执键不得与 /chat 的 client_request_id 冲突。"""
    import fakeredis.aioredis
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from core.concurrency import KeyedLocks
    from memory.conversation_memory import MemoryManager
    from api.main import ChatResponse

    memory = MemoryManager.__new__(MemoryManager)
    memory._redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    memory._request_locks = KeyedLocks()
    memory._query_episodic = AsyncMock(return_value={})
    memory._get_profile = AsyncMock(return_value={})

    async def commit_transcript(run, answer):
        # 复刻 api/main.py 的提交回调语义
        response = ChatResponse(conv_id=run.conv_id, request_id=run.id,
            response=answer, intent="other", agent_type="general", escalated=False,
            latency_ms=0, status="success")
        message = (run.input_json or {}).get("message", "")
        run_turn_id = f"run:{run.id}:final"
        async with memory.conversation(run.user_id, run.conv_id):
            receipt = await memory.get_turn_receipt(run.user_id, run.conv_id, run_turn_id)
            if receipt:
                if receipt.get("message") != message:
                    return
                return  # 已提交过
            await memory.commit_turn(run.user_id, run.conv_id, run_turn_id,
                                     message, response.model_dump())

    run = SimpleNamespace(id="run-1", user_id="u1", conv_id="c1",
                          input_json={"message": "订单 #12345 被扣了两次款，99 元，昨天"})
    await commit_transcript(run, "回答")
    await commit_transcript(run, "回答")  # Redis 成功、SQLite 标记失败后的恢复重试
    messages = await memory._redis.lrange(memory._wm_key("u1", "c1"), 0, -1)
    assert len(messages) == 2  # 一对消息，没有重复 LPUSH
    # run 命名空间与 /chat 的 client_request_id 隔离
    assert await memory.get_turn_receipt("u1", "c1", "run:run-1:final") is not None
    assert await memory.get_turn_receipt("u1", "c1", "msg-001") is None
    await memory._redis.aclose()


# ── resume 与 approval（PR5）───────────────────────────


@pytest.mark.asyncio
async def test_resume_on_waiting_user_merges_input_and_dispatches(services, monkeypatch):
    store, http = services
    dispatched = []

    async def fake_dispatch(run_id):
        dispatched.append(run_id)

    monkeypatch.setattr(runs_api, "_dispatch", fake_dispatch)

    run = await setup_run_at(store, RunStatus.WAITING_USER)
    version = run.version
    resp = await http.post(f"/runs/{run.id}/resume",
                           json={"message": "订单号是 #12345，99 元，昨天支付", "expected_version": version})
    assert resp.status_code == 202
    updated = await store.get_run(run.id)
    assert updated.status == RunStatus.RUNNING
    assert updated.input_json["supplements"] == ["订单号是 #12345，99 元，昨天支付"]
    assert dispatched == [run.id]  # 恢复后调度引擎继续


@pytest.mark.asyncio
async def test_resume_version_mismatch_and_terminal_conflicts(services):
    store, http = services
    run = await setup_run_at(store, RunStatus.WAITING_USER)
    resp = await http.post(f"/runs/{run.id}/resume",
                           json={"message": "补充", "expected_version": run.version + 99})
    assert resp.status_code == 409
    done = await setup_run_at(store, RunStatus.SUCCEEDED,
                              run_fields=dict(client_request_id="msg-002"))
    resp = await http.post(f"/runs/{done.id}/resume",
                           json={"message": "补充", "expected_version": done.version})
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_approval_flow_and_decide_once(services, monkeypatch):
    from runtime.models import ApprovalDecision
    # 无认证开发环境：显式开启匿名审批开关（默认拒绝）
    monkeypatch.setenv("ECHOMIND_RUNTIME_ALLOW_ANONYMOUS_APPROVAL", "1")
    store, http = services
    dispatched = []

    async def fake_dispatch(run_id):
        dispatched.append(run_id)

    monkeypatch.setattr(runs_api, "_dispatch", fake_dispatch)

    run = await setup_run_at(store, RunStatus.WAITING_APPROVAL,
                             run_fields={"user_id": "anonymous"})
    await store.create_approval(run.id, "APPROVAL_GATE", "风险摘要")

    resp = await http.post(f"/runs/{run.id}/approval",
                           json={"step_key": "APPROVAL_GATE", "decision": "approve",
                                 "comment": "金额一致", "expected_version": run.version})
    assert resp.status_code == 202 and resp.json()["decision"] == "approved"
    assert (await store.get_approval(run.id, "APPROVAL_GATE")).decision == ApprovalDecision.APPROVED
    assert (await store.get_run(run.id)).status == RunStatus.RUNNING
    assert dispatched == [run.id]

    # 决定不可覆盖
    run = await store.get_run(run.id)
    again = await http.post(f"/runs/{run.id}/approval",
                            json={"step_key": "APPROVAL_GATE", "decision": "reject",
                                  "expected_version": run.version})
    assert again.status_code == 409


@pytest.mark.asyncio
async def test_approval_reject_fails_run_safely(services, monkeypatch):
    from runtime.models import ApprovalDecision, StepStatus
    from runtime.plans import plan_for
    # 无认证开发环境：显式开启匿名审批开关（默认拒绝）
    monkeypatch.setenv("ECHOMIND_RUNTIME_ALLOW_ANONYMOUS_APPROVAL", "1")
    store, http = services
    run = await setup_run_at(store, RunStatus.WAITING_APPROVAL,
                             run_fields={"user_id": "anonymous"})
    await store.create_steps(run.id, plan_for(run.task_type))
    for _from, _to in ((StepStatus.PENDING, StepStatus.READY),
                       (StepStatus.READY, StepStatus.RUNNING),
                       (StepStatus.RUNNING, StepStatus.WAITING_APPROVAL)):
        await store.update_step_status(run.id, "APPROVAL_GATE", _from, _to)
    await store.create_approval(run.id, "APPROVAL_GATE", "风险摘要")

    resp = await http.post(f"/runs/{run.id}/approval",
                           json={"step_key": "APPROVAL_GATE", "decision": "reject",
                                 "expected_version": run.version})
    assert resp.status_code == 202 and resp.json()["decision"] == "rejected"
    final = await store.get_run(run.id)
    assert final.status == RunStatus.FAILED and final.error_code == "approval_rejected"
    assert "不执行退款" in final.response_json["response"]
    assert (await store.get_step(run.id, "APPROVAL_GATE")).status == StepStatus.FAILED


# ── 无认证审批：默认拒绝 + 显式开关（用户裁决 C）─────────


@pytest.mark.asyncio
async def test_anonymous_approval_denied_by_default(services, monkeypatch):
    """无认证且开关未设：approve/reject 均 403，审批保持 pending。"""
    from runtime.models import ApprovalDecision
    monkeypatch.delenv("ECHOMIND_RUNTIME_ALLOW_ANONYMOUS_APPROVAL", raising=False)
    store, http = services
    run = await setup_run_at(store, RunStatus.WAITING_APPROVAL)
    await store.create_approval(run.id, "APPROVAL_GATE", "风险摘要")

    for decision in ("approve", "reject"):
        resp = await http.post(
            f"/runs/{run.id}/approval",
            json={"step_key": "APPROVAL_GATE", "decision": decision,
                  "expected_version": run.version})
        assert resp.status_code == 403
        assert "ECHOMIND_RUNTIME_ALLOW_ANONYMOUS_APPROVAL" in resp.json()["detail"]
    assert (await store.get_approval(run.id, "APPROVAL_GATE")).decision \
        == ApprovalDecision.PENDING


@pytest.mark.asyncio
async def test_anonymous_approval_allowed_with_explicit_switch(services, monkeypatch):
    """无认证但显式开关=1：仅允许 anonymous 用户自己的 Run。"""
    from runtime.models import ApprovalDecision
    monkeypatch.setenv("ECHOMIND_RUNTIME_ALLOW_ANONYMOUS_APPROVAL", "1")
    store, http = services
    run = await setup_run_at(store, RunStatus.WAITING_APPROVAL,
                             run_fields={"user_id": "anonymous"})
    await store.create_approval(run.id, "APPROVAL_GATE", "风险摘要")

    resp = await http.post(
        f"/runs/{run.id}/approval",
        json={"step_key": "APPROVAL_GATE", "decision": "approve",
              "expected_version": run.version})
    assert resp.status_code == 202 and resp.json()["decision"] == "approved"
    assert (await store.get_approval(run.id, "APPROVAL_GATE")).decision \
        == ApprovalDecision.APPROVED


@pytest.mark.asyncio
async def test_anonymous_switch_cannot_approve_other_users_run(services, monkeypatch):
    """开关开启时，匿名者也不能审批非 anonymous 用户的 Run。"""
    monkeypatch.setenv("ECHOMIND_RUNTIME_ALLOW_ANONYMOUS_APPROVAL", "1")
    store, http = services
    run = await setup_run_at(store, RunStatus.WAITING_APPROVAL)  # user_id=u1
    await store.create_approval(run.id, "APPROVAL_GATE", "风险摘要")
    resp = await http.post(
        f"/runs/{run.id}/approval",
        json={"step_key": "APPROVAL_GATE", "decision": "approve",
              "expected_version": run.version})
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_approval_permissions_and_missing(authed):
    store, http, tokens = authed
    run_id = (await http.post("/runs", json=body(), headers=auth("token-user1-0000000000000000"))).json()["run_id"]
    run = await store.get_run(run_id)
    run = await store.update_run_status(run_id, run.version, RunStatus.RUNNING)
    run = await store.update_run_status(run_id, run.version, RunStatus.WAITING_APPROVAL)
    await store.create_approval(run_id, "APPROVAL_GATE", "风险摘要")

    # 审批记录缺失 → 404（用 admin 查询他人 Run，避免触发 owner 自批 403）
    admin_token = next(t for t, info in tokens.items() if info["role"] == "admin")
    other = (await http.post("/runs", json=body(client_request_id="msg-002"), headers=auth("token-user1-0000000000000000"))).json()["run_id"]
    other_run = await store.get_run(other)
    other_run = await store.update_run_status(other, other_run.version, RunStatus.RUNNING)
    other_run = await store.update_run_status(other, other_run.version, RunStatus.WAITING_APPROVAL)
    resp = await http.post(f"/runs/{other}/approval",
                           json={"step_key": "APPROVAL_GATE", "decision": "approve",
                                 "expected_version": other_run.version},
                           headers=auth(admin_token))
    assert resp.status_code == 404
    # 其他用户审批 → 404 隐藏资源存在性
    resp = await http.post(f"/runs/{run_id}/approval",
                           json={"step_key": "APPROVAL_GATE", "decision": "approve",
                                 "expected_version": run.version},
                           headers=auth("token-user2-0000000000000000"))
    assert resp.status_code == 404
    # 管理员可以审批（7.4 节：所有者或管理员）
    resp = await http.post(f"/runs/{run_id}/approval",
                           json={"step_key": "APPROVAL_GATE", "decision": "approve",
                                 "expected_version": run.version},
                           headers=auth("token-admin-0000000000000000"))
    assert resp.status_code == 202 and resp.json()["decision"] == "approved"


# ── S4：Step 输入/证据脱敏 ─────────────────────────────


@pytest.mark.asyncio
async def test_get_run_scrubs_step_input_evidence_and_business_ids(services):
    """S4/12.3：Step 的 input/evidence（含 user_id、扣款明细、操作号）必须脱敏。"""
    store, http = services
    run_id = (await http.post("/runs", json=body())).json()["run_id"]
    from runtime.models import RunStep, StepStatus
    await store.create_steps(run_id, [RunStep(
        run_id="", step_key="LOAD_ORDER", ordinal=0, status=StepStatus.PENDING,
        idempotency_key="IDEMSECRET1234",
        input_json={"user_id": "USERSECRET8888", "order_id": "ORDERSECRET7777",
                    "api_key": "not-a-real-api-key"},
        output_json={"user_id": "OUTPUTSECRET2222",
                     "refund_operation_id": "OUTREFSECRET1111"},
        evidence_json={"operation_id": "OPSECRET9999",
                       "refund_operation_id": "REFUNDSECRET5555",
                       "charges": [{"amount_cents": 9900, "user_id": "CHARGESECRET6666"}]},
    )])
    await store.record_event(run_id, "OUTCOME_UNKNOWN", step_key="LOAD_ORDER",
                             payload={"operation_id": "EVENTOPSECRET3333",
                                      "refund_operation_id": "EVENTREFSECRET4444",
                                      "user_id": "EVENTUSERSECRET0000", "ok": "yes"})

    view = (await http.get(f"/runs/{run_id}")).json()
    step = view["steps"][0]
    assert step["input_json"]["user_id"] == "***8888"
    assert step["input_json"]["order_id"] == "***7777"
    assert step["input_json"]["api_key"] == "[REDACTED]"
    assert step["output_json"]["user_id"] == "***2222"
    assert step["output_json"]["refund_operation_id"] == "***1111"
    assert step["evidence_json"]["operation_id"] == "***9999"
    assert step["evidence_json"]["refund_operation_id"] == "***5555"
    assert step["evidence_json"]["charges"][0]["user_id"] == "***6666"
    assert step["evidence_json"]["charges"][0]["amount_cents"] == 9900  # 业务金额保留
    assert step["idempotency_key"] == "***1234"  # 幂等键不返回明文
    event = next(e for e in view["events"] if e["event_type"] == "OUTCOME_UNKNOWN")
    assert event["payload_json"]["operation_id"] == "***3333"
    assert event["payload_json"]["refund_operation_id"] == "***4444"
    assert event["payload_json"]["user_id"] == "***0000"
    assert event["payload_json"]["ok"] == "yes"
    raw = json.dumps(view, ensure_ascii=False)
    for secret in ("USERSECRET8888", "ORDERSECRET7777", "OPSECRET9999",
                   "CHARGESECRET6666", "not-a-real-api-key", "IDEMSECRET1234",
                   "OUTPUTSECRET2222", "OUTREFSECRET1111", "REFUNDSECRET5555",
                   "EVENTOPSECRET3333", "EVENTREFSECRET4444", "EVENTUSERSECRET0000"):
        assert secret not in raw


# ── S9：resume 指标 ──────────────────────────────────


def _counter_value(counter, **labels):
    for metric in counter.collect():
        for sample in metric.samples:
            if sample.name.endswith("_total") \
                    and all(sample.labels.get(k) == v for k, v in labels.items()):
                return sample.value
    return 0


@pytest.mark.asyncio
async def test_resume_records_user_input_metric(services, monkeypatch):
    """S9/12.2：POST /runs/{id}/resume 必须记录 run_resume_total{reason=user_input}。"""
    from runtime import metrics as runtime_metrics
    store, http = services

    async def fake_dispatch(run_id):
        return None

    monkeypatch.setattr(runs_api, "_dispatch", fake_dispatch)
    run = await setup_run_at(store, RunStatus.WAITING_USER)
    before = _counter_value(runtime_metrics.resume_total, reason="user_input")
    resp = await http.post(f"/runs/{run.id}/resume",
                           json={"message": "补充：订单号 #12345", "expected_version": run.version})
    assert resp.status_code == 202
    after = _counter_value(runtime_metrics.resume_total, reason="user_input")
    assert after == before + 1


# ── S1 竞态：真实 API 取消 + 调度引擎对账 ───────────────


@pytest.mark.asyncio
async def test_cancel_running_run_dispatches_engine_and_reconciles_success(services,
                                                                           monkeypatch):
    """S1：POST /cancel（RUNNING）必须唤醒引擎；写工具已成功时对账为 SUCCEEDED 而非 CANCELLED。

    走真实 API + engine.schedule 路径，不使用裸 SQL 置取消标记。
    """
    import pathlib
    import sqlite3
    from runtime.billing import SandboxBillingClient, demo_seed
    from runtime.engine import TaskEngine
    from runtime.models import RunStep, StepStatus

    store, http = services
    billing = SandboxBillingClient(pathlib.Path(store.path).parent / "billing.db")
    await billing.bootstrap()
    await billing.seed(*demo_seed())

    run = await store.create_run(
        user_id="u1", conv_id="c1", client_request_id="msg-cancel",
        message_hash="h", task_type="duplicate_charge_refund", plan_version="v1",
        input_json={"message": "订单 #12345 被扣了两次款，99 元，昨天支付"}, context_json={})
    # 崩溃遗留场景：写工具结果未知，但幂等键与远端退款记录都已存在
    await store.create_steps(run.id, [
        RunStep(run_id="", step_key="SUBMIT_REFUND", ordinal=0, tool_name="submit_refund",
                status=StepStatus.OUTCOME_UNKNOWN, idempotency_key="op-cancel-1"),
        RunStep(run_id="", step_key="POLL_REFUND_STATUS", ordinal=1,
                tool_name="get_refund_status", status=StepStatus.PENDING),
        RunStep(run_id="", step_key="BUILD_FINAL_ANSWER", ordinal=2,
                status=StepStatus.PENDING),
    ])
    await billing.submit_refund("12345", "u1", 9900, "重复扣款", operation_id="op-cancel-1")
    await billing.get_refund_status("op-cancel-1", "u1")  # submitted → processing
    run = await store.update_run_status(run.id, 0, RunStatus.RUNNING)

    committed = []

    async def commit(fresh, answer):
        committed.append(fresh.id)

    engine = TaskEngine(store, billing, transcript_commit=commit)

    async def dispatch(run_id):
        engine.schedule(run_id)
        while run_id in engine._inflight:
            await asyncio.sleep(0)

    monkeypatch.setattr(runs_api, "_dispatch", dispatch)

    resp = await http.post(f"/runs/{run.id}/cancel")
    assert resp.status_code == 202 and resp.json()["cancel_requested"] is True
    final = await store.get_run(run.id)
    assert final.status == RunStatus.SUCCEEDED  # 退款已成功，不能宣称取消
    assert "已原路退回" in final.response_json["response"]
    assert final.transcript_committed is True and committed == [run.id]
    conn = sqlite3.connect(billing.path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM refunds").fetchone()[0] == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_schedule_during_inflight_is_replayed_once():
    """S1 竞态：同 Run 执行中再次 schedule（如 cancel 触发）必须在首任务结束后补跑一次。"""
    from runtime.engine import TaskEngine
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        store = RunStore(f"{tmp}/runtime.db")
        await store.bootstrap()
        from runtime.billing import SandboxBillingClient
        billing = SandboxBillingClient(f"{tmp}/billing.db")
        await billing.bootstrap()
        engine = TaskEngine(store, billing)
        run = await store.create_run(
            user_id="u1", conv_id="c1", client_request_id="run-x", message_hash="h",
            task_type="duplicate_charge_refund", plan_version="v1")
        calls = []
        started = asyncio.Event()
        release = asyncio.Event()

        async def fake_run(run_id):
            calls.append(run_id)
            started.set()
            await release.wait()

        engine.run = fake_run  # 只替换执行体，保留 _guarded 的待重调度逻辑
        engine.schedule(run.id)
        await started.wait()
        engine.schedule(run.id)  # 执行中：应登记待重调度
        assert engine._pending == {run.id}
        release.set()
        for _ in range(50):
            if calls == [run.id, run.id]:
                break
            await asyncio.sleep(0.01)
        assert calls == [run.id, run.id]
        assert engine._pending == set() and engine._inflight == set()


@pytest.mark.asyncio
async def test_api_dispatch_accepts_sync_engine_schedule(services, monkeypatch):
    """LOW：生产路径 _dispatch = engine.schedule 是同步函数；POST /runs、cancel、approval 均不得 TypeError。"""
    import pathlib
    from runtime.billing import SandboxBillingClient
    from runtime.engine import TaskEngine

    store, http = services
    billing = SandboxBillingClient(pathlib.Path(store.path).parent / "billing-sync.db")
    await billing.bootstrap()
    engine = TaskEngine(store, billing)
    monkeypatch.setattr(runs_api, "_dispatch", engine.schedule)  # 同步生产回调

    async def wait_until(predicate, timeout=3.0):
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if await predicate():
                return
            await asyncio.sleep(0.01)
        raise AssertionError("等待状态超时")

    async def status_is(run_id, expected):
        return (await store.get_run(run_id)).status == expected

    # POST /runs → 调度引擎 → WAITING_USER
    created = await http.post("/runs", json=body(message="我这个月好像被扣了两次款"))
    assert created.status_code == 202
    created_id = created.json()["run_id"]
    await wait_until(lambda: status_is(created_id, RunStatus.WAITING_USER))

    # cancel → 调度引擎 → CANCELLED
    cancel_run = await setup_run_at(store, RunStatus.RUNNING,
                                    run_fields={"client_request_id": "msg-sync-cancel"})
    assert (await http.post(f"/runs/{cancel_run.id}/cancel")).status_code == 202
    await wait_until(lambda: status_is(cancel_run.id, RunStatus.CANCELLED))

    # approval → 调度引擎 → 继续执行（缺信息则回到 WAITING_USER）
    approval_run = await setup_run_at(
        store, RunStatus.WAITING_APPROVAL,
        run_fields={"client_request_id": "msg-sync-approval", "user_id": "anonymous"})
    await store.create_approval(approval_run.id, "APPROVAL_GATE", "风险摘要")
    monkeypatch.setenv("ECHOMIND_RUNTIME_ALLOW_ANONYMOUS_APPROVAL", "1")
    resp = await http.post(
        f"/runs/{approval_run.id}/approval",
        json={"step_key": "APPROVAL_GATE", "decision": "approve",
              "expected_version": approval_run.version})
    assert resp.status_code == 202
    await wait_until(lambda: status_is(approval_run.id, RunStatus.WAITING_USER))


# ── S4：真实 FAILED 流程的全视图明文检查 ────────────────


def test_scrub_text_masks_order_ids_without_false_positives():
    """LOW/MEDIUM：自由文本脱敏与引擎文法一致，覆盖常见写法且不误伤普通文本。"""
    mask = runs_api._scrub_text
    assert mask("订单 #12345 被扣款") == "订单 #***2345 被扣款"
    assert mask("订单号是12345") == "订单号是***2345"
    assert mask("订单 1234") == "订单 ***"
    assert mask("订单 ABCD") == "订单 ***"
    assert mask("order id 12345 failed") == "order id ***2345 failed"
    assert mask("order_id ORDPLAIN12345") == "order_id ***2345"
    assert mask("#:ORDPLAIN12345") == "#:***2345"
    assert mask("订单 ORDPLAIN12345") == "订单 ***2345"
    assert mask("# Overview") == "# Overview"
    assert mask("order overview") == "order overview"
    assert mask("2026-09-01") == "2026-09-01"
    # 已知业务 ID 精确替换：无前缀也能脱敏
    assert mask("编号 ORDPLAIN12345 已处理", {"ORDPLAIN12345"}) == "编号 ***2345 已处理"


@pytest.mark.asyncio
async def test_failed_run_view_has_no_plaintext_business_ids(services):
    """安全复审 MEDIUM：订单号/用户/审批人/幂等键在 GET /runs 全视图中不得明文出现。"""
    import pathlib
    from runtime.billing import SandboxBillingClient
    from runtime.engine import TaskEngine
    from runtime.models import ApprovalDecision, EventSpec

    order_id = "ORDPLAIN12345"
    store, http = services
    billing = SandboxBillingClient(pathlib.Path(store.path).parent / "billing-plain.db")
    await billing.bootstrap()
    await billing.seed(
        [{"order_id": order_id, "user_id": "u1", "amount_cents": 9900,
          "status": "paid", "created_at": "2026-11-01T10:00:00"}],
        [{"charge_id": "chg-plain-1", "order_id": order_id, "user_id": "u1",
          "amount_cents": 9900, "paid_at": "2026-11-01T10:01:00", "channel": "wechat"},
         {"charge_id": "chg-plain-2", "order_id": order_id, "user_id": "u1",
          "amount_cents": 9900, "paid_at": "2026-11-01T10:02:00", "channel": "wechat"}])

    async def search(params, ctx):
        return [{"title": "退款政策", "content": "重复扣款可在 7 日内申请原路退回"}]

    engine = TaskEngine(store, billing, tools={"search_knowledge_base": search})

    async def refund_flow(client_request_id):
        run = await store.create_run(
            user_id="u1", conv_id="c1", client_request_id=client_request_id,
            message_hash="h", task_type="duplicate_charge_refund", plan_version="v1",
            input_json={"message": f"用户 u1 的订单 #{order_id} 这个月被扣了两次款，99 元，昨天支付"})
        await engine.run(run.id)
        current = await store.get_run(run.id)
        await store.decide_approval(run.id, "APPROVAL_GATE",
                                    ApprovalDecision.APPROVED, decided_by="admin")
        current = await store.update_run_status(
            run.id, current.version, RunStatus.RUNNING,
            event=EventSpec("RUN_RESUMED"))
        await engine.run(run.id)
        return await store.get_run(run.id)

    first = await refund_flow("msg-plain-1")
    assert first.status == RunStatus.SUCCEEDED
    second = await refund_flow("msg-plain-2")
    assert second.status == RunStatus.FAILED
    assert second.error_code == "refund_limit_exceeded"

    view = (await http.get(f"/runs/{second.id}")).json()
    raw = json.dumps(view, ensure_ascii=False)
    for plaintext in (order_id, "u1", "admin", "chg-plain-1", "chg-plain-2"):
        assert plaintext not in raw, plaintext
    assert "***2345" in raw  # 订单号仍以掩码形式保留可诊断性
    approval = view["approvals"][0]
    assert approval["decided_by"] == "***dmin"


@pytest.mark.asyncio
async def test_order_id_variant_message_and_task_type_are_masked(services):
    """MEDIUM：'order_id XXX' 消息进入 Run 后不得在视图明文出现；task_type 走白名单。"""
    import pathlib
    from runtime.billing import SandboxBillingClient
    from runtime.engine import TaskEngine

    order_id = "ORDPLAIN12345"
    store, http = services
    billing = SandboxBillingClient(pathlib.Path(store.path).parent / "billing-variant.db")
    await billing.bootstrap()
    await billing.seed(
        [{"order_id": order_id, "user_id": "u1", "amount_cents": 9900,
          "status": "paid", "created_at": "2026-11-01T10:00:00"}],
        [{"charge_id": "chg-variant-1", "order_id": order_id, "user_id": "u1",
          "amount_cents": 9900, "paid_at": "2026-11-01T10:01:00", "channel": "wechat"},
         {"charge_id": "chg-variant-2", "order_id": order_id, "user_id": "u1",
          "amount_cents": 9900, "paid_at": "2026-11-01T10:02:00", "channel": "wechat"}])

    async def search(params, ctx):
        return [{"title": "退款政策", "content": "重复扣款可在 7 日内申请原路退回"}]

    engine = TaskEngine(store, billing, tools={"search_knowledge_base": search})
    run = await store.create_run(
        user_id="u1", conv_id="c1", client_request_id="msg-variant",
        message_hash="h", task_type="duplicate_charge_refund", plan_version="v1",
        input_json={"message": f"order_id {order_id} 被扣了两次款，99 元，昨天支付"})
    await engine.run(run.id)
    assert (await store.get_run(run.id)).status == RunStatus.WAITING_APPROVAL

    view = (await http.get(f"/runs/{run.id}")).json()
    assert order_id not in json.dumps(view, ensure_ascii=False)
    # task_type 由 API 白名单限制，任意字符串无法进入存储/视图
    bad = body(client_request_id="msg-bad-task")
    bad["task_type"] = f"dup_{order_id}"
    assert (await http.post("/runs", json=bad)).status_code == 422


# ── 审批禁止自批（用户裁决）─────────────────────────────


@pytest.mark.asyncio
async def test_owner_cannot_approve_own_refund_admin_can(authed):
    """有认证身份时：owner approve/reject → 403；非 owner → 404；admin → 202。"""
    from runtime.models import ApprovalDecision

    store, http, tokens = authed
    run_id = (await http.post("/runs", json=body(),
                              headers=auth("token-user1-0000000000000000"))).json()["run_id"]
    run = await store.get_run(run_id)
    run = await store.update_run_status(run_id, run.version, RunStatus.RUNNING)
    run = await store.update_run_status(run_id, run.version, RunStatus.WAITING_APPROVAL)
    await store.create_approval(run_id, "APPROVAL_GATE", "风险摘要")

    for decision in ("approve", "reject"):
        resp = await http.post(
            f"/runs/{run_id}/approval",
            json={"step_key": "APPROVAL_GATE", "decision": decision,
                  "expected_version": run.version},
            headers=auth("token-user1-0000000000000000"))
        assert resp.status_code == 403
    approval = await store.get_approval(run_id, "APPROVAL_GATE")
    assert approval.decision == ApprovalDecision.PENDING  # 自批被拒后决定未落库

    resp = await http.post(
        f"/runs/{run_id}/approval",
        json={"step_key": "APPROVAL_GATE", "decision": "approve",
              "expected_version": run.version},
        headers=auth("token-user2-0000000000000000"))
    assert resp.status_code == 404  # 非所有者按 404 隐藏资源存在性

    resp = await http.post(
        f"/runs/{run_id}/approval",
        json={"step_key": "APPROVAL_GATE", "decision": "approve",
              "expected_version": run.version},
        headers=auth(next(t for t, info in tokens.items() if info["role"] == "admin")))
    assert resp.status_code == 202 and resp.json()["decision"] == "approved"
    assert (await store.get_approval(run_id, "APPROVAL_GATE")).decision \
        == ApprovalDecision.APPROVED


@pytest.mark.asyncio
async def test_admin_cannot_approve_own_run(authed):
    """自批禁令对所有角色生效：admin 也不能审批自己发起的 Run。"""
    from runtime.models import ApprovalDecision
    store, http, tokens = authed
    admin_token = next(t for t, info in tokens.items() if info["role"] == "admin")
    run_id = (await http.post("/runs", json=body(), headers=auth(admin_token))).json()["run_id"]
    run = await store.get_run(run_id)
    assert run.user_id == "admin"
    run = await store.update_run_status(run_id, run.version, RunStatus.RUNNING)
    run = await store.update_run_status(run_id, run.version, RunStatus.WAITING_APPROVAL)
    await store.create_approval(run_id, "APPROVAL_GATE", "风险摘要")

    resp = await http.post(
        f"/runs/{run_id}/approval",
        json={"step_key": "APPROVAL_GATE", "decision": "approve",
              "expected_version": run.version},
        headers=auth(admin_token))
    assert resp.status_code == 403
    assert (await store.get_approval(run_id, "APPROVAL_GATE")).decision \
        == ApprovalDecision.PENDING
