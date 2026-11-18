"""Runs API 回归：创建幂等、消息冲突、属主隔离、取消与鉴权覆盖。

使用 ASGI 直连（不经过真实网络），依赖临时 SQLite，不需要 Redis/模型/外网。
"""
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
    store, http = services
    dispatched = []

    async def fake_dispatch(run_id):
        dispatched.append(run_id)

    monkeypatch.setattr(runs_api, "_dispatch", fake_dispatch)

    run = await setup_run_at(store, RunStatus.WAITING_APPROVAL)
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
async def test_approval_reject_fails_run_safely(services):
    from runtime.models import ApprovalDecision, StepStatus
    from runtime.plans import plan_for
    store, http = services
    run = await setup_run_at(store, RunStatus.WAITING_APPROVAL)
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


@pytest.mark.asyncio
async def test_approval_permissions_and_missing(authed):
    store, http, tokens = authed
    run_id = (await http.post("/runs", json=body(), headers=auth("token-user1-0000000000000000"))).json()["run_id"]
    run = await store.get_run(run_id)
    run = await store.update_run_status(run_id, run.version, RunStatus.RUNNING)
    run = await store.update_run_status(run_id, run.version, RunStatus.WAITING_APPROVAL)
    await store.create_approval(run_id, "APPROVAL_GATE", "风险摘要")

    # 无审批记录 → 404
    other = (await http.post("/runs", json=body(client_request_id="msg-002"), headers=auth("token-user1-0000000000000000"))).json()["run_id"]
    other_run = await store.get_run(other)
    other_run = await store.update_run_status(other, other_run.version, RunStatus.RUNNING)
    other_run = await store.update_run_status(other, other_run.version, RunStatus.WAITING_APPROVAL)
    resp = await http.post(f"/runs/{other}/approval",
                           json={"step_key": "APPROVAL_GATE", "decision": "approve",
                                 "expected_version": other_run.version},
                           headers=auth("token-user1-0000000000000000"))
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
