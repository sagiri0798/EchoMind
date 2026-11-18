# EchoMind Task Runtime v1 实施计划

## 1. 目标

把 EchoMind 从“单轮聊天内的有界 Tool Loop”升级为“可暂停、审批、恢复、取消和幂等执行的客服任务运行时”，并用一个可验证的退款异常处理沙箱证明该能力。

首个旗舰任务：

```text
用户反馈重复扣款
→ 收集订单信息
→ 查询订单与扣款记录
→ 判断是否疑似重复扣款
→ 检索退款政策
→ 生成处理建议
→ 超过风险阈值时等待人工审批
→ 在沙箱中提交退款申请
→ 查询退款最终状态
→ 生成最终回复并一次性提交会话
```

本阶段成功标准不是“模型能自己规划退款”，而是系统能够证明：

- 任务暂停后可以恢复。
- 任务恢复后不会重复产生副作用。
- 外部系统已经执行、但本地状态未提交时，可以查询并收敛结果。
- 模型、工具和用户都不能绕过审批和权限。
- 每次失败都能定位到具体 Task、Step、Tool、参数或 Evidence。

## 2. 实施范围

### 2.1 本阶段实现

- Python 后端。
- 新增 `/runs` 任务运行时 API，不改变现有 `/chat` 外部行为。
- SQLite 持久化 Run、Step、Approval、Event。
- 固定、审核过的退款任务 DAG，不做自由 Planner。
- 可暂停等待用户补充信息或人工审批。
- 进程重启后的恢复与外部结果对账。
- 独立的沙箱账单/退款存储，用于模拟外部业务系统。
- 工具读/写风险、审批和幂等执行。
- 结构化任务 trace、Prometheus 指标和确定性评测集。

### 2.2 Python / Java 选择

本阶段以 Python 为参考实现。Java 版只记录后续 parity backlog，不进行同步开发，避免两套运行时同时变更扩大范围。

### 2.3 明确不做

- 不做任意任务自由生成 Planner。
- 不接真实支付、订单或退款系统。
- 不做前端页面。
- 不做语音、多模态、Kafka、MySQL、ES。
- 不重写现有会话锁、Redis 记忆和 `/chat` 提交流程。
- 不宣称 exactly-once；目标是“外部副作用至多一次，结果最终可确认”。
- 不增加没有数据集证明的准确率、缓存命中率或业务收益数字。

## 3. 现有代码基线

开始前必须阅读：

- 根目录 [`AGENTS.md`](AGENTS.md)。
- [`EchoMind/README.md`](EchoMind/README.md)。
- [`详细文档+简历说明/业务流程说明.md`](详细文档+简历说明/业务流程说明.md)。
- [`详细文档+简历说明/重点代码.md`](详细文档+简历说明/重点代码.md)。
- [`EchoMind/api/main.py`](EchoMind/api/main.py)。
- [`EchoMind/agents/agent_orchestrator.py`](EchoMind/agents/agent_orchestrator.py)。
- [`EchoMind/tooling/tool_manager.py`](EchoMind/tooling/tool_manager.py)。
- [`EchoMind/memory/conversation_memory.py`](EchoMind/memory/conversation_memory.py)。
- [`EchoMind/tests/test_chat_reliability.py`](EchoMind/tests/test_chat_reliability.py)。

必须保留的现有行为：

- `/chat` 的 50 秒预算、会话锁、回执和 Redis 整轮提交。
- `client_request_id` 冲突返回 409。
- `retry_safe=false` 工具执行前写入回执。
- 失败轮次不写入正常对话历史。
- 知识库失败不能被当作成功证据。
- 现有鉴权、trace、Prometheus 和评测接口。

## 4. 架构

### 4.1 新增模块

建议目录：

```text
EchoMind/
  runtime/
    __init__.py
    models.py          # Run/Step/Approval/Event 数据模型
    store.py           # SQLite 持久化与原子状态转换
    state_machine.py   # 合法状态和转换
    engine.py          # Task Runner 与执行循环
    plans.py           # v1 固定退款 DAG
    policy.py          # 读写风险、审批与幂等键
    tools.py           # Runtime Tool 适配器
    billing.py         # 沙箱账单/退款客户端
    api.py             # FastAPI router
    metrics.py         # Prometheus 指标
    recovery.py        # 启动扫描与对账
  tests/
    test_runtime_store.py
    test_runtime_api.py
    test_runtime_engine.py
    test_runtime_recovery.py
    test_billing_sandbox.py
  runtime-eval/
    refund-v1.json
```

### 4.2 与现有模块的关系

- `RunStore` 是任务执行状态的唯一事实源。
- Redis 会话记忆仍负责聊天上下文、回执和最终对话提交。
- 现有知识库和 `MCPToolManager` 负责只读检索工具。
- 沙箱账单数据库与 `RunStore` 使用不同数据库文件，模拟外部系统边界。
- `TaskEngine` 不直接把任意工具交给 LLM；固定 DAG 决定允许执行的工具。
- 任务完成后，通过 `MemoryManager.commit_turn()` 只提交一次最终问答。

### 4.3 启动集成

在 [`EchoMind/api/main.py`](EchoMind/api/main.py) 的 lifespan 中初始化：

```text
RunStore
SandboxBillingClient
TaskRuntime
TaskRecovery
```

并把 runtime router 挂到 FastAPI。关闭应用时停止后台 runner、关闭 SQLite 连接。

## 5. 状态模型

### 5.1 RunStatus

```text
CREATED
RUNNING
WAITING_USER
WAITING_APPROVAL
PAUSED
OUTCOME_UNKNOWN
SUCCEEDED
FAILED
CANCELLED
```

`OUTCOME_UNKNOWN` 同时用于 Run 和写 Step。它表示“外部系统可能已经执行，但本地尚未得到可确认结果”，不要在查询远端前直接判定失败。

### 5.2 StepStatus

```text
PENDING
READY
RUNNING
WAITING_APPROVAL
SUCCEEDED
FAILED
SKIPPED
OUTCOME_UNKNOWN
```

### 5.3 合法转换

```text
CREATED → RUNNING
RUNNING → WAITING_USER | WAITING_APPROVAL | PAUSED | OUTCOME_UNKNOWN |
          SUCCEEDED | FAILED | CANCELLED
WAITING_USER → RUNNING | CANCELLED
WAITING_APPROVAL → RUNNING | CANCELLED
PAUSED → RUNNING | CANCELLED
OUTCOME_UNKNOWN → RUNNING | SUCCEEDED | FAILED
```

终态不得转回运行态。非法转换返回 409，并写入 Event。

## 6. 数据表

SQLite 使用 WAL、foreign keys、busy timeout。建议通过一个 `RunStore` 接口封装，方便以后替换为 Postgres。

### 6.1 runs

```text
id                    TEXT PRIMARY KEY
user_id               TEXT NOT NULL
conv_id               TEXT NOT NULL
client_request_id     TEXT NOT NULL
message_hash          TEXT NOT NULL
task_type             TEXT NOT NULL
plan_version          TEXT NOT NULL
status                TEXT NOT NULL
current_step          TEXT
context_json          TEXT NOT NULL
input_json            TEXT NOT NULL
result_json           TEXT
response_json         TEXT
error_code            TEXT
retryable             INTEGER NOT NULL DEFAULT 0
version               INTEGER NOT NULL DEFAULT 0
lease_owner           TEXT
lease_until           TEXT
created_at            TEXT NOT NULL
updated_at            TEXT NOT NULL
finished_at           TEXT

UNIQUE(user_id, conv_id, client_request_id)
```

### 6.2 run_steps

```text
id                    TEXT PRIMARY KEY
run_id                TEXT NOT NULL
step_key              TEXT NOT NULL
ordinal               INTEGER NOT NULL
tool_name             TEXT
status                TEXT NOT NULL
attempt               INTEGER NOT NULL DEFAULT 0
idempotency_key       TEXT
input_json            TEXT
output_json           TEXT
evidence_json         TEXT
error_code            TEXT
started_at            TEXT
finished_at           TEXT

UNIQUE(run_id, step_key)
```

### 6.3 run_approvals

```text
id                    TEXT PRIMARY KEY
run_id                TEXT NOT NULL
step_key              TEXT NOT NULL
risk_summary          TEXT NOT NULL
decision              TEXT NOT NULL       # pending/approved/rejected
decided_by            TEXT
comment               TEXT
created_at            TEXT NOT NULL
decided_at            TEXT
```

### 6.4 run_events

追加式事件日志，用于审计和 trace：

```text
seq                   INTEGER PRIMARY KEY AUTOINCREMENT
run_id                TEXT NOT NULL
event_type            TEXT NOT NULL
step_key              TEXT
payload_json          TEXT NOT NULL
created_at            TEXT NOT NULL
```

事件包括 `RUN_CREATED`、`STEP_STARTED`、`TOOL_ATTEMPTED`、`APPROVAL_REQUESTED`、`APPROVAL_DECIDED`、`CHECKPOINT_COMMITTED`、`RUN_RESUMED`、`RUN_CANCELLED`、`RECONCILED`。

## 7. API 合同

现有 AccessFilter 只把少数路径视为普通用户可以访问。必须更新路由判断，使普通用户可访问自己的 `/runs` 资源，但仍不能访问其他人的 Run。管理员可以只读查看任意 Run。

### 7.1 创建任务

```http
POST /runs
```

请求：

```json
{
  "message": "订单 #12345 这个月好像被扣了两次款",
  "user_id": "u1",
  "conv_id": "c1",
  "client_request_id": "msg-001",
  "task_type": "duplicate_charge_refund"
}
```

响应：HTTP 202

```json
{
  "run_id": "uuid",
  "status": "CREATED",
  "current_step": "COLLECT_CONTEXT",
  "poll_after_ms": 500
}
```

行为：

- 鉴权开启时，服务端 Token 身份覆盖 `user_id`。
- 同一用户、会话、`client_request_id` 和相同消息返回原 Run。
- 同一 ID 对应不同消息返回 409。
- 创建 Run 后持久化，再调度后台执行。

### 7.2 查询任务

```http
GET /runs/{run_id}
```

返回 Run 状态、当前 Step、全部 Step、approval、可用结果和脱敏 Event。非所有者普通用户返回 404，避免暴露资源存在性。

### 7.3 恢复/补充输入

```http
POST /runs/{run_id}/resume
```

请求：

```json
{
  "message": "订单号是 #12345，支付金额 99 元",
  "expected_version": 3
}
```

响应：HTTP 202。

行为：

- 只有 `WAITING_USER`、`PAUSED`、`OUTCOME_UNKNOWN` 或可继续的 `RUNNING` 状态允许恢复。
- `expected_version` 不匹配返回 409。
- 补充输入经过现有意图实体提取和 `merge_state()`，但不能自动扩大工具权限。

### 7.4 审批

```http
POST /runs/{run_id}/approval
```

请求：

```json
{
  "step_key": "SUBMIT_REFUND",
  "decision": "approve",
  "comment": "金额与订单一致",
  "expected_version": 4
}
```

规则：

- v1 的退款提交必须经过审批。
- 只有任务所有者或管理员可以审批；生产后续应限制为有退款权限的角色。
- `reject` 后任务进入安全的失败/升级路径，不执行退款。
- 审批记录不可覆盖，重复决定返回 409。

### 7.5 取消

```http
POST /runs/{run_id}/cancel
```

行为：

- `WAITING_USER`、`WAITING_APPROVAL`、`PAUSED` 可立即取消。
- `RUNNING` 设置取消标记，Step 在安全检查点停止。
- 如果写工具已经进入 `RUNNING` 或 `OUTCOME_UNKNOWN`，不能宣称已取消外部操作，必须先对账。

## 8. v1 固定任务 DAG

Plan 由代码模板生成并持久化。LLM 只能处理受限分类、建议文案或抽取，不能创建新 Step 或调用模板外工具。

```text
COLLECT_CONTEXT
  → LOAD_ORDER
  → LOAD_CHARGES
  → EVALUATE_DUPLICATE
      ├─ 不疑似重复 → BUILD_NO_REFUND_ANSWER
      └─ 疑似重复 → SEARCH_REFUND_POLICY
                     → PREPARE_REFUND_PROPOSAL
                     → APPROVAL_GATE
                     → SUBMIT_REFUND
                     → POLL_REFUND_STATUS
                     → BUILD_FINAL_ANSWER
```

### 8.1 补充信息分支

如果缺少订单号、金额或支付时间：

- `COLLECT_CONTEXT` 进入 `WAITING_USER`。
- 返回明确的缺失字段。
- 用户补充后从当前 Step 恢复，不重新创建 Run。

### 8.2 不重复扣款分支

如果扣款记录不支持重复扣款：

- 不检索退款执行工具。
- 不创建审批。
- 返回解释、证据和后续建议。

### 8.3 工具白名单

只允许：

```text
get_order
get_charges
evaluate_duplicate
search_knowledge_base
submit_refund
get_refund_status
```

`get_order`、`get_charges`、`search_knowledge_base`、`get_refund_status` 为只读；`submit_refund` 为写工具，要求审批和业务幂等键。

## 9. 工具体系

### 9.1 RuntimeToolSpec

至少包含：

```text
name
description
input_schema
handler
read_only
side_effect
requires_approval
retry_safe
timeout_s
idempotency_scope
risk_level
```

现有 `AgentToolSpec` 保持兼容；不要通过无约束默认值把新写工具静默变成可执行工具。

### 9.2 幂等键

写操作幂等键：

```text
sha256(
  user_id + conv_id + run_id + step_key +
  tool_name + canonical_json(input)
)
```

必须：

- 在执行写工具前持久化到 Step。
- 以同一值作为沙箱退款服务的 `operation_id`。
- Step 重试时复用相同键。
- 日志中保存哈希，不暴露敏感明文。

### 9.3 风险策略

至少定义：

- `READ_ONLY`：可自动执行。
- `LOW_RISK_WRITE`：v1 也不自动执行，预留审批接口。
- `HIGH_RISK_WRITE`：必须审批。
- `BLOCKED`：角色、租户或参数不允许。

用户、租户、订单归属由服务端上下文注入，不能由模型参数决定。

## 10. 沙箱账单/退款系统

新增独立 SQLite 数据库，例如 `data/runtime/sandbox_billing.db`。

表：

```text
orders(order_id, user_id, amount_cents, status, created_at)
charges(charge_id, order_id, user_id, amount_cents, paid_at, channel)
refunds(operation_id PRIMARY KEY, order_id, user_id, amount_cents,
        reason, status, created_at, updated_at)
```

接口语义：

```text
get_order(order_id, user_id)
get_charges(order_id, user_id)
submit_refund(order_id, user_id, amount_cents, reason, operation_id)
get_refund_status(operation_id, user_id)
```

必须保证：

- `operation_id` 唯一。
- 相同 `operation_id` 重复提交返回相同退款记录。
- 参数不同但重复使用同一 `operation_id` 返回冲突。
- 查询时校验 `user_id` 和订单归属。
- `submit_refund` 的状态可以经历 `submitted → processing → succeeded/failed`。

仅供测试的故障注入：

```text
FAIL_BEFORE_EFFECT
FAIL_AFTER_EFFECT_BEFORE_RESPONSE
FAIL_AFTER_RESPONSE_BEFORE_CHECKPOINT
DELAYED_SUCCESS
```

生产代码路径不得读取用户可控的故障注入参数。

## 11. TaskEngine

### 11.1 执行循环

```text
claim run lease
→ load run + steps
→ validate transition/version
→ choose next READY step
→ mark RUNNING and emit event
→ execute step
→ validate output
→ persist step result + checkpoint
→ advance to next step
→ repeat until WAITING_* / terminal
```

### 11.2 租约

- `lease_owner` 使用实例 ID + 随机令牌。
- `lease_until` 默认 30 秒，可续租。
- 每次状态提交验证 owner 和 version，防止过期 runner 写入。
- 同一 Run 同一时刻只允许一个执行者。
- 调度器内存中的 task 去重只是优化，不能替代数据库租约。

### 11.3 恢复

启动时扫描非终态 Run：

- 只读 Step 的过期 `RUNNING`：回到 `READY`，允许重试。
- 写 Step 的过期 `RUNNING`：标记 `OUTCOME_UNKNOWN`，先调用 `get_refund_status(operation_id)` 对账。
- 远端已成功：Step → `SUCCEEDED`，继续。
- 远端未执行：Step → `READY`，按审批状态继续或重试。
- 远端状态不明：Run → `PAUSED` / `OUTCOME_UNKNOWN`，需要人工核验。

### 11.4 完成与会话提交

- 所有必需 Step 成功后，生成最终回答。
- 通过现有会话锁读取最新上下文。
- 使用任务创建时的 `client_request_id` 或专用 `run_turn_id` 提交一次。
- 在 RunStore 记录 `transcript_committed=true`。
- 若回答已提交但 Run 结果未写回，恢复时先检查 transcript committed，避免重复写入。
- `/chat` 行为保持不变。

## 12. 可观测性

### 12.1 Event

每个 Step 至少记录：输入摘要、工具、幂等键、尝试次数、耗时、状态、错误码、证据 ID 和 checkpoint 时间。

### 12.2 Prometheus

建议指标：

```text
echomind_runs_total{task_type,status}
echomind_run_step_duration_seconds{step,status}
echomind_run_resume_total{reason}
echomind_run_outcome_unknown_total{task_type}
echomind_run_approval_total{decision}
echomind_run_duplicate_prevented_total{tool}
```

标签不得包含用户正文、Token 或原始订单号。

### 12.3 Trace

`GET /runs/{run_id}` 返回脱敏 Step 和 Event，足以回答：

- 停在哪一步。
- 为什么等待。
- 哪个工具失败。
- 是否已执行外部副作用。
- 是否正在对账。
- 最终回答是否已写入会话。

## 13. 测试计划

### 13.1 单元测试

- 合法/非法状态转换。
- Run/Step 版本冲突。
- 同一请求 ID 的创建幂等和消息冲突。
- 幂等键稳定性与参数变化。
- 同一 operation_id 的退款重复提交。
- 审批只能决定一次。
- 取消后不会启动新的写操作。
- 过期 lease 不能提交。

### 13.2 集成测试

1. 完整成功路径。
2. 缺少订单号 → WAITING_USER → 补充后恢复。
3. 审批拒绝 → 不执行退款。
4. 审批通过 → 提交退款 → 查询最终状态。
5. 相同 client_request_id 重试 → 只有一个 Run、一次外部写。
6. 两个并发 resume → 只有一个 runner 获得 lease。
7. 进程模拟重启 → 从 Checkpoint 继续。
8. 写工具成功后、本地 Checkpoint 前失败 → `OUTCOME_UNKNOWN`。
9. `OUTCOME_UNKNOWN` 对账发现远端成功 → 不重复退款。
10. 对账发现远端未执行 → 重新提交时复用同一 operation_id。
11. 知识库超时 → 不进入自动退款，保留失败原因。
12. 非重复扣款 → 不产生退款工具调用。
13. 普通用户不能读取/审批他人 Run。
14. 管理员可以查看但不能绕过 Run 状态非法转换。
15. `cancel` 与 `resume` 并发 → 最终状态可解释。

### 13.3 评测用例

建立 `refund-v1.json`，至少包含：

- 正常退款。
- 金额不一致。
- 时间不在政策范围。
- 订单不属于当前用户。
- 无重复扣款。
- 知识库失败。
- 审批拒绝。
- 写操作超时但远端成功。
- 写操作超时且远端未执行。
- 用户要求取消。

每个 case 记录：

```text
初始用户输入
期望最终状态
期望关键 Step
期望工具
期望不调用工具
期望审批
期望退款记录数量
期望回答中的关键事实
```

### 13.4 回归门槛

必须运行：

```text
python -m pytest -q
```

新增 runtime 测试不得依赖真实模型 Key、真实 Redis、真实 Chroma 或外网。现有测试若需要环境，按仓库文档执行。

## 14. PR 拆分

### PR 1：Runtime 数据模型与 SQLite Store

内容：

- `runtime/models.py`
- `runtime/state_machine.py`
- `runtime/store.py`
- migration/bootstrap
- 单元测试

验收：

- 创建、读取、版本更新、lease、状态转换和事件记录通过测试。
- 非法状态转换和旧 owner 写入被拒绝。

### PR 2：Runs API 与鉴权

内容：

- `runtime/api.py`
- lifespan 初始化
- AccessFilter 放行规则
- POST/GET/cancel 基础接口

验收：

- 用户只能查看自己的 Run。
- 创建幂等和 409 冲突生效。
- 现有 `/chat` API 测试不回归。

### PR 3：沙箱账单系统与只读工具

内容：

- `runtime/billing.py`
- orders/charges seed 数据和测试夹具
- get_order/get_charges
- ownership 检查

验收：

- 跨用户订单不可见。
- 重复查询结果稳定。
- 无需真实外部服务。

### PR 4：固定任务 DAG 与成功路径

内容：

- `runtime/plans.py`
- `runtime/engine.py`
- Task runner、lease 和 Step checkpoint
- 不重复扣款分支
- 知识库只读调用适配

验收：

- 完整 happy path 可执行。
- 非重复扣款不调用退款工具。
- 缺少信息进入 WAITING_USER。

### PR 5：审批、退款写工具与恢复

内容：

- approval API
- submit_refund、get_refund_status
- 幂等键
- `OUTCOME_UNKNOWN` 对账
- resume/cancel

验收：

- 审批前绝不执行退款。
- 写成功但 checkpoint 丢失时不重复退款。
- 进程重启后可以恢复。

### PR 6：指标、评测集与文档

内容：

- Prometheus 指标
- `runtime-eval/refund-v1.json`
- 运行器或 pytest 参数化评测
- README、架构图和故障手册

验收：

- 所有评测用例有确定性预期。
- 可从 Run trace 定位失败阶段。
- 文档明确“沙箱”“非真实支付”“exactly-once 边界”。

## 15. Definition of Done

以下全部满足才算 v1 完成：

- 新功能默认关闭或只在显式 `/runs` 路径启用；`/chat` 回归通过。
- Run、Step、Approval、Event 持久化到 SQLite。
- 退款任务可等待用户输入和人工审批。
- 进程重启后可恢复。
- 写工具远端成功但本地未提交时，不重复执行。
- 所有写操作有幂等键和审批记录。
- 用户级资源隔离通过测试。
- 每次 Run 能查看脱敏 trace。
- 至少 15 个确定性 runtime 测试和 10 个评测 case。
- `python -m pytest -q` 通过。
- 不修改生成的 `openwiki/` 页面。

## 16. 交付给 CodeAgent 的执行顺序

严格按 PR 1 到 PR 6 顺序做，不要先接自由 Planner 或前端。

每个 PR 必须：

1. 先补失败测试。
2. 实现最小改动。
3. 跑新增测试。
4. 跑全量 Python 测试。
5. 更新对应文档。
6. 报告变更文件、测试命令、测试结果、未解决风险。

遇到以下情况立即停止并向用户说明，不要自行扩大范围：

- 需要真实支付或生产凭证。
- 必须修改现有 `/chat` 语义才能继续。
- SQLite 方案无法满足当前单进程目标之外的需求。
- 发现协议或权限边界与现有设计冲突。

## 17. 后续版本 Backlog

v1 完成后，再按顺序考虑：

1. 通用 PlanGraph 与 LLM 受限重规划。
2. 20+ 工具候选召回与选择评测。
3. Skill Progressive Disclosure 与版本管理。
4. Python Hybrid RAG、RRF、rerank 前后归因和文档 ACL。
5. OpenTelemetry 端到端 Span、成本与 Prompt 版本。
6. Java parity、前端 Run 时间线和多租户权限。

---

## 18. 实现补充说明（2026-11-18 hardening 记录）

以下实现决策超出原始计划文本，作为计划的正式补充（P2 裁决：保留实现、同步文档）：

1. **Step 转换补充**（第 5.3 节仅定义 Run 转换）：`RUNNING → READY`（只读 Step 过期恢复重试）、`FAILED → READY`（按 attempt 重试，幂等键防重复）、`OUTCOME_UNKNOWN → READY`（对账发现远端未执行/处理中，以同一幂等键重提）。
2. **runs 表补充两列**：`cancel_requested`（7.5 节 RUNNING 取消标记）、`transcript_committed`（11.4 节会话提交标记），通过 `PRAGMA user_version` 迁移机制加入。
3. **conv_id / client_request_id 改为可选**：与 `/chat` 行为对齐——未传 conv_id 时按 `uuid5(user_id + ":" + client_id)` 确定性推导，保证重试幂等。
4. **金额/支付时间一致性校验**（13.3 评测用例落地）：`EVALUATE_DUPLICATE` 以实际扣款记录为准——声称金额与记录不符 → `amount_mismatch` 失败；声称绝对日期与记录不符 → `date_mismatch` 失败；相对日期（昨天/今天）无法确定性比对，跳过。
5. **故障注入**：`FAIL_AFTER_RESPONSE_BEFORE_CHECKPOINT` 与 `FAIL_AFTER_EFFECT_BEFORE_RESPONSE` 从外部系统视角留下同一状态（记录已写入、客户端未确认），前者用于引擎侧"收到响应但检查点写入前崩溃"的模拟测试。
6. **RuntimeToolSpec（9.1 节）落地方式**：`runtime/tools.py` 注册表承载白名单与治理元数据（风险/审批/幂等），v1 DAG 由引擎执行器驱动；引擎内部判断类工具（evaluate_duplicate、search_knowledge_base）的 handler 置 None 并注明由执行器实现。
