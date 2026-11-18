# Task-Runtime-v1 Post-Merge Review 问题清单

审查范围：`git diff 8c84443...HEAD`（6 个 commit：50e2863、d7bf80b、7716d63、32fbdf4、89e41a1、2e0f49e）
审查日期：2026-11-18　规格：`docs/Task-Runtime-v1-实施计划.md`

## 优先级说明

- **P0 规格不符/正确性**：与实施计划矛盾或可能造成业务错误，必须修
- **P1 标准/质量**：编码标准与可维护性问题
- **P2 范围蔓延**：实现超出规格，需裁决（升级文档 or 回退）

## P0 规格不符与正确性

| # | 问题 | 规格依据 |
|---|------|----------|
| S1 | 取消与写工具无对账：SUBMIT_REFUND 已成功但用户此刻 cancel，安全检查点无条件转 CANCELLED——退款已发生却无对账、无会话提交 | 7.5「写工具已进入 RUNNING 或 OUTCOME_UNKNOWN，不能宣称已取消外部操作，必须先对账」 |
| S2 | 恢复路径从不检查 transcript_committed：commit 前崩溃会重复提交会话 | 11.4「恢复时先检查 transcript committed，避免重复写入」 |
| S3 | resume 补充输入用引擎自带正则，未走现有意图实体提取和 merge_state() | 7.3 |
| S4 | 脱敏不完整：仅 Event payload 过 redact()，Step 的 input/evidence（含扣款明细、user_id）原样返回 | 12.3 / DoD「脱敏 Step 和 Event」 |
| S5 | 非法状态转换被拒绝时不写 Event | 5.3「非法转换返回 409，并写入 Event」 |
| S6 | CHECKPOINT_COMMITTED 事件从未发出；Event 缺输入摘要/耗时/证据 ID | 6.4 事件清单 / 12.1 |
| S7 | RuntimeToolSpec 完全未实现（name/description/input_schema/handler/read_only/side_effect/requires_approval/retry_safe/timeout_s/idempotency_scope/risk_level）；9.3 四级风险策略未定义，审批硬编码在 submit_refund 上 | 9.1 / 9.3 |
| S8 | 故障注入缺 FAIL_AFTER_RESPONSE_BEFORE_CHECKPOINT | 10 |
| S9 | API resume 不记录 echomind_run_resume_total{reason=user_input} | 12.2 |
| S10 | billing 状态只升不降，「failed」终态无任何路径可达 | 10「submitted → processing → succeeded/failed」 |
| S11 | 测试缺口：13.2 #5（同 client_request_id 一次外部写）、#6（两个并发 resume 抢租约）、#15（cancel/resume 并发）无测试；API reject 分支无测试；BillingUnavailable（FAIL_BEFORE_EFFECT）无端到端用例，会落成 engine_crash 而非业务错误码 | 13.2 |
| S12 | 评测「金额不一致」「时间不在政策范围」两个 case 的预期被降级为「v1 不做校验照常退款」——规格未授权跳过校验。需裁决：实现校验 or 显式写入规格 backlog | 13.3 |

## P1 标准与质量

| # | 问题 | 依据 |
|---|------|------|
| A1 | README 配置表缺 `ECHOMIND_RUNTIME_DB`、`ECHOMIND_BILLING_DB` | README §配置说明 |
| A2 | store.py 8 个写方法重复「BEGIN IMMEDIATE + SELECT + ROLLBACK + VersionConflict」样板；`_insert_run` 调 `_run_to_row` 三次 | Duplicated Code |
| A3 | `event_type+event_payload` 成对出现在 10+ 签名，update_run_status 11 个参数 | Data Clumps |
| A4 | store.py、billing.py 的 `logger` 定义后从未使用 | 死代码 |
| A5 | step_key 裸字符串散落 plans/engine/api/recovery/tests 五处，与状态用 Enum 不一致 | Primitive Obsession |
| A6 | 审批拒绝双实现：api.py reject（不 SKIP 后续 Step）与 engine reject（SKIP 三个 Step）文案相同、行为不同 | Duplicated Code / Shotgun Surgery |
| A7 | 写工具名单三处重复：recovery.WRITE_TOOLS、plans 注释、engine 知识 | Duplicated Code |
| A8 | test_runtime_metrics.py 用 prometheus 私有字段 `_value.get()` | 脆弱测试 |
| A9 | engine._rebuild_context 的 if/elif step_key 级联 | Repeated Switches |

## P2 范围蔓延（待裁决）

| # | 问题 | 备注 |
|---|------|------|
| B1 | Step 转换表自行扩展 RUNNING→READY、FAILED→READY、OUTCOME_UNKNOWN→READY | 文件注释已自认；服务于 11.3 恢复语义 |
| B2 | runs 表新增 cancel_requested、transcript_committed 两列（6.1 未列） | 服务 7.5 / 11.4，已自认 |
| B3 | conv_id/client_request_id 从必填改为可选自动生成 | 与 /chat 行为对齐 |

以上 P2 建议：在实施计划文档中补记这些补充决策（文档同步），不改代码。

## 已完成的修复（本次审查前）

- 既有失败根因：tests 内 `read_text()` 缺 `encoding="utf-8"`（Windows GBK），已修于 hardening 分支；全量 228 通过、0 失败。
- CI workflow 已起草于 hardening 分支（待提交）。
