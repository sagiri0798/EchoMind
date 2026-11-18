"""Runtime 工具体系：RuntimeToolSpec 注册表（实施计划 9.1 节）与 DAG 白名单（8.3 节）。

与现有 AgentToolSpec 保持兼容思路，但独立定义：新写工具不能通过
无约束默认值静默变成可执行工具（9.1 节）。v1 中引擎执行器按固定 DAG
调用工具，spec 负责元数据治理（白名单/风险/审批/幂等），
handler 字段为只读/写工具提供真实实现，引擎内部判断类工具
（evaluate_duplicate、search_knowledge_base）由执行器实现，handler 置 None。
"""
from dataclasses import dataclass
from typing import Any, Callable, Optional

from runtime.billing import SandboxBillingClient
from runtime.policy import RiskLevel


@dataclass(frozen=True)
class RuntimeToolSpec:
    """9.1 节要求的字段：名称、描述、schema、handler 与治理元数据。"""

    name: str
    description: str
    input_schema: dict
    handler: Optional[Callable]
    read_only: bool = True
    side_effect: bool = False
    requires_approval: bool = False
    retry_safe: bool = True
    timeout_s: float = 30.0
    idempotency_scope: str = "none"  # none | run_step | business
    risk_level: str = RiskLevel.READ_ONLY


def build_runtime_tools(billing: SandboxBillingClient,
                        knowledge_handler: Optional[Callable] = None) -> dict:
    """v1 固定退款 DAG 的工具白名单（8.3 节）。

    只读工具自动执行；submit_refund 是唯一写工具：要求审批 + 业务幂等键。
    handler 的 ctx 由服务端注入（user_id），模型参数不能决定归属（9.3 节）。
    """
    async def _get_order(params, ctx):
        return await billing.get_order(params["order_id"], ctx["user_id"])

    async def _get_charges(params, ctx):
        return await billing.get_charges(params["order_id"], ctx["user_id"])

    async def _submit_refund(params, ctx):
        return await billing.submit_refund(
            order_id=params["order_id"], user_id=ctx["user_id"],
            amount_cents=params["amount_cents"], reason=params["reason"],
            operation_id=params["operation_id"])

    async def _get_refund_status(params, ctx):
        return await billing.get_refund_status(params["operation_id"], ctx["user_id"])

    return {
        "get_order": RuntimeToolSpec(
            name="get_order", description="查询订单（属主隔离）",
            input_schema={"type": "object",
                          "properties": {"order_id": {"type": "string"}},
                          "required": ["order_id"]},
            handler=_get_order),
        "get_charges": RuntimeToolSpec(
            name="get_charges", description="查询订单扣款记录（属主隔离）",
            input_schema={"type": "object",
                          "properties": {"order_id": {"type": "string"}},
                          "required": ["order_id"]},
            handler=_get_charges),
        "evaluate_duplicate": RuntimeToolSpec(
            name="evaluate_duplicate", description="判断疑似重复扣款（引擎内置规则）",
            input_schema={"type": "object", "properties": {}},
            handler=None),
        "search_knowledge_base": RuntimeToolSpec(
            name="search_knowledge_base", description="检索退款政策（只读知识库）",
            input_schema={"type": "object",
                          "properties": {"query": {"type": "string"}, "top_k": {"type": "integer"}},
                          "required": ["query"]},
            handler=knowledge_handler, timeout_s=10.0),
        "submit_refund": RuntimeToolSpec(
            name="submit_refund", description="提交退款申请（写工具，需审批）",
            input_schema={"type": "object",
                          "properties": {"order_id": {"type": "string"},
                                         "amount_cents": {"type": "integer"},
                                         "reason": {"type": "string"},
                                         "operation_id": {"type": "string"}},
                          "required": ["order_id", "amount_cents", "reason", "operation_id"]},
            handler=_submit_refund,
            read_only=False, side_effect=True, requires_approval=True, retry_safe=False,
            idempotency_scope="run_step", risk_level=RiskLevel.HIGH_RISK_WRITE),
        "get_refund_status": RuntimeToolSpec(
            name="get_refund_status", description="查询退款状态（只读对账）",
            input_schema={"type": "object",
                          "properties": {"operation_id": {"type": "string"}},
                          "required": ["operation_id"]},
            handler=_get_refund_status),
    }


def write_tool_names(registry: dict) -> set:
    """写工具名单的单一来源（供恢复扫描使用，11.3 节）。"""
    return {name for name, spec in registry.items() if spec.side_effect}
