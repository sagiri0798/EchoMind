"""风险策略与审批判定（实施计划 9.3 节）。

用户、租户、订单归属由服务端上下文注入，不能由模型参数决定；
审批判定只看工具的声明风险与 DAG 位置，不信任模型。
"""


class RiskLevel:
    """四级风险策略（9.3 节）。"""

    READ_ONLY = "READ_ONLY"            # 可自动执行
    LOW_RISK_WRITE = "LOW_RISK_WRITE"  # v1 也不自动执行，预留审批接口
    HIGH_RISK_WRITE = "HIGH_RISK_WRITE"  # 必须审批
    BLOCKED = "BLOCKED"                # 角色、租户或参数不允许


def requires_approval(spec) -> bool:
    """工具是否需要人工审批：HIGH_RISK_WRITE 或显式声明。"""
    return spec.risk_level == RiskLevel.HIGH_RISK_WRITE or bool(spec.requires_approval)


def can_execute(spec, role: str = "task_engine") -> bool:
    """工具是否允许执行。BLOCKED 一律拒绝（9.3 节：模型、工具和用户都不能绕过权限）。"""
    return spec.risk_level != RiskLevel.BLOCKED
