"""v1 固定退款任务 DAG（实施计划第 8 节）。

Plan 由代码模板生成并持久化到 run_steps；LLM 只能处理受限分类、建议文案
或抽取，不能创建新 Step 或调用模板外工具。
"""
from enum import Enum

from runtime.models import RunStep, StepStatus

TASK_TYPE = "duplicate_charge_refund"
PLAN_VERSION = "v1"


class StepKey(str, Enum):
    """DAG 步骤键的唯一来源（str 枚举：与持久化的字符串值直接可比较）。"""

    COLLECT_CONTEXT = "COLLECT_CONTEXT"
    LOAD_ORDER = "LOAD_ORDER"
    LOAD_CHARGES = "LOAD_CHARGES"
    EVALUATE_DUPLICATE = "EVALUATE_DUPLICATE"
    SEARCH_REFUND_POLICY = "SEARCH_REFUND_POLICY"
    PREPARE_REFUND_PROPOSAL = "PREPARE_REFUND_PROPOSAL"
    APPROVAL_GATE = "APPROVAL_GATE"
    SUBMIT_REFUND = "SUBMIT_REFUND"
    POLL_REFUND_STATUS = "POLL_REFUND_STATUS"
    BUILD_FINAL_ANSWER = "BUILD_FINAL_ANSWER"
    BUILD_NO_REFUND_ANSWER = "BUILD_NO_REFUND_ANSWER"


# 重复扣款分支（按 DAG 顺序）；不重复分支走 BUILD_NO_REFUND_ANSWER
DUP_BRANCH = [
    StepKey.SEARCH_REFUND_POLICY,
    StepKey.PREPARE_REFUND_PROPOSAL,
    StepKey.APPROVAL_GATE,
    StepKey.SUBMIT_REFUND,
    StepKey.POLL_REFUND_STATUS,
    StepKey.BUILD_FINAL_ANSWER,
]

# 工具白名单（8.3 节）：只读工具自动执行；submit_refund 是写工具，要求审批与幂等键
TOOL_BY_STEP = {
    StepKey.LOAD_ORDER:           "get_order",
    StepKey.LOAD_CHARGES:         "get_charges",
    StepKey.EVALUATE_DUPLICATE:   "evaluate_duplicate",
    StepKey.SEARCH_REFUND_POLICY: "search_knowledge_base",
    StepKey.SUBMIT_REFUND:        "submit_refund",
    StepKey.POLL_REFUND_STATUS:   "get_refund_status",
}

# 首批可执行的线性前缀；其余 Step 等待 EVALUATE_DUPLICATE 分支激活
_INITIAL_READY = {StepKey.COLLECT_CONTEXT, StepKey.LOAD_ORDER, StepKey.LOAD_CHARGES,
                  StepKey.EVALUATE_DUPLICATE}

STEP_KEYS = [
    StepKey.COLLECT_CONTEXT,
    StepKey.LOAD_ORDER,
    StepKey.LOAD_CHARGES,
    StepKey.EVALUATE_DUPLICATE,
    *DUP_BRANCH,
    StepKey.BUILD_NO_REFUND_ANSWER,
]


def plan_for(task_type: str) -> list[RunStep]:
    """生成固定退款 DAG 的 Step 列表（含工具映射与初始状态）。"""
    if task_type != TASK_TYPE:
        raise ValueError(f"未知任务类型: {task_type}（v1 仅支持 {TASK_TYPE}）")
    return [
        RunStep(
            run_id="",  # create_steps 时统一填充
            step_key=key.value,
            ordinal=index,
            tool_name=TOOL_BY_STEP.get(key),
            status=StepStatus.READY if key in _INITIAL_READY else StepStatus.PENDING,
        )
        for index, key in enumerate(STEP_KEYS)
    ]
