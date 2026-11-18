"""v1 固定退款任务 DAG（实施计划第 8 节）。

Plan 由代码模板生成并持久化到 run_steps；LLM 只能处理受限分类、建议文案
或抽取，不能创建新 Step 或调用模板外工具。
"""
from runtime.models import RunStep, StepStatus

TASK_TYPE = "duplicate_charge_refund"
PLAN_VERSION = "v1"

# 重复扣款分支（按 DAG 顺序）；不重复分支走 BUILD_NO_REFUND_ANSWER
DUP_BRANCH = [
    "SEARCH_REFUND_POLICY",
    "PREPARE_REFUND_PROPOSAL",
    "APPROVAL_GATE",
    "SUBMIT_REFUND",
    "POLL_REFUND_STATUS",
    "BUILD_FINAL_ANSWER",
]

# 工具白名单（8.3 节）：只读工具自动执行；submit_refund 是写工具，PR5 接入审批与幂等键
TOOL_BY_STEP = {
    "LOAD_ORDER":           "get_order",
    "LOAD_CHARGES":         "get_charges",
    "EVALUATE_DUPLICATE":   "evaluate_duplicate",
    "SEARCH_REFUND_POLICY": "search_knowledge_base",
    "SUBMIT_REFUND":        "submit_refund",
    "POLL_REFUND_STATUS":   "get_refund_status",
}

# 首批可执行的线性前缀；其余 Step 等待 EVALUATE_DUPLICATE 分支激活
_INITIAL_READY = {"COLLECT_CONTEXT", "LOAD_ORDER", "LOAD_CHARGES", "EVALUATE_DUPLICATE"}

STEP_KEYS = [
    "COLLECT_CONTEXT",
    "LOAD_ORDER",
    "LOAD_CHARGES",
    "EVALUATE_DUPLICATE",
    *DUP_BRANCH,
    "BUILD_NO_REFUND_ANSWER",
]


def plan_for(task_type: str) -> list[RunStep]:
    """生成固定退款 DAG 的 Step 列表（含工具映射与初始状态）。"""
    if task_type != TASK_TYPE:
        raise ValueError(f"未知任务类型: {task_type}（v1 仅支持 {TASK_TYPE}）")
    return [
        RunStep(
            run_id="",  # create_steps 时统一填充
            step_key=key,
            ordinal=index,
            tool_name=TOOL_BY_STEP.get(key),
            status=StepStatus.READY if key in _INITIAL_READY else StepStatus.PENDING,
        )
        for index, key in enumerate(STEP_KEYS)
    ]
