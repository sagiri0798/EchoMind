"""Runtime Prometheus 指标（实施计划 12.2 节）。

标签只含任务类型、状态、步骤名等枚举值，不含用户正文、Token 或原始订单号。
指标注册在 prometheus_client 默认 REGISTRY，随 /metrics 端点一起暴露。
"""
from prometheus_client import Counter, Histogram

runs_total = Counter(
    "echomind_runs_total", "任务终态计数",
    ["task_type", "status"],
)

step_duration = Histogram(
    "echomind_run_step_duration_seconds", "Step 执行耗时（秒）",
    ["step", "status"], buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)

resume_total = Counter(
    "echomind_run_resume_total", "任务恢复次数",
    ["reason"],
)

outcome_unknown_total = Counter(
    "echomind_run_outcome_unknown_total", "写操作结果未知次数",
    ["task_type"],
)

approval_total = Counter(
    "echomind_run_approval_total", "审批决定次数",
    ["decision"],
)

duplicate_prevented_total = Counter(
    "echomind_run_duplicate_prevented_total", "幂等键拦截的重复写操作次数",
    ["tool"],
)


def record_run_terminal(task_type: str, status: str) -> None:
    runs_total.labels(task_type=task_type, status=status).inc()


def record_step(step: str, status: str, duration_s: float) -> None:
    step_duration.labels(step=step, status=status).observe(duration_s)


def record_resume(reason: str) -> None:
    resume_total.labels(reason=reason).inc()


def record_outcome_unknown(task_type: str) -> None:
    outcome_unknown_total.labels(task_type=task_type).inc()


def record_approval(decision: str) -> None:
    approval_total.labels(decision=decision).inc()


def record_duplicate_prevented(tool: str) -> None:
    duplicate_prevented_total.labels(tool=tool).inc()
