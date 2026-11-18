"""Run/Step 状态机：合法状态转换与终态判断。

转换表与 Task-Runtime-v1 实施计划第 5.3 节一致。与计划相比的补充：
- Step 的 RUNNING → READY 用于只读 Step 过期后的恢复重试（11.3 节）；
- Step 的 FAILED → READY 允许按 attempt 重试（9.2 节幂等键保证写操作不重复）。
"""
from runtime.models import RunStatus, StepStatus

RUN_TRANSITIONS: dict[RunStatus, frozenset[RunStatus]] = {
    RunStatus.CREATED:          frozenset({RunStatus.RUNNING}),
    RunStatus.RUNNING:          frozenset({
        RunStatus.WAITING_USER, RunStatus.WAITING_APPROVAL, RunStatus.PAUSED,
        RunStatus.OUTCOME_UNKNOWN, RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED,
    }),
    RunStatus.WAITING_USER:     frozenset({RunStatus.RUNNING, RunStatus.CANCELLED}),
    RunStatus.WAITING_APPROVAL: frozenset({RunStatus.RUNNING, RunStatus.CANCELLED}),
    RunStatus.PAUSED:           frozenset({RunStatus.RUNNING, RunStatus.CANCELLED}),
    RunStatus.OUTCOME_UNKNOWN:  frozenset({RunStatus.RUNNING, RunStatus.SUCCEEDED, RunStatus.FAILED}),
    RunStatus.SUCCEEDED:        frozenset(),
    RunStatus.FAILED:           frozenset(),
    RunStatus.CANCELLED:        frozenset(),
}

STEP_TRANSITIONS: dict[StepStatus, frozenset[StepStatus]] = {
    StepStatus.PENDING:          frozenset({StepStatus.READY, StepStatus.SKIPPED}),
    StepStatus.READY:            frozenset({StepStatus.RUNNING, StepStatus.SKIPPED}),
    StepStatus.RUNNING:          frozenset({
        StepStatus.SUCCEEDED, StepStatus.FAILED, StepStatus.WAITING_APPROVAL,
        StepStatus.OUTCOME_UNKNOWN, StepStatus.READY,
    }),
    StepStatus.WAITING_APPROVAL: frozenset({StepStatus.RUNNING, StepStatus.SKIPPED, StepStatus.FAILED}),
    StepStatus.SUCCEEDED:        frozenset(),
    StepStatus.FAILED:           frozenset({StepStatus.READY}),
    StepStatus.SKIPPED:          frozenset(),
    StepStatus.OUTCOME_UNKNOWN:  frozenset({StepStatus.RUNNING, StepStatus.SUCCEEDED, StepStatus.FAILED}),
}

RUN_TERMINAL = frozenset({RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED})
STEP_TERMINAL = frozenset({StepStatus.SUCCEEDED, StepStatus.SKIPPED})


def can_transition_run(current: RunStatus, next_status: RunStatus) -> bool:
    """Run 的 current → next_status 是否在合法转换表内。"""
    return next_status in RUN_TRANSITIONS.get(current, frozenset())


def can_transition_step(current: StepStatus, next_status: StepStatus) -> bool:
    """Step 的 current → next_status 是否在合法转换表内。"""
    return next_status in STEP_TRANSITIONS.get(current, frozenset())


def is_terminal_run(status: RunStatus) -> bool:
    """终态不得转回运行态（实施计划 5.3 节）。"""
    return status in RUN_TERMINAL


def is_terminal_step(status: StepStatus) -> bool:
    return status in STEP_TERMINAL
