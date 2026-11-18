"""Runtime 数据模型：Run、RunStep、RunApproval、RunEvent。

字段与 Task-Runtime-v1 实施计划第 6 节的数据表一一对应。
JSON 字段在模型内是解析后的 dict/list，由 RunStore 负责序列化落库；
时间戳统一使用无时区的 datetime.now().isoformat() 字符串，与现有模块一致。
"""
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


def new_id() -> str:
    """生成 Run/Step/Approval 的随机 ID。"""
    return str(uuid.uuid4())


def now_iso() -> str:
    """当前时间的 ISO 字符串，与现有模块的持久化格式一致。"""
    return datetime.now().isoformat()


class RunStatus(str, Enum):
    """Run 生命周期状态，值即数据库存储值（实施计划 5.1 节）。"""

    CREATED          = "CREATED"
    RUNNING          = "RUNNING"
    WAITING_USER     = "WAITING_USER"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    PAUSED           = "PAUSED"
    OUTCOME_UNKNOWN  = "OUTCOME_UNKNOWN"
    SUCCEEDED        = "SUCCEEDED"
    FAILED           = "FAILED"
    CANCELLED        = "CANCELLED"


class StepStatus(str, Enum):
    """Step 执行状态，值即数据库存储值（实施计划 5.2 节）。"""

    PENDING          = "PENDING"
    READY            = "READY"
    RUNNING          = "RUNNING"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    SUCCEEDED        = "SUCCEEDED"
    FAILED           = "FAILED"
    SKIPPED          = "SKIPPED"
    OUTCOME_UNKNOWN  = "OUTCOME_UNKNOWN"


class ApprovalDecision(str, Enum):
    """审批决定：pending/approved/rejected（实施计划 6.3 节）。"""

    PENDING  = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


@dataclass
class Run:
    """一次任务运行。runs 表的一行。

    transcript_committed 是 11.4 节要求的提交标记，在建议表结构上补充，
    避免后续版本再做迁移。
    """

    user_id:            str
    conv_id:            str
    client_request_id:  str
    message_hash:       str
    task_type:          str
    plan_version:       str
    status:             RunStatus        = RunStatus.CREATED
    current_step:       Optional[str]    = None
    context_json:       Optional[dict]   = None
    input_json:         Optional[dict]   = None
    result_json:        Optional[dict]   = None
    response_json:      Optional[dict]   = None
    error_code:         Optional[str]    = None
    retryable:          bool             = False
    version:            int              = 0
    lease_owner:        Optional[str]    = None
    lease_until:        Optional[str]    = None
    transcript_committed: bool           = False
    created_at:         str              = field(default_factory=now_iso)
    updated_at:         str              = field(default_factory=now_iso)
    finished_at:        Optional[str]    = None
    id:                 str              = field(default_factory=new_id)


@dataclass
class RunStep:
    """任务中的一个执行步骤。run_steps 表的一行。"""

    run_id:           str
    step_key:         str
    ordinal:          int
    tool_name:        Optional[str]   = None
    status:           StepStatus      = StepStatus.PENDING
    attempt:          int             = 0
    idempotency_key:  Optional[str]   = None
    input_json:       Optional[dict]  = None
    output_json:      Optional[dict]  = None
    evidence_json:    Optional[dict]  = None
    error_code:       Optional[str]   = None
    started_at:       Optional[str]   = None
    finished_at:      Optional[str]   = None
    id:               str             = field(default_factory=new_id)


@dataclass
class RunApproval:
    """一次人工审批记录。run_approvals 表的一行。"""

    run_id:        str
    step_key:      str
    risk_summary:  str
    decision:      ApprovalDecision = ApprovalDecision.PENDING
    decided_by:    Optional[str]    = None
    comment:       Optional[str]    = None
    created_at:    str              = field(default_factory=now_iso)
    decided_at:    Optional[str]    = None
    id:            str              = field(default_factory=new_id)


@dataclass
class RunEvent:
    """追加式审计事件。run_events 表的一行，seq 落库后才有值。"""

    run_id:       str
    event_type:   str
    payload_json: dict
    step_key:     Optional[str] = None
    created_at:   str           = field(default_factory=now_iso)
    seq:          Optional[int] = None
