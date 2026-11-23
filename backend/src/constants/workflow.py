from enum import StrEnum


class Role(StrEnum):
    OPERATOR = 'operator'
    REVIEWER = 'reviewer'
    VIEWER = 'viewer'


class Status(StrEnum):
    QUEUED = 'queued'
    RUNNING = 'running'
    COMPLETE = 'complete'
    FAILED = 'failed'


class HandoverState(StrEnum):
    PENDING = 'pending'
    DISPATCHED = 'dispatched'
    RECEIVED = 'received'


HANDOVER_LABELS = {
    HandoverState.PENDING: '待交接',
    HandoverState.DISPATCHED: '已派发',
    HandoverState.RECEIVED: '已回执',
}
