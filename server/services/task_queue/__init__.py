"""Durable, tenant-aware execution task ledger."""

from .auth import InvalidToken, JwtPrincipalVerifier
from .acceptance import (
    AdmissionRejected,
    IdempotencyConflict,
    TaskAdmission,
)
from .ledger import (
    PostgresTaskLedger,
    StaleLease,
    TaskResultConflict,
)
from .models import (
    ExecutorKind,
    FailureCode,
    Principal,
    SubmitTask,
    TaskFailure,
    TaskLease,
    TaskRecord,
    TaskStatus,
    canonical_json,
)
from .outbox import (
    OutboxFailureCode,
    OutboxPublishError,
    PostgresWakeOutbox,
    RelayOutcome,
    WakeEvent,
    WakeEventLease,
    WakeOutboxRelay,
    append_task_wake,
)
from .broker import (
    BoundedTaskWorkerWakeHandler,
    RabbitMQWakeBroker,
    RabbitMQWakePublisher,
)
from .execution import ExecutionFailure, ExecutionSuspended
from .service import MissingScope, TaskService
from .suspension import (
    RunStateCompatibility,
    RunStateIncompatible,
    TaskSuspension,
    TaskSuspensionRecord,
    suspension_record_from_row,
)

__all__ = [
    "AdmissionRejected",
    "BoundedTaskWorkerWakeHandler",
    "ExecutorKind",
    "ExecutionFailure",
    "ExecutionSuspended",
    "FailureCode",
    "IdempotencyConflict",
    "InvalidToken",
    "JwtPrincipalVerifier",
    "MissingScope",
    "OutboxFailureCode",
    "OutboxPublishError",
    "PostgresTaskLedger",
    "PostgresWakeOutbox",
    "RabbitMQWakeBroker",
    "RabbitMQWakePublisher",
    "Principal",
    "SubmitTask",
    "StaleLease",
    "TaskFailure",
    "TaskAdmission",
    "TaskLease",
    "TaskRecord",
    "TaskResultConflict",
    "TaskService",
    "TaskStatus",
    "RelayOutcome",
    "RunStateCompatibility",
    "RunStateIncompatible",
    "WakeEvent",
    "WakeEventLease",
    "WakeOutboxRelay",
    "TaskSuspension",
    "TaskSuspensionRecord",
    "append_task_wake",
    "canonical_json",
    "suspension_record_from_row",
]
