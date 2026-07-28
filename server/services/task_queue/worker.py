"""One bounded claim, execution, and fenced completion cycle."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from uuid import UUID

from .execution import (
    ExecutionFailure,
    ExecutionSuspended,
    ExecutorRegistry,
    UnknownExecutor,
)
from .ledger import PostgresTaskLedger, StaleLease
from .models import (
    ExecutorKind,
    FailureCode,
    TaskFailure,
    TaskLease,
    TaskRecord,
    TaskStatus,
)


class WorkerOutcomeStatus(str, Enum):
    IDLE = "idle"
    COMPLETED = "completed"
    RETRIED = "retried"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"
    STALE = "stale"
    PROJECTION_FAILED = "projection_failed"
    SUSPENDED = "suspended"


@dataclass(frozen=True)
class WorkerOutcome:
    status: WorkerOutcomeStatus
    task_id: UUID | None = None
    attempt_count: int | None = None
    failure: FailureCode | None = None


ResultSink = Callable[[TaskRecord], Awaitable[None]]


class TaskWorker:
    """Hide claim, execution, fencing, and projection behind ``run_once``."""

    def __init__(
        self,
        ledger: PostgresTaskLedger,
        executors: ExecutorRegistry,
        *,
        worker_id: str,
        lease_duration: timedelta = timedelta(seconds=120),
        execution_timeout_seconds: float = 90,
        projection_timeout_seconds: float = 90,
        result_sink: ResultSink | None = None,
    ) -> None:
        if execution_timeout_seconds <= 0 or projection_timeout_seconds <= 0:
            raise ValueError("worker timeouts must be positive")
        self._ledger = ledger
        self._executors = executors
        self._worker_id = worker_id
        self._lease_duration = lease_duration
        self._execution_timeout_seconds = execution_timeout_seconds
        self._projection_timeout_seconds = projection_timeout_seconds
        self._result_sink = result_sink

    async def run_once(
        self,
        *,
        executor_kind: ExecutorKind | None = None,
    ) -> WorkerOutcome:
        lease = await self._ledger.claim(
            self._worker_id,
            self._lease_duration,
            executor_kind=executor_kind,
        )
        if lease is None:
            return WorkerOutcome(status=WorkerOutcomeStatus.IDLE)

        try:
            executor = self._executors.resolve(lease.executor_kind)
            result = await asyncio.wait_for(
                executor.execute(lease),
                timeout=self._execution_timeout_seconds,
            )
        except ExecutionFailure as exc:
            return await self._record_failure(lease, exc.failure)
        except ExecutionSuspended as exc:
            try:
                await self._ledger.suspend(lease, exc.suspension)
            except StaleLease:
                return self._stale_outcome(lease)
            return WorkerOutcome(
                status=WorkerOutcomeStatus.SUSPENDED,
                task_id=lease.task_id,
                attempt_count=lease.attempt_count,
            )
        except UnknownExecutor:
            return await self._record_failure(
                lease,
                TaskFailure(code=FailureCode.UNKNOWN_EXECUTOR),
            )
        except TimeoutError:
            return await self._record_failure(
                lease,
                TaskFailure(code=FailureCode.EXECUTION_TIMEOUT),
            )
        except Exception:
            return await self._record_failure(
                lease,
                TaskFailure(code=FailureCode.AGENT_NON_RETRYABLE),
            )

        try:
            completed = await self._ledger.complete(lease, result)
        except StaleLease:
            return self._stale_outcome(lease)

        if self._result_sink is not None:
            try:
                await asyncio.wait_for(
                    self._result_sink(completed),
                    timeout=self._projection_timeout_seconds,
                )
            except Exception:
                return WorkerOutcome(
                    status=WorkerOutcomeStatus.PROJECTION_FAILED,
                    task_id=lease.task_id,
                    attempt_count=lease.attempt_count,
                )

        return WorkerOutcome(
            status=WorkerOutcomeStatus.COMPLETED,
            task_id=lease.task_id,
            attempt_count=lease.attempt_count,
        )

    async def _record_failure(
        self,
        lease: TaskLease,
        failure: TaskFailure,
    ) -> WorkerOutcome:
        try:
            record = await self._ledger.fail(lease, failure)
        except StaleLease:
            return self._stale_outcome(lease)

        match record.status:
            case TaskStatus.QUEUED:
                status = WorkerOutcomeStatus.RETRIED
            case TaskStatus.CANCELLED:
                status = WorkerOutcomeStatus.CANCELLED
            case _:
                status = WorkerOutcomeStatus.DEAD_LETTERED
        return WorkerOutcome(
            status=status,
            task_id=lease.task_id,
            attempt_count=lease.attempt_count,
            failure=failure.code,
        )

    @staticmethod
    def _stale_outcome(lease: TaskLease) -> WorkerOutcome:
        return WorkerOutcome(
            status=WorkerOutcomeStatus.STALE,
            task_id=lease.task_id,
            attempt_count=lease.attempt_count,
        )
