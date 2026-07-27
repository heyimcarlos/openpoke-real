"""Closed routing between existing delegation and reasoning Step execution."""

from __future__ import annotations

from typing import Protocol

from pydantic import JsonValue

from ...services.task_queue import (
    ExecutionFailure,
    FailureCode,
    TaskFailure,
    TaskLease,
)


BOUNDED_REASONING_AGENT_NAME = "bounded-reasoning-manager"


class _Executor(Protocol):
    async def execute(self, lease: TaskLease) -> dict[str, JsonValue]: ...


class WorkflowAwareAgentExecutor:
    """Select the trusted reasoning profile without changing delegation."""

    def __init__(
        self,
        *,
        independent: _Executor,
        bounded_reasoning: _Executor,
    ) -> None:
        self._independent = independent
        self._bounded_reasoning = bounded_reasoning

    async def execute(self, lease: TaskLease) -> dict[str, JsonValue]:
        if lease.agent_name != BOUNDED_REASONING_AGENT_NAME:
            return await self._independent.execute(lease)
        if lease.workflow_instance_id is None or lease.workflow_step_id is None:
            raise ExecutionFailure(TaskFailure(code=FailureCode.AGENT_NON_RETRYABLE))
        return await self._bounded_reasoning.execute(lease)
