"""Allowlisted execution adapters for claimed tasks."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from enum import Enum
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from .models import ExecutorKind, FailureCode, TaskFailure, TaskLease


class Executor(Protocol):
    async def execute(self, lease: TaskLease) -> dict[str, JsonValue]:
        """Execute one claimed task and return its typed result."""


class UnknownExecutor(LookupError):
    """A claimed task names no trusted executor."""


class ExecutionFailure(RuntimeError):
    """An executor produced a typed, persistence-safe failure."""

    def __init__(self, failure: TaskFailure) -> None:
        super().__init__(failure.code.value)
        self.failure = failure


class ExecutorRegistry:
    """Resolve only executors registered by trusted worker configuration."""

    def __init__(self, executors: Mapping[ExecutorKind, Executor]) -> None:
        self._executors = dict(executors)

    def resolve(self, kind: ExecutorKind | str) -> Executor:
        try:
            trusted_kind = ExecutorKind(kind)
            return self._executors[trusted_kind]
        except (ValueError, KeyError):
            raise UnknownExecutor("executor is not allowlisted") from None


class SyntheticMode(str, Enum):
    SUCCESS = "success"
    FAIL_ONCE = "fail_once"
    FAIL_ALWAYS = "fail_always"
    NON_RETRYABLE = "non_retryable"


class _SyntheticInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: SyntheticMode
    duration_ms: int = Field(default=0, ge=0, le=5_000)


class SyntheticExecutor:
    """Run bounded, deterministic work without a model provider."""

    def __init__(
        self,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._sleep = sleep

    async def execute(self, lease: TaskLease) -> dict[str, JsonValue]:
        task_input = _SyntheticInput.model_validate(lease.input)
        await self._sleep(task_input.duration_ms / 1_000)

        if task_input.mode is SyntheticMode.NON_RETRYABLE:
            raise ExecutionFailure(
                TaskFailure(code=FailureCode.SYNTHETIC_NON_RETRYABLE)
            )
        if task_input.mode is SyntheticMode.FAIL_ALWAYS or (
            task_input.mode is SyntheticMode.FAIL_ONCE
            and lease.attempt_count == 1
        ):
            raise ExecutionFailure(
                TaskFailure(code=FailureCode.SYNTHETIC_RETRYABLE)
            )

        return {
            "agent_name": lease.agent_name,
            "response": "synthetic task completed",
        }
