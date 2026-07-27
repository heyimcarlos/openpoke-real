"""Typed task-ledger commands, identities, and records."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator


MAX_TASK_INPUT_BYTES = 16_384


class TaskStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"


class ExecutorKind(str, Enum):
    AGENT = "agent"
    SYNTHETIC = "synthetic"


class FailureCode(str, Enum):
    SYNTHETIC_RETRYABLE = "synthetic_retryable"
    SYNTHETIC_NON_RETRYABLE = "synthetic_non_retryable"
    LEASE_EXPIRED = "lease_expired"
    ATTEMPTS_EXHAUSTED = "attempts_exhausted"
    EXECUTION_TIMEOUT = "execution_timeout"
    AGENT_NON_RETRYABLE = "agent_non_retryable"
    UNKNOWN_EXECUTOR = "unknown_executor"


class TaskFailure(BaseModel):
    """Allowlisted failure data safe to persist and expose."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: FailureCode

    @property
    def retryable(self) -> bool:
        return self.code is FailureCode.SYNTHETIC_RETRYABLE


def canonical_json(value: JsonValue) -> str:
    """Serialize JSON exactly as validation and PostgreSQL submission expect."""

    _reject_postgres_null_character(value)
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _reject_postgres_null_character(value: JsonValue) -> None:
    if isinstance(value, str):
        if "\u0000" in value:
            raise ValueError("task input strings cannot contain a null character")
        return
    if isinstance(value, list):
        for item in value:
            _reject_postgres_null_character(item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _reject_postgres_null_character(key)
            _reject_postgres_null_character(item)


@dataclass(frozen=True)
class Principal:
    """Verified user identity passed in from the authentication boundary."""

    actor_id: str
    tenant_id: str
    scopes: frozenset[str]
    composio_user_id: str | None = None


class SubmitTask(BaseModel):
    """Trusted control-plane command; tenant and actor are deliberately absent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    idempotency_key: str = Field(min_length=1, max_length=128)
    origin_turn_id: str = Field(min_length=1, max_length=128)
    agent_name: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[^\x00-\x1f\x7f]+$",
    )
    executor_kind: ExecutorKind = ExecutorKind.AGENT
    input: dict[str, JsonValue]

    @model_validator(mode="after")
    def _bound_serialized_input(self) -> SubmitTask:
        serialized = canonical_json(self.input).encode("utf-8")
        if len(serialized) > MAX_TASK_INPUT_BYTES:
            raise ValueError(
                f"task input exceeds {MAX_TASK_INPUT_BYTES} serialized bytes"
            )
        return self


@dataclass(frozen=True)
class TaskRecord:
    task_id: UUID
    tenant_id: str
    actor_id: str
    idempotency_key: str
    origin_turn_id: str
    agent_name: str
    executor_kind: ExecutorKind
    input: dict[str, JsonValue]
    status: TaskStatus
    result: dict[str, JsonValue] | None
    attempt_count: int
    failure: FailureCode | None
    created_at: datetime
    origin_thread_id: UUID | None = None
    origin_agent_run_id: UUID | None = None


@dataclass(frozen=True)
class TaskLease:
    task_id: UUID
    tenant_id: str
    actor_id: str
    origin_turn_id: str
    agent_name: str
    executor_kind: ExecutorKind
    input: dict[str, JsonValue]
    attempt_count: int
    lease_generation: int
    worker_id: str
    expires_at: datetime
    origin_thread_id: UUID | None = None
    origin_agent_run_id: UUID | None = None
