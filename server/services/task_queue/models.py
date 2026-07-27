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


class FailureCode(str, Enum):
    SYNTHETIC_RETRYABLE = "synthetic_retryable"
    SYNTHETIC_NON_RETRYABLE = "synthetic_non_retryable"
    LEASE_EXPIRED = "lease_expired"


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


class SubmitTask(BaseModel):
    """Trusted control-plane command; tenant and actor are deliberately absent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    idempotency_key: str = Field(min_length=1, max_length=128)
    origin_turn_id: str = Field(min_length=1, max_length=128)
    agent_name: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$",
    )
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
    input: dict[str, JsonValue]
    status: TaskStatus
    result: dict[str, JsonValue] | None
    attempt_count: int
    failure: FailureCode | None
    created_at: datetime


@dataclass(frozen=True)
class TaskLease:
    task_id: UUID
    tenant_id: str
    agent_name: str
    input: dict[str, JsonValue]
    attempt_count: int
    lease_generation: int
    worker_id: str
    expires_at: datetime
