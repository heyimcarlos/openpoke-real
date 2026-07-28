"""Persistence-safe contracts for suspended Agents SDK task Attempts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from .models import canonical_json


MAX_RUN_STATE_BYTES = 1_048_576


class RunStateCompatibility(BaseModel):
    """Versions that must match exactly before a suspended run may resume."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    codec_version: int = Field(ge=1)
    agents_sdk_version: str = Field(min_length=1, max_length=64)
    agent_definition_version: str = Field(min_length=1, max_length=128)


class TaskSuspension(BaseModel):
    """Write-once state emitted by an interrupted task executor."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    wait_key: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    compatibility: RunStateCompatibility
    model_requests_used: int = Field(ge=0)
    specialist_calls_used: int = Field(ge=0)
    state: dict[str, JsonValue]

    @model_validator(mode="after")
    def _bound_state(self) -> TaskSuspension:
        if len(canonical_json(self.state).encode("utf-8")) > MAX_RUN_STATE_BYTES:
            raise ValueError(
                f"serialized RunState exceeds {MAX_RUN_STATE_BYTES} bytes"
            )
        return self


@dataclass(frozen=True)
class TaskSuspensionRecord:
    task_id: UUID
    instance_id: UUID
    step_id: UUID
    wait_id: UUID
    attempt_count: int
    lease_generation: int
    compatibility: RunStateCompatibility
    model_requests_used: int
    specialist_calls_used: int
    state: dict[str, JsonValue]
    created_at: datetime


class RunStateIncompatible(RuntimeError):
    """Persisted state cannot be resumed by the current trusted runtime."""


def suspension_record_from_row(
    row: Mapping[str, Any],
) -> TaskSuspensionRecord:
    state = row["state_json"]
    return TaskSuspensionRecord(
        task_id=row["task_id"],
        instance_id=row["instance_id"],
        step_id=row["step_id"],
        wait_id=row["wait_id"],
        attempt_count=row["attempt_count"],
        lease_generation=row["lease_generation"],
        compatibility=RunStateCompatibility(
            codec_version=row["codec_version"],
            agents_sdk_version=row["agents_sdk_version"],
            agent_definition_version=row["agent_definition_version"],
        ),
        model_requests_used=row["model_requests_used"],
        specialist_calls_used=row["specialist_calls_used"],
        state=json.loads(state) if isinstance(state, str) else state,
        created_at=row["created_at"],
    )
