"""Closed workflow definitions, typed commands, and durable records."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from ..task_queue import ExecutorKind, TaskRecord, canonical_json


class FieldType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"


class WorkflowInstanceStatus(str, Enum):
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class WorkflowStepStatus(str, Enum):
    RUNNABLE = "runnable"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class FieldContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_]*$")
    value_type: FieldType


class StepTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    key: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_]*$")
    agent_name: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[^\x00-\x1f\x7f]+$",
    )
    executor_kind: ExecutorKind


class WorkflowDefinition(BaseModel):
    """Published structure. Callers can select it but cannot alter its graph."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    key: str = Field(
        min_length=3,
        max_length=128,
        pattern=r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$",
    )
    version: int = Field(ge=1)
    input_contract: tuple[FieldContract, ...] = Field(max_length=32)
    entry_step: StepTemplate

    @model_validator(mode="after")
    def _unique_fields(self) -> WorkflowDefinition:
        names = [field.name for field in self.input_contract]
        if len(names) != len(set(names)):
            raise ValueError("workflow input field names must be unique")
        return self

    @property
    def content_hash(self) -> str:
        body = canonical_json(self.model_dump(mode="json"))
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    def validate_input(self, value: dict[str, JsonValue]) -> None:
        expected = {field.name for field in self.input_contract}
        if set(value) != expected:
            raise ValueError(
                f"workflow input fields must be exactly {sorted(expected)}"
            )
        for field in self.input_contract:
            item = value[field.name]
            valid = (
                field.value_type is FieldType.STRING
                and isinstance(item, str)
                or field.value_type is FieldType.INTEGER
                and isinstance(item, int)
                and not isinstance(item, bool)
                or field.value_type is FieldType.BOOLEAN
                and isinstance(item, bool)
            )
            if not valid:
                raise ValueError(
                    f"workflow input field {field.name!r} must be "
                    f"{field.value_type.value}"
                )


class WorkflowStartCommand(BaseModel):
    """Trusted command. Definition structure, tenant, and actor are absent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    idempotency_key: str = Field(min_length=1, max_length=128)
    origin_turn_id: str = Field(min_length=1, max_length=128)
    definition_key: str = Field(min_length=3, max_length=128)
    definition_version: int = Field(ge=1)
    input: dict[str, JsonValue]


@dataclass(frozen=True)
class WorkflowDefinitionRecord:
    key: str
    version: int
    definition: WorkflowDefinition
    content_hash: str
    published_at: datetime


@dataclass(frozen=True)
class WorkflowInstanceRecord:
    instance_id: UUID
    tenant_id: str
    actor_id: str
    definition_key: str
    definition_version: int
    definition_hash: str
    input: dict[str, JsonValue]
    status: WorkflowInstanceStatus
    created_at: datetime
    origin_thread_id: UUID | None
    origin_agent_run_id: UUID | None


@dataclass(frozen=True)
class WorkflowStepRecord:
    step_id: UUID
    instance_id: UUID
    key: str
    execution_task_id: UUID
    status: WorkflowStepStatus
    created_at: datetime


@dataclass(frozen=True)
class WorkflowStartResult:
    instance: WorkflowInstanceRecord
    step: WorkflowStepRecord
    task: TaskRecord
