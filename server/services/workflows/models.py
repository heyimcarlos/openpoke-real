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
    BLOCKED = "blocked"
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


class StepDependency(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    step_key: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    prerequisite_key: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_]*$",
    )


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
    entry_step: StepTemplate | None = None
    steps: tuple[StepTemplate, ...] | None = Field(
        default=None,
        max_length=32,
    )
    dependencies: tuple[StepDependency, ...] | None = Field(
        default=None,
        max_length=128,
    )

    @model_validator(mode="after")
    def _validate_closed_structure(self) -> WorkflowDefinition:
        names = [field.name for field in self.input_contract]
        if len(names) != len(set(names)):
            raise ValueError("workflow input field names must be unique")
        if (self.entry_step is None) == (self.steps is None):
            raise ValueError(
                "workflow must declare either one entry_step or a steps graph"
            )
        if self.entry_step is not None:
            if self.dependencies:
                raise ValueError(
                    "single-step workflows cannot declare dependencies"
                )
            return self
        if not self.steps:
            raise ValueError("workflow steps graph must not be empty")
        step_keys = [step.key for step in self.steps]
        if len(step_keys) != len(set(step_keys)):
            raise ValueError("workflow step keys must be unique")
        dependencies = self.dependencies or ()
        edges = {
            (edge.step_key, edge.prerequisite_key)
            for edge in dependencies
        }
        if len(edges) != len(dependencies):
            raise ValueError("workflow dependencies must be unique")
        known = set(step_keys)
        if any(
            edge.step_key not in known or edge.prerequisite_key not in known
            for edge in dependencies
        ):
            raise ValueError("workflow dependency references an unknown step")
        graph = {key: set() for key in step_keys}
        for edge in dependencies:
            graph[edge.step_key].add(edge.prerequisite_key)
        _validate_acyclic(graph)
        return self

    @property
    def step_templates(self) -> tuple[StepTemplate, ...]:
        if self.steps is not None:
            return self.steps
        if self.entry_step is None:
            raise RuntimeError("validated workflow has no steps")
        return (self.entry_step,)

    @property
    def content_hash(self) -> str:
        body = canonical_json(
            self.model_dump(mode="json", exclude_none=True)
        )
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
    position: int
    execution_task_id: UUID
    status: WorkflowStepStatus
    created_at: datetime


@dataclass(frozen=True)
class WorkflowStartResult:
    instance: WorkflowInstanceRecord
    steps: tuple[WorkflowStepRecord, ...]
    tasks: tuple[TaskRecord, ...]

    @property
    def step(self) -> WorkflowStepRecord:
        return self.steps[0]

    @property
    def task(self) -> TaskRecord:
        return self.tasks[0]


def _validate_acyclic(graph: dict[str, set[str]]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(step_key: str) -> None:
        if step_key in visiting:
            raise ValueError("workflow dependencies must be acyclic")
        if step_key in visited:
            return
        visiting.add(step_key)
        for prerequisite in graph[step_key]:
            visit(prerequisite)
        visiting.remove(step_key)
        visited.add(step_key)

    for step_key in graph:
        visit(step_key)
