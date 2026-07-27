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


class WorkflowWaitStatus(str, Enum):
    OPEN = "open"
    SATISFIED = "satisfied"
    CANCELLED = "cancelled"


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


class WaitTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    key: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_]*$")
    signal_key: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    input_contract: tuple[FieldContract, ...] = Field(max_length=32)

    @model_validator(mode="after")
    def _validate_contract(self) -> WaitTemplate:
        _validate_unique_fields(self.input_contract, "Signal")
        return self

    def validate_input(self, value: dict[str, JsonValue]) -> None:
        _validate_input(self.input_contract, value, "Signal")


class WaitPrerequisite(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    wait_key: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    prerequisite_step_key: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_]*$",
    )


class WaitRoute(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    wait_key: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    step_key: str = Field(
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
    waits: tuple[WaitTemplate, ...] | None = Field(default=None, max_length=32)
    wait_prerequisites: tuple[WaitPrerequisite, ...] | None = Field(
        default=None,
        max_length=128,
    )
    wait_routes: tuple[WaitRoute, ...] | None = Field(
        default=None,
        max_length=128,
    )

    @model_validator(mode="after")
    def _validate_closed_structure(self) -> WorkflowDefinition:
        _validate_unique_fields(self.input_contract, "Workflow")
        if (self.entry_step is None) == (self.steps is None):
            raise ValueError(
                "workflow must declare either one entry_step or a steps graph"
            )
        if self.entry_step is not None:
            if self.dependencies:
                raise ValueError(
                    "single-step workflows cannot declare dependencies"
                )
        if not self.step_templates:
            raise ValueError("workflow steps graph must not be empty")
        step_keys = [step.key for step in self.step_templates]
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
        waits = self.waits or ()
        wait_keys = [wait.key for wait in waits]
        if len(wait_keys) != len(set(wait_keys)):
            raise ValueError("workflow Wait keys must be unique")
        prerequisites = self.wait_prerequisites or ()
        routes = self.wait_routes or ()
        if (prerequisites or routes) and not waits:
            raise ValueError("workflow Wait relations require a Wait")
        known_waits = set(wait_keys)
        if any(
            item.wait_key not in known_waits
            or item.prerequisite_step_key not in known
            for item in prerequisites
        ):
            raise ValueError("Wait prerequisite references an unknown node")
        if any(
            item.wait_key not in known_waits or item.step_key not in known
            for item in routes
        ):
            raise ValueError("Wait route references an unknown node")
        if len(
            {(item.wait_key, item.prerequisite_step_key) for item in prerequisites}
        ) != len(prerequisites):
            raise ValueError("Wait prerequisites must be unique")
        if len({(item.wait_key, item.step_key) for item in routes}) != len(routes):
            raise ValueError("Wait routes must be unique")
        routed_waits = {item.wait_key for item in routes}
        if known_waits - routed_waits:
            raise ValueError("every Wait must have a predefined route")

        graph = {f"step:{key}": set() for key in step_keys}
        graph.update({f"wait:{key}": set() for key in wait_keys})
        for edge in dependencies:
            graph[f"step:{edge.step_key}"].add(
                f"step:{edge.prerequisite_key}"
            )
        for edge in prerequisites:
            graph[f"wait:{edge.wait_key}"].add(
                f"step:{edge.prerequisite_step_key}"
            )
        for edge in routes:
            graph[f"step:{edge.step_key}"].add(f"wait:{edge.wait_key}")
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
        _validate_input(self.input_contract, value, "Workflow")


class WorkflowSignalCommand(BaseModel):
    """Trusted command. Tenant, actor, and route are deliberately absent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    idempotency_key: str = Field(min_length=1, max_length=128)
    wait_id: UUID
    signal_key: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_]*$",
    )
    input: dict[str, JsonValue]

    @model_validator(mode="after")
    def _bound_serialized_input(self) -> WorkflowSignalCommand:
        if len(canonical_json(self.input).encode("utf-8")) > 16_384:
            raise ValueError("Signal input exceeds 16384 serialized bytes")
        return self


def _validate_unique_fields(
    contract: tuple[FieldContract, ...],
    owner: str,
) -> None:
    names = [field.name for field in contract]
    if len(names) != len(set(names)):
        raise ValueError(f"{owner} input field names must be unique")


def _validate_input(
    contract: tuple[FieldContract, ...],
    value: dict[str, JsonValue],
    owner: str,
) -> None:
    expected = {field.name for field in contract}
    if set(value) != expected:
        raise ValueError(
            f"{owner} input fields must be exactly {sorted(expected)}"
        )
    for field in contract:
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
                f"{owner} input field {field.name!r} must be "
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
class WorkflowWaitTarget:
    wait_id: UUID
    instance_id: UUID
    key: str
    signal_key: str


@dataclass(frozen=True)
class WorkflowWaitRecord:
    wait_id: UUID
    instance_id: UUID
    key: str
    signal_key: str
    status: WorkflowWaitStatus
    created_at: datetime
    satisfied_at: datetime | None
    satisfied_by_signal_id: UUID | None


@dataclass(frozen=True)
class WorkflowSignalResult:
    signal_id: UUID
    wait: WorkflowWaitRecord
    released_step_ids: tuple[UUID, ...]
    accepted_at: datetime


@dataclass(frozen=True)
class WorkflowStartResult:
    instance: WorkflowInstanceRecord
    steps: tuple[WorkflowStepRecord, ...]
    tasks: tuple[TaskRecord, ...]
    wait_targets: tuple[WorkflowWaitTarget, ...]
    waits: tuple[WorkflowWaitRecord, ...]

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
