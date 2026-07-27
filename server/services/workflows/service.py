"""Authorization boundaries for published Workflows."""

from __future__ import annotations

from uuid import UUID

from ..task_queue import Principal
from ..threads import AgentRunLease
from .models import (
    WorkflowDefinition,
    WorkflowDefinitionRecord,
    WorkflowInstanceRecord,
    WorkflowStartCommand,
    WorkflowStartResult,
)
from .store import PostgresWorkflowStore


class MissingWorkflowScope(PermissionError):
    pass


class WorkflowDefinitionRegistry:
    def __init__(self, store: PostgresWorkflowStore) -> None:
        self._store = store

    async def publish(
        self,
        principal: Principal,
        definition: WorkflowDefinition,
    ) -> WorkflowDefinitionRecord:
        _require_scope(principal, "workflows:publish")
        return await self._store.publish(definition)


class WorkflowService:
    def __init__(self, store: PostgresWorkflowStore) -> None:
        self._store = store

    async def start(
        self,
        principal: Principal,
        command: WorkflowStartCommand,
    ) -> WorkflowStartResult:
        _require_scope(principal, "workflows:start")
        return await self._store.start(principal, command)

    async def start_for_run(
        self,
        principal: Principal,
        command: WorkflowStartCommand,
        lease: AgentRunLease,
    ) -> WorkflowStartResult:
        _require_scope(principal, "workflows:start")
        return await self._store.start(principal, command, lease)

    async def get(
        self,
        principal: Principal,
        instance_id: UUID,
    ) -> WorkflowInstanceRecord | None:
        _require_scope(principal, "workflows:read")
        return await self._store.get(principal.tenant_id, instance_id)


def _require_scope(principal: Principal, scope: str) -> None:
    if scope not in principal.scopes:
        raise MissingWorkflowScope(f"missing required scope: {scope}")
