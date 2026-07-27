"""Internal control-plane boundary for tenant-owned execution tasks."""

from __future__ import annotations

from uuid import UUID

from .ledger import PostgresTaskLedger
from .models import Principal, SubmitTask, TaskRecord


class MissingScope(PermissionError):
    """A verified principal lacks permission for an operation."""


class TaskService:
    """Apply authorization before calling the tenant-aware task ledger."""

    def __init__(self, ledger: PostgresTaskLedger) -> None:
        self._ledger = ledger

    async def submit(
        self,
        principal: Principal,
        command: SubmitTask,
    ) -> TaskRecord:
        _require_scope(principal, "tasks:create")
        return await self._ledger.submit(principal, command)

    async def get(
        self,
        principal: Principal,
        task_id: UUID,
    ) -> TaskRecord | None:
        _require_scope(principal, "tasks:read")
        return await self._ledger.get(principal.tenant_id, task_id)

    async def cancel(
        self,
        principal: Principal,
        task_id: UUID,
    ) -> TaskRecord | None:
        _require_scope(principal, "tasks:cancel")
        return await self._ledger.cancel(principal.tenant_id, task_id)


def _require_scope(principal: Principal, required: str) -> None:
    if required not in principal.scopes:
        raise MissingScope(f"missing required scope: {required}")
