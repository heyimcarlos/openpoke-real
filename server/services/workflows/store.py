"""PostgreSQL authority for workflow publication and typed starts."""

from __future__ import annotations

import hashlib
import json
from typing import Protocol
from uuid import UUID

import asyncpg

from ..task_queue import Principal, SubmitTask, TaskAdmission, canonical_json
from ..threads import AgentRunLease
from .models import (
    WorkflowDefinition,
    WorkflowDefinitionRecord,
    WorkflowInstanceRecord,
    WorkflowInstanceStatus,
    WorkflowStartCommand,
    WorkflowStartResult,
    WorkflowStepRecord,
    WorkflowStepStatus,
)


class _RunSubmissionAuthority(Protocol):
    async def lock_submission(
        self,
        connection: asyncpg.Connection,
        lease: AgentRunLease,
        principal: Principal,
    ) -> None: ...

    async def consume_submission(
        self,
        connection: asyncpg.Connection,
        run_id: UUID,
    ) -> None: ...


class DefinitionConflict(ValueError):
    """One immutable Definition identity has different content."""


class WorkflowIdempotencyConflict(ValueError):
    """One start key was reused for different semantic work."""


class DefinitionNotFound(LookupError):
    """A start command selected an unpublished Definition version."""


class PostgresWorkflowStore:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        tenant_outstanding_limit: int = 50,
        admission_retry_after_seconds: int = 5,
        run_authority: _RunSubmissionAuthority | None = None,
    ) -> None:
        self._pool = pool
        self._admission = TaskAdmission(
            tenant_outstanding_limit=tenant_outstanding_limit,
            retry_after_seconds=admission_retry_after_seconds,
        )
        self._run_authority = run_authority

    async def publish(
        self,
        definition: WorkflowDefinition,
    ) -> WorkflowDefinitionRecord:
        body = canonical_json(definition.model_dump(mode="json"))
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    INSERT INTO workflow_definitions (
                        definition_key,
                        definition_version,
                        body,
                        content_hash
                    )
                    VALUES ($1, $2, $3::jsonb, $4)
                    ON CONFLICT DO NOTHING
                    RETURNING *
                    """,
                    definition.key,
                    definition.version,
                    body,
                    definition.content_hash,
                )
                if row is None:
                    row = await connection.fetchrow(
                        """
                        SELECT *
                        FROM workflow_definitions
                        WHERE definition_key = $1
                          AND definition_version = $2
                        FOR UPDATE
                        """,
                        definition.key,
                        definition.version,
                    )
                    if (
                        row is None
                        or row["content_hash"] != definition.content_hash
                    ):
                        raise DefinitionConflict(
                            "Definition identity has different published content"
                        )
        return _definition_from_row(row)

    async def start(
        self,
        principal: Principal,
        command: WorkflowStartCommand,
        lease: AgentRunLease | None = None,
    ) -> WorkflowStartResult:
        serialized_input = canonical_json(command.input)
        fingerprint = _start_fingerprint(
            principal,
            command,
            serialized_input,
            lease,
        )
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                if lease is not None:
                    if self._run_authority is None:
                        raise RuntimeError(
                            "Agent Run submission authority is unavailable"
                        )
                    await self._run_authority.lock_submission(
                        connection,
                        lease,
                        principal,
                    )
                row = await connection.fetchrow(
                    """
                    INSERT INTO workflow_instances (
                        tenant_id,
                        actor_id,
                        idempotency_key,
                        request_fingerprint,
                        definition_key,
                        definition_version,
                        definition_hash,
                        input,
                        origin_thread_id,
                        origin_agent_run_id
                    )
                    SELECT $1, $2, $3, $4,
                           definition_key, definition_version, content_hash,
                           $7::jsonb, $8, $9
                    FROM workflow_definitions
                    WHERE definition_key = $5
                      AND definition_version = $6
                    ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
                    RETURNING *
                    """,
                    principal.tenant_id,
                    principal.actor_id,
                    command.idempotency_key,
                    fingerprint,
                    command.definition_key,
                    command.definition_version,
                    serialized_input,
                    lease.thread_id if lease else None,
                    lease.run_id if lease else None,
                )
                if row is None:
                    existing = await connection.fetchrow(
                        """
                        SELECT *
                        FROM workflow_instances
                        WHERE tenant_id = $1 AND idempotency_key = $2
                        FOR UPDATE
                        """,
                        principal.tenant_id,
                        command.idempotency_key,
                    )
                    if existing is not None:
                        if existing["request_fingerprint"] != fingerprint:
                            raise WorkflowIdempotencyConflict(
                                "idempotency key identifies a different Workflow start"
                            )
                        return await self._load_start(
                            connection,
                            existing["instance_id"],
                        )
                    raise DefinitionNotFound(
                        "Workflow Definition version is not published"
                    )

                definition_row = await connection.fetchrow(
                    """
                    SELECT *
                    FROM workflow_definitions
                    WHERE definition_key = $1 AND definition_version = $2
                    """,
                    command.definition_key,
                    command.definition_version,
                )
                definition_record = _definition_from_row(definition_row)
                definition_record.definition.validate_input(command.input)
                entry = definition_record.definition.entry_step
                task = await self._admission.accept(
                    connection,
                    principal,
                    SubmitTask(
                        idempotency_key=f"workflow:{row['instance_id']}:{entry.key}",
                        origin_turn_id=command.origin_turn_id,
                        agent_name=entry.agent_name,
                        executor_kind=entry.executor_kind,
                        input=command.input,
                    ),
                    origin_thread_id=lease.thread_id if lease else None,
                    origin_agent_run_id=lease.run_id if lease else None,
                )
                step = await connection.fetchrow(
                    """
                    INSERT INTO workflow_steps (
                        instance_id,
                        step_key,
                        execution_task_id
                    )
                    VALUES ($1, $2, $3)
                    RETURNING *
                    """,
                    row["instance_id"],
                    entry.key,
                    task.task_id,
                )
                await connection.execute(
                    """
                    INSERT INTO workflow_events (
                        instance_id,
                        sequence,
                        event_type,
                        payload
                    )
                    VALUES ($1, 1, 'workflow_started', $2::jsonb)
                    """,
                    row["instance_id"],
                    canonical_json(
                        {
                            "step_id": str(step["step_id"]),
                            "task_id": str(task.task_id),
                        }
                    ),
                )
                if lease is not None:
                    await self._run_authority.consume_submission(
                        connection,
                        lease.run_id,
                    )
        return WorkflowStartResult(
            instance=_instance_from_row(row),
            step=_step_from_row(step),
            task=task,
        )

    async def get(
        self,
        tenant_id: str,
        instance_id: UUID,
    ) -> WorkflowInstanceRecord | None:
        row = await self._pool.fetchrow(
            """
            SELECT *
            FROM workflow_instances
            WHERE tenant_id = $1 AND instance_id = $2
            """,
            tenant_id,
            instance_id,
        )
        return _instance_from_row(row) if row else None

    async def _load_start(
        self,
        connection: asyncpg.Connection,
        instance_id: UUID,
    ) -> WorkflowStartResult:
        row = await connection.fetchrow(
            "SELECT * FROM workflow_instances WHERE instance_id = $1",
            instance_id,
        )
        step = await connection.fetchrow(
            "SELECT * FROM workflow_steps WHERE instance_id = $1",
            instance_id,
        )
        return WorkflowStartResult(
            instance=_instance_from_row(row),
            step=_step_from_row(step),
            task=await self._admission.get(
                connection,
                step["execution_task_id"],
            ),
        )


def _start_fingerprint(
    principal: Principal,
    command: WorkflowStartCommand,
    serialized_input: str,
    lease: AgentRunLease | None,
) -> str:
    value = canonical_json(
        {
            "actor_id": principal.actor_id,
            "definition_key": command.definition_key,
            "definition_version": command.definition_version,
            "input_json": serialized_input,
            "origin_turn_id": command.origin_turn_id,
            "origin_thread_id": str(lease.thread_id) if lease else None,
            "origin_agent_run_id": str(lease.run_id) if lease else None,
        }
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _definition_from_row(row: asyncpg.Record) -> WorkflowDefinitionRecord:
    body = json.loads(row["body"])
    definition = WorkflowDefinition.model_validate(body)
    if definition.content_hash != row["content_hash"]:
        raise DefinitionConflict("persisted Definition failed digest verification")
    return WorkflowDefinitionRecord(
        key=row["definition_key"],
        version=row["definition_version"],
        definition=definition,
        content_hash=row["content_hash"],
        published_at=row["published_at"],
    )


def _instance_from_row(row: asyncpg.Record) -> WorkflowInstanceRecord:
    return WorkflowInstanceRecord(
        instance_id=row["instance_id"],
        tenant_id=row["tenant_id"],
        actor_id=row["actor_id"],
        definition_key=row["definition_key"],
        definition_version=row["definition_version"],
        definition_hash=row["definition_hash"],
        input=json.loads(row["input"]),
        status=WorkflowInstanceStatus(row["status"]),
        created_at=row["created_at"],
        origin_thread_id=row["origin_thread_id"],
        origin_agent_run_id=row["origin_agent_run_id"],
    )


def _step_from_row(row: asyncpg.Record) -> WorkflowStepRecord:
    return WorkflowStepRecord(
        step_id=row["step_id"],
        instance_id=row["instance_id"],
        key=row["step_key"],
        execution_task_id=row["execution_task_id"],
        status=WorkflowStepStatus(row["status"]),
        created_at=row["created_at"],
    )
