"""Connection-scoped task acceptance for atomic control-plane transactions."""

from __future__ import annotations

import hashlib
import json
from uuid import UUID

import asyncpg

from .models import (
    ExecutorKind,
    FailureCode,
    Principal,
    SubmitTask,
    TaskRecord,
    TaskStatus,
    canonical_json,
)


_TENANT_ADMISSION_LOCK_NAMESPACE = 1_331_862_839


class IdempotencyConflict(ValueError):
    """An idempotency key was reused for different semantic work."""


class AdmissionRejected(RuntimeError):
    """A tenant has reached its configured outstanding-task limit."""

    def __init__(self, retry_after_seconds: int) -> None:
        super().__init__("tenant task backlog is full; retry later")
        self.retry_after_seconds = retry_after_seconds


class TaskAdmission:
    """Accept one tenant-owned task inside the caller's transaction."""

    def __init__(
        self,
        *,
        tenant_outstanding_limit: int = 50,
        retry_after_seconds: int = 5,
    ) -> None:
        if tenant_outstanding_limit < 1 or retry_after_seconds < 1:
            raise ValueError("task admission limits must be positive")
        self._tenant_outstanding_limit = tenant_outstanding_limit
        self._retry_after_seconds = retry_after_seconds

    async def accept(
        self,
        connection: asyncpg.Connection,
        principal: Principal,
        command: SubmitTask,
        *,
        origin_thread_id: UUID | None = None,
        origin_agent_run_id: UUID | None = None,
    ) -> TaskRecord:
        serialized_input = canonical_json(command.input)
        fingerprint = task_request_fingerprint(
            principal,
            command,
            serialized_input,
        )
        await connection.execute(
            "SELECT pg_advisory_xact_lock($1, hashtext($2))",
            _TENANT_ADMISSION_LOCK_NAMESPACE,
            principal.tenant_id,
        )
        existing = await connection.fetchrow(
            """
            SELECT *
            FROM execution_tasks
            WHERE tenant_id = $1 AND idempotency_key = $2
            """,
            principal.tenant_id,
            command.idempotency_key,
        )
        if existing is not None:
            if existing["request_fingerprint"] != fingerprint:
                raise IdempotencyConflict(
                    "idempotency key already identifies different task input"
                )
            return task_from_row(existing)

        outstanding = await connection.fetchval(
            """
            SELECT count(*)
            FROM execution_tasks
            WHERE tenant_id = $1
              AND status IN ('queued', 'running')
            """,
            principal.tenant_id,
        )
        if outstanding >= self._tenant_outstanding_limit:
            raise AdmissionRejected(self._retry_after_seconds)

        columns = {
            row["attname"]
            for row in await connection.fetch(
                """
                SELECT attname
                FROM pg_attribute
                WHERE attrelid = 'execution_tasks'::regclass
                  AND attname IN (
                      'executor_kind',
                      'origin_thread_id',
                      'origin_agent_run_id'
                  )
                  AND NOT attisdropped
                """
            )
        }
        if "executor_kind" not in columns:
            if command.executor_kind is not ExecutorKind.AGENT:
                raise RuntimeError("executor policy migration has not been applied")
            if origin_thread_id is not None or origin_agent_run_id is not None:
                raise RuntimeError("Thread provenance migration has not been applied")
            row = await connection.fetchrow(
                """
                INSERT INTO execution_tasks (
                    tenant_id,
                    actor_id,
                    idempotency_key,
                    request_fingerprint,
                    origin_turn_id,
                    agent_name,
                    input
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
                RETURNING *
                """,
                principal.tenant_id,
                principal.actor_id,
                command.idempotency_key,
                fingerprint,
                command.origin_turn_id,
                command.agent_name,
                serialized_input,
            )
        elif "origin_thread_id" not in columns:
            if origin_thread_id is not None or origin_agent_run_id is not None:
                raise RuntimeError("Thread provenance migration has not been applied")
            row = await connection.fetchrow(
                """
                INSERT INTO execution_tasks (
                    tenant_id,
                    actor_id,
                    idempotency_key,
                    request_fingerprint,
                    origin_turn_id,
                    agent_name,
                    executor_kind,
                    input
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
                RETURNING *
                """,
                principal.tenant_id,
                principal.actor_id,
                command.idempotency_key,
                fingerprint,
                command.origin_turn_id,
                command.agent_name,
                command.executor_kind.value,
                serialized_input,
            )
        else:
            row = await connection.fetchrow(
                """
                INSERT INTO execution_tasks (
                    tenant_id,
                    actor_id,
                    idempotency_key,
                    request_fingerprint,
                    origin_turn_id,
                    agent_name,
                    executor_kind,
                    input,
                    origin_thread_id,
                    origin_agent_run_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10)
                ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
                RETURNING *
                """,
                principal.tenant_id,
                principal.actor_id,
                command.idempotency_key,
                fingerprint,
                command.origin_turn_id,
                command.agent_name,
                command.executor_kind.value,
                serialized_input,
                origin_thread_id,
                origin_agent_run_id,
            )
        if row is None:
            row = await connection.fetchrow(
                """
                SELECT *
                FROM execution_tasks
                WHERE tenant_id = $1 AND idempotency_key = $2
                """,
                principal.tenant_id,
                command.idempotency_key,
            )
            if row is None:
                raise RuntimeError("conflicting task acceptance record disappeared")
            if row["request_fingerprint"] != fingerprint:
                raise IdempotencyConflict(
                    "idempotency key already identifies different task input"
                )
        return task_from_row(row)

    async def get(
        self,
        connection: asyncpg.Connection,
        task_id: UUID,
    ) -> TaskRecord:
        row = await connection.fetchrow(
            "SELECT * FROM execution_tasks WHERE task_id = $1",
            task_id,
        )
        if row is None:
            raise RuntimeError("accepted workflow task disappeared")
        return task_from_row(row)


def task_request_fingerprint(
    principal: Principal,
    command: SubmitTask,
    serialized_input: str,
) -> str:
    semantic_request = {
        "actor_id": principal.actor_id,
        "agent_name": command.agent_name,
        "input_json": serialized_input,
        "origin_turn_id": command.origin_turn_id,
    }
    if command.executor_kind is not ExecutorKind.AGENT:
        semantic_request["executor_kind"] = command.executor_kind.value
    canonical = canonical_json(semantic_request)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def task_from_row(row: asyncpg.Record) -> TaskRecord:
    return TaskRecord(
        task_id=row["task_id"],
        tenant_id=row["tenant_id"],
        actor_id=row["actor_id"],
        idempotency_key=row["idempotency_key"],
        origin_turn_id=row["origin_turn_id"],
        agent_name=row["agent_name"],
        executor_kind=ExecutorKind(
            row.get("executor_kind", ExecutorKind.AGENT.value)
        ),
        input=json.loads(row["input"]),
        status=TaskStatus(row["status"]),
        result=json.loads(row["result"]) if row["result"] is not None else None,
        attempt_count=row.get("attempt_count", 0),
        failure=(
            FailureCode(row["failure_code"])
            if row.get("failure_code") is not None
            else None
        ),
        created_at=row["created_at"],
        origin_thread_id=row.get("origin_thread_id"),
        origin_agent_run_id=row.get("origin_agent_run_id"),
    )
