"""PostgreSQL-backed task acceptance and lifecycle authority."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from uuid import UUID

import asyncpg

from .models import (
    Principal,
    SubmitTask,
    TaskRecord,
    TaskStatus,
    canonical_json,
)


class IdempotencyConflict(ValueError):
    """An idempotency key was reused for different semantic work."""


class PostgresTaskLedger:
    """Keep accepted execution work durable behind one small interface."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def migrate(self) -> None:
        migration_path = (
            Path(__file__).resolve().parents[2]
            / "migrations"
            / "001_execution_tasks.sql"
        )
        async with self._pool.acquire() as connection:
            await connection.execute(migration_path.read_text(encoding="utf-8"))

    async def submit(
        self,
        principal: Principal,
        command: SubmitTask,
    ) -> TaskRecord:
        serialized_input = canonical_json(command.input)
        fingerprint = _request_fingerprint(principal, command, serialized_input)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                inserted = await connection.fetchrow(
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
                row = inserted or await connection.fetchrow(
                    """
                    SELECT *
                    FROM execution_tasks
                    WHERE tenant_id = $1 AND idempotency_key = $2
                    """,
                    principal.tenant_id,
                    command.idempotency_key,
                )

        if row is None:
            raise RuntimeError("task acceptance did not return a durable record")
        if row["request_fingerprint"] != fingerprint:
            raise IdempotencyConflict(
                "idempotency key already identifies different task input"
            )
        return _task_from_row(row)

    async def get(self, tenant_id: str, task_id: UUID) -> TaskRecord | None:
        row = await self._pool.fetchrow(
            """
            SELECT *
            FROM execution_tasks
            WHERE tenant_id = $1 AND task_id = $2
            """,
            tenant_id,
            task_id,
        )
        return _task_from_row(row) if row else None


def _request_fingerprint(
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
    canonical = canonical_json(semantic_request)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _task_from_row(row: asyncpg.Record) -> TaskRecord:
    return TaskRecord(
        task_id=row["task_id"],
        tenant_id=row["tenant_id"],
        actor_id=row["actor_id"],
        idempotency_key=row["idempotency_key"],
        origin_turn_id=row["origin_turn_id"],
        agent_name=row["agent_name"],
        input=json.loads(row["input"]),
        status=TaskStatus(row["status"]),
        result=json.loads(row["result"]) if row["result"] is not None else None,
        created_at=row["created_at"],
    )
