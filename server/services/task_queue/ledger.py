"""PostgreSQL-backed task acceptance and lifecycle authority."""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import asyncpg
from pydantic import JsonValue

from .models import (
    FailureCode,
    Principal,
    SubmitTask,
    TaskFailure,
    TaskLease,
    TaskRecord,
    TaskStatus,
    canonical_json,
)

_CLAIM_CAPACITY_LOCK_ID = 5_716_553_685_489_545
_MIGRATION_LOCK_ID = 5_716_553_685_489_546
_TENANT_ADMISSION_LOCK_NAMESPACE = 1_331_862_839


class IdempotencyConflict(ValueError):
    """An idempotency key was reused for different semantic work."""


class StaleLease(RuntimeError):
    """A worker no longer holds completion authority for a task."""


class AdmissionRejected(RuntimeError):
    """A tenant has reached its configured outstanding-task limit."""

    def __init__(self, retry_after_seconds: int) -> None:
        super().__init__("tenant task backlog is full; retry later")
        self.retry_after_seconds = retry_after_seconds


class PostgresTaskLedger:
    """Keep accepted execution work durable behind one small interface."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        global_active_limit: int = 8,
        tenant_active_limit: int = 2,
        tenant_outstanding_limit: int = 50,
        max_attempts: int = 3,
        admission_retry_after_seconds: int = 5,
    ) -> None:
        if (
            global_active_limit < 1
            or tenant_active_limit < 1
            or tenant_outstanding_limit < 1
            or max_attempts < 1
        ):
            raise ValueError("task limits must be positive")
        if admission_retry_after_seconds < 1:
            raise ValueError("admission retry guidance must be positive")
        self._pool = pool
        self._global_active_limit = global_active_limit
        self._tenant_active_limit = tenant_active_limit
        self._tenant_outstanding_limit = tenant_outstanding_limit
        self._max_attempts = max_attempts
        self._admission_retry_after_seconds = admission_retry_after_seconds

    async def migrate(self) -> None:
        migrations_path = (
            Path(__file__).resolve().parents[2]
            / "migrations"
        )
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock($1)",
                    _MIGRATION_LOCK_ID,
                )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS openpoke_schema_migrations (
                        version TEXT PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL
                            DEFAULT clock_timestamp()
                    )
                    """
                )
                applied = {
                    row["version"]
                    for row in await connection.fetch(
                        "SELECT version FROM openpoke_schema_migrations"
                    )
                }
                for migration_path in sorted(migrations_path.glob("*.sql")):
                    if migration_path.name in applied:
                        continue
                    await connection.execute(
                        migration_path.read_text(encoding="utf-8")
                    )
                    await connection.execute(
                        """
                        INSERT INTO openpoke_schema_migrations (version)
                        VALUES ($1)
                        """,
                        migration_path.name,
                    )

    async def submit(
        self,
        principal: Principal,
        command: SubmitTask,
    ) -> TaskRecord:
        serialized_input = canonical_json(command.input)
        fingerprint = _request_fingerprint(principal, command, serialized_input)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    SELECT pg_advisory_xact_lock(
                        $1,
                        hashtext($2)
                    )
                    """,
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
                    return _task_from_row(existing)

                outstanding_count = await connection.fetchval(
                    """
                    SELECT count(*)
                    FROM execution_tasks
                    WHERE tenant_id = $1
                      AND status IN ('queued', 'running')
                    """,
                    principal.tenant_id,
                )
                if outstanding_count >= self._tenant_outstanding_limit:
                    raise AdmissionRejected(self._admission_retry_after_seconds)

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

        if row is None:
            raise RuntimeError("task acceptance did not return a durable record")
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

    async def cancel(
        self,
        tenant_id: str,
        task_id: UUID,
    ) -> TaskRecord | None:
        row = await self._pool.fetchrow(
            """
            UPDATE execution_tasks
            SET status = 'cancelled'
            WHERE tenant_id = $1
              AND task_id = $2
              AND status = 'queued'
            RETURNING *
            """,
            tenant_id,
            task_id,
        )
        return _task_from_row(row) if row else None

    async def claim(
        self,
        worker_id: str,
        lease_duration: timedelta,
    ) -> TaskLease | None:
        """Claim one task, serializing admission to keep capacity limits exact."""

        if not worker_id or len(worker_id) > 128:
            raise ValueError("worker_id must contain 1 to 128 characters")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")

        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock($1)",
                    _CLAIM_CAPACITY_LOCK_ID,
                )
                await connection.execute(
                    """
                    UPDATE execution_tasks
                    SET status = 'dead_lettered',
                        failure_code = 'lease_expired',
                        lease_owner = NULL,
                        lease_expires_at = NULL
                    WHERE status = 'running'
                      AND lease_expires_at <= clock_timestamp()
                      AND attempt_count >= $1
                    """,
                    self._max_attempts,
                )
                row = await connection.fetchrow(
                    """
                    WITH active_by_tenant AS (
                        SELECT tenant_id, count(*) AS active_count
                        FROM execution_tasks
                        WHERE status = 'running'
                          AND lease_expires_at > clock_timestamp()
                        GROUP BY tenant_id
                    ),
                    global_capacity AS (
                        SELECT count(*) AS active_count
                        FROM execution_tasks
                        WHERE status = 'running'
                          AND lease_expires_at > clock_timestamp()
                    ),
                    candidate AS (
                        SELECT task.task_id
                        FROM execution_tasks AS task
                        LEFT JOIN active_by_tenant AS tenant
                          ON tenant.tenant_id = task.tenant_id
                        CROSS JOIN global_capacity AS global
                        WHERE global.active_count < $3
                          AND COALESCE(tenant.active_count, 0) < $4
                          AND (
                              (
                                  task.status = 'queued'
                                  AND task.attempt_count < $5
                              )
                              OR (
                                  task.status = 'running'
                                  AND task.lease_expires_at <= clock_timestamp()
                                  AND task.attempt_count < $5
                              )
                          )
                        ORDER BY COALESCE(tenant.active_count, 0),
                                 task.created_at,
                                 task.task_id
                        FOR UPDATE OF task SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE execution_tasks AS task
                    SET status = 'running',
                        attempt_count = task.attempt_count + 1,
                        lease_generation = task.lease_generation + 1,
                        lease_owner = $1,
                        lease_expires_at = clock_timestamp() + $2::interval
                    FROM candidate
                    WHERE task.task_id = candidate.task_id
                    RETURNING task.*
                    """,
                    worker_id,
                    lease_duration,
                    self._global_active_limit,
                    self._tenant_active_limit,
                    self._max_attempts,
                )
        return _lease_from_row(row) if row else None

    async def complete(
        self,
        lease: TaskLease,
        result: dict[str, JsonValue],
    ) -> TaskRecord:
        serialized_result = canonical_json(result)
        row = await self._pool.fetchrow(
            """
            UPDATE execution_tasks
            SET status = 'completed',
                result = $4::jsonb,
                failure_code = NULL,
                lease_owner = NULL,
                lease_expires_at = NULL
            WHERE task_id = $1
              AND status = 'running'
              AND lease_owner = $2
              AND lease_generation = $3
              AND lease_expires_at > clock_timestamp()
            RETURNING *
            """,
            lease.task_id,
            lease.worker_id,
            lease.lease_generation,
            serialized_result,
        )
        if row is None:
            raise StaleLease("task lease is expired or superseded")
        return _task_from_row(row)

    async def fail(
        self,
        lease: TaskLease,
        failure: TaskFailure,
    ) -> TaskRecord:
        row = await self._pool.fetchrow(
            """
            UPDATE execution_tasks
            SET status = CASE
                    WHEN $5 AND attempt_count < $6
                    THEN 'queued'
                    ELSE 'dead_lettered'
                END,
                failure_code = $4,
                lease_owner = NULL,
                lease_expires_at = NULL
            WHERE task_id = $1
              AND status = 'running'
              AND lease_owner = $2
              AND lease_generation = $3
              AND lease_expires_at > clock_timestamp()
            RETURNING *
            """,
            lease.task_id,
            lease.worker_id,
            lease.lease_generation,
            failure.code.value,
            failure.retryable,
            self._max_attempts,
        )
        if row is None:
            raise StaleLease("task lease is expired or superseded")
        return _task_from_row(row)


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
        attempt_count=row.get("attempt_count", 0),
        failure=(
            FailureCode(row["failure_code"])
            if row.get("failure_code") is not None
            else None
        ),
        created_at=row["created_at"],
    )


def _lease_from_row(row: asyncpg.Record) -> TaskLease:
    return TaskLease(
        task_id=row["task_id"],
        tenant_id=row["tenant_id"],
        agent_name=row["agent_name"],
        input=json.loads(row["input"]),
        attempt_count=row["attempt_count"],
        lease_generation=row["lease_generation"],
        worker_id=row["lease_owner"],
        expires_at=row["lease_expires_at"],
    )
