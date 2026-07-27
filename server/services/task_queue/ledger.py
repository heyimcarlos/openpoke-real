"""PostgreSQL-backed task acceptance and lifecycle authority."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import asyncpg
from pydantic import JsonValue

from .acceptance import (
    TaskAdmission,
    task_from_row,
)
from .models import (
    ExecutorKind,
    Principal,
    SubmitTask,
    TaskFailure,
    TaskLease,
    TaskRecord,
    canonical_json,
)

_CLAIM_CAPACITY_LOCK_ID = 5_716_553_685_489_545
_MIGRATION_LOCK_ID = 5_716_553_685_489_546
class StaleLease(RuntimeError):
    """A worker no longer holds completion authority for a task."""


class TaskResultConflict(ValueError):
    """A completed task received a different result for the same Attempt."""


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
        self._max_attempts = max_attempts
        self._admission = TaskAdmission(
            tenant_outstanding_limit=tenant_outstanding_limit,
            retry_after_seconds=admission_retry_after_seconds,
        )

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
        await self._pool.expire_connections()

    async def submit(
        self,
        principal: Principal,
        command: SubmitTask,
    ) -> TaskRecord:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                return await self._admission.accept(
                    connection,
                    principal,
                    command,
                )

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
        return task_from_row(row) if row else None

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
        return task_from_row(row) if row else None

    async def claim(
        self,
        worker_id: str,
        lease_duration: timedelta,
        *,
        executor_kind: ExecutorKind | None = None,
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
                await _reconcile_terminal_tasks(
                    connection,
                    self._max_attempts,
                )
                while True:
                    candidate = await connection.fetchrow(
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
                        )
                        SELECT task.task_id
                        FROM execution_tasks AS task
                        LEFT JOIN active_by_tenant AS tenant
                          ON tenant.tenant_id = task.tenant_id
                        CROSS JOIN global_capacity AS global
                        WHERE global.active_count < $1
                          AND COALESCE(tenant.active_count, 0) < $2
                          AND (
                              $4::text IS NULL
                              OR COALESCE(
                                  to_jsonb(task)->>'executor_kind',
                                  'agent'
                              ) = $4
                          )
                          AND (
                              (
                                  task.status = 'queued'
                                  AND task.attempt_count < $3
                              )
                              OR (
                                  task.status = 'running'
                                  AND task.lease_expires_at
                                      <= clock_timestamp()
                                  AND task.attempt_count < $3
                              )
                          )
                        ORDER BY COALESCE(tenant.active_count, 0),
                                 task.created_at,
                                 task.task_id
                        LIMIT 1
                        """,
                        self._global_active_limit,
                        self._tenant_active_limit,
                        self._max_attempts,
                        executor_kind.value if executor_kind else None,
                    )
                    if candidate is None:
                        row = None
                        break
                    instance = await _lock_workflow_instance_for_task(
                        connection,
                        candidate["task_id"],
                    )
                    if instance is not None and instance["status"] != "active":
                        await _cancel_inactive_workflow_task(
                            connection,
                            candidate["task_id"],
                            instance["status"],
                        )
                        continue
                    row = await connection.fetchrow(
                        """
                        UPDATE execution_tasks
                        SET status = 'running',
                            attempt_count = attempt_count + 1,
                            lease_generation = lease_generation + 1,
                            lease_owner = $2,
                            lease_expires_at = (
                                clock_timestamp() + $3::interval
                            )
                        WHERE task_id = $1
                          AND attempt_count < $4
                          AND (
                              status = 'queued'
                              OR (
                                  status = 'running'
                                  AND lease_expires_at
                                      <= clock_timestamp()
                              )
                          )
                        RETURNING *
                        """,
                        candidate["task_id"],
                        worker_id,
                        lease_duration,
                        self._max_attempts,
                    )
                    if row is None:
                        continue
                    from ..workflows import record_workflow_task_claimed

                    await record_workflow_task_claimed(connection, row)
                    break
        return _lease_from_row(row) if row else None

    async def complete(
        self,
        lease: TaskLease,
        result: dict[str, JsonValue],
    ) -> TaskRecord:
        serialized_result = canonical_json(result)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await _lock_workflow_instance_for_task(
                    connection,
                    lease.task_id,
                )
                row = await connection.fetchrow(
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
                    existing = await connection.fetchrow(
                        """
                        SELECT *
                        FROM execution_tasks
                        WHERE task_id = $1
                        FOR UPDATE
                        """,
                        lease.task_id,
                    )
                    if (
                        existing is not None
                        and existing["status"] == "completed"
                        and existing["lease_generation"]
                        == lease.lease_generation
                    ):
                        accepted_result = canonical_json(
                            json.loads(existing["result"])
                        )
                        if accepted_result != serialized_result:
                            raise TaskResultConflict(
                                "task Attempt already accepted a different result"
                            )
                        return task_from_row(existing)
                    raise StaleLease("task lease is expired or superseded")

                from ..workflows import advance_workflow_for_task

                workflow_completed = await advance_workflow_for_task(
                    connection,
                    row,
                )
                if (
                    row.get("origin_thread_id") is not None
                    and workflow_completed in {None, True}
                ):
                    from ..threads.continuation import append_execution_result

                    await append_execution_result(connection, row)
        return task_from_row(row)

    async def fail(
        self,
        lease: TaskLease,
        failure: TaskFailure,
    ) -> TaskRecord:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await _lock_workflow_instance_for_task(
                    connection,
                    lease.task_id,
                )
                row = await connection.fetchrow(
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
                from ..workflows import record_workflow_task_failed

                await record_workflow_task_failed(connection, row)
                row = await connection.fetchrow(
                    """
                    SELECT *
                    FROM execution_tasks
                    WHERE task_id = $1
                    """,
                    lease.task_id,
                )
                if row is None:
                    raise RuntimeError("failed task disappeared")
                if row["status"] == "queued":
                    from .outbox import append_task_wake

                    await append_task_wake(
                        connection,
                        task_id=row["task_id"],
                        executor_kind=row.get(
                            "executor_kind",
                            ExecutorKind.AGENT.value,
                        ),
                        source_transition="retry",
                        source_generation=row["attempt_count"],
                    )
        return task_from_row(row)

    async def recover_expired(self, *, limit: int = 100) -> int:
        """Requeue recoverable expired leases and append fenced wakes."""

        if limit < 1:
            raise ValueError("recovery limit must be positive")
        recovered = 0
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                candidates = await connection.fetch(
                    """
                    SELECT task_id
                    FROM execution_tasks
                    WHERE status = 'running'
                      AND lease_expires_at <= clock_timestamp()
                      AND attempt_count < $1
                    ORDER BY lease_expires_at, task_id
                    LIMIT $2
                    """,
                    self._max_attempts,
                    limit,
                )
                from ..workflows import record_workflow_task_failed
                from .outbox import append_task_wake

                for candidate in candidates:
                    await _lock_workflow_instance_for_task(
                        connection,
                        candidate["task_id"],
                    )
                    row = await connection.fetchrow(
                        """
                        UPDATE execution_tasks
                        SET status = 'queued',
                            failure_code = 'lease_expired',
                            lease_owner = NULL,
                            lease_expires_at = NULL
                        WHERE task_id = $1
                          AND status = 'running'
                          AND lease_expires_at <= clock_timestamp()
                          AND attempt_count < $2
                        RETURNING *
                        """,
                        candidate["task_id"],
                        self._max_attempts,
                    )
                    if row is None:
                        continue
                    await record_workflow_task_failed(connection, row)
                    current = await connection.fetchrow(
                        "SELECT * FROM execution_tasks WHERE task_id = $1",
                        row["task_id"],
                    )
                    if current is not None and current["status"] == "queued":
                        await append_task_wake(
                            connection,
                            task_id=row["task_id"],
                            executor_kind=row["executor_kind"],
                            source_transition="lease_expired",
                            source_generation=row["lease_generation"],
                        )
                    recovered += 1
        return recovered


async def _lock_workflow_instance_for_task(
    connection: asyncpg.Connection,
    task_id: UUID,
) -> asyncpg.Record | None:
    if await connection.fetchval(
        "SELECT to_regclass('workflow_steps')"
    ) is None:
        return None
    instance_id = await connection.fetchval(
        """
        SELECT instance_id
        FROM workflow_steps
        WHERE execution_task_id = $1
        """,
        task_id,
    )
    if instance_id is None:
        return None
    return await connection.fetchrow(
        """
        SELECT *
        FROM workflow_instances
        WHERE instance_id = $1
        FOR UPDATE
        """,
        instance_id,
    )


async def _reconcile_terminal_tasks(
    connection: asyncpg.Connection,
    max_attempts: int,
) -> None:
    candidates = {
        row["task_id"]
        for row in await connection.fetch(
            """
            SELECT task_id
            FROM execution_tasks
            WHERE (
                    status = 'running'
                    AND lease_expires_at <= clock_timestamp()
                    AND attempt_count >= $1
                  )
               OR (
                    status = 'queued'
                    AND attempt_count >= $1
                  )
            """,
            max_attempts,
        )
    }
    if await connection.fetchval(
        "SELECT to_regclass('workflow_steps')"
    ) is not None:
        candidates.update(
            row["task_id"]
            for row in await connection.fetch(
                """
                SELECT task.task_id
                FROM execution_tasks AS task
                JOIN workflow_steps AS step
                  ON step.execution_task_id = task.task_id
                JOIN workflow_instances AS instance
                  ON instance.instance_id = step.instance_id
                WHERE instance.status = 'failed'
                  AND task.status = 'running'
                  AND task.lease_expires_at <= clock_timestamp()
                """
            )
        )
    from ..workflows import record_workflow_task_failed

    for task_id in sorted(candidates):
        instance = await _lock_workflow_instance_for_task(
            connection,
            task_id,
        )
        if instance is not None and instance["status"] == "failed":
            row = await connection.fetchrow(
                """
                UPDATE execution_tasks
                SET status = 'cancelled',
                    failure_code = CASE
                        WHEN status = 'running' THEN 'lease_expired'
                        ELSE 'attempts_exhausted'
                    END,
                    lease_owner = NULL,
                    lease_expires_at = NULL
                WHERE task_id = $1
                  AND (
                      status = 'queued'
                      OR (
                          status = 'running'
                          AND lease_expires_at <= clock_timestamp()
                      )
                  )
                RETURNING *
                """,
                task_id,
            )
        else:
            row = await connection.fetchrow(
                """
                UPDATE execution_tasks
                SET status = 'dead_lettered',
                    failure_code = CASE
                        WHEN status = 'running' THEN 'lease_expired'
                        ELSE 'attempts_exhausted'
                    END,
                    lease_owner = NULL,
                    lease_expires_at = NULL
                WHERE task_id = $1
                  AND (
                      (
                          status = 'running'
                          AND lease_expires_at <= clock_timestamp()
                          AND attempt_count >= $2
                      )
                      OR (
                          status = 'queued'
                          AND attempt_count >= $2
                      )
                  )
                RETURNING *
                """,
                task_id,
                max_attempts,
            )
        if row is not None:
            await record_workflow_task_failed(connection, row)


async def _cancel_inactive_workflow_task(
    connection: asyncpg.Connection,
    task_id: UUID,
    instance_status: str,
) -> None:
    row = await connection.fetchrow(
        """
        UPDATE execution_tasks
        SET status = 'cancelled',
            failure_code = CASE
                WHEN status = 'running' THEN 'lease_expired'
                ELSE failure_code
            END,
            lease_owner = NULL,
            lease_expires_at = NULL
        WHERE task_id = $1
          AND (
              status = 'queued'
              OR (
                  status = 'running'
                  AND lease_expires_at <= clock_timestamp()
              )
          )
        RETURNING *
        """,
        task_id,
    )
    if row is not None and instance_status == "failed":
        from ..workflows import record_workflow_task_failed

        await record_workflow_task_failed(connection, row)


def _lease_from_row(row: asyncpg.Record) -> TaskLease:
    return TaskLease(
        task_id=row["task_id"],
        tenant_id=row["tenant_id"],
        actor_id=row["actor_id"],
        origin_turn_id=row["origin_turn_id"],
        agent_name=row["agent_name"],
        executor_kind=ExecutorKind(
            row.get("executor_kind", ExecutorKind.AGENT.value)
        ),
        input=json.loads(row["input"]),
        attempt_count=row["attempt_count"],
        lease_generation=row["lease_generation"],
        worker_id=row["lease_owner"],
        expires_at=row["lease_expires_at"],
        origin_thread_id=row.get("origin_thread_id"),
        origin_agent_run_id=row.get("origin_agent_run_id"),
    )
