"""PostgreSQL authority for ordered Threads and coalesced Agent Runs."""

from __future__ import annotations

import json
from datetime import timedelta

import asyncpg

from ..task_queue.ledger import (
    AdmissionRejected,
    _request_fingerprint,
    _task_from_row,
)
from ..task_queue.models import Principal, SubmitTask, TaskRecord, canonical_json
from .models import (
    MAX_MESSAGE_BYTES,
    AgentRunLease,
    AgentRunRecord,
    AgentRunStatus,
    ThreadMessage,
)


class MessageConflict(ValueError):
    """One source message identity was reused with different content."""


class StaleAgentRunLease(RuntimeError):
    """An Agent Run worker no longer owns transition authority."""


class DelegationLimitReached(RuntimeError):
    """An Agent Run has consumed its two durable delegation slots."""


class PostgresThreadLedger:
    """Hide Thread ordering, run coalescing, fencing, and delegation budgets."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        tenant_outstanding_limit: int = 50,
        admission_retry_after_seconds: int = 5,
    ) -> None:
        if tenant_outstanding_limit < 1:
            raise ValueError("tenant_outstanding_limit must be positive")
        self._pool = pool
        self._tenant_outstanding_limit = tenant_outstanding_limit
        self._admission_retry_after_seconds = admission_retry_after_seconds

    async def append_message(
        self,
        principal: Principal,
        *,
        message_id: str,
        content: str,
        channel_key: str = "web:primary",
    ) -> ThreadMessage:
        if not message_id or len(message_id) > 128:
            raise ValueError("message_id must contain 1 to 128 characters")
        normalized = content.strip()
        if not normalized:
            raise ValueError("message content must not be blank")
        if len(normalized.encode("utf-8")) > MAX_MESSAGE_BYTES:
            raise ValueError(f"message exceeds {MAX_MESSAGE_BYTES} bytes")
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                thread_id = await self._lock_thread(
                    connection,
                    principal,
                    channel_key,
                )
                return await self._append_ingress(
                    connection,
                    thread_id=thread_id,
                    source_kind="inbound",
                    source_id=message_id,
                    role="user",
                    content=normalized,
                )

    async def list_messages(
        self,
        principal: Principal,
        *,
        channel_key: str = "web:primary",
        limit: int = 100,
    ) -> list[ThreadMessage]:
        if limit < 1 or limit > 500:
            raise ValueError("limit must be between 1 and 500")
        rows = await self._pool.fetch(
            """
            SELECT message.*
            FROM conversation_messages AS message
            JOIN conversation_threads AS thread
              ON thread.thread_id = message.thread_id
            WHERE thread.tenant_id = $1
              AND thread.actor_id = $2
              AND thread.channel_key = $3
              AND message.role IN ('user', 'assistant', 'agent')
              AND message.committed
              AND message.context_generation = thread.context_generation
            ORDER BY message.created_at DESC, message.message_id DESC
            LIMIT $4
            """,
            principal.tenant_id,
            principal.actor_id,
            channel_key,
            limit,
        )
        return [_message_from_row(row) for row in reversed(rows)]

    async def clear(
        self,
        principal: Principal,
        *,
        channel_key: str = "web:primary",
    ) -> None:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT *
                    FROM conversation_threads
                    WHERE tenant_id = $1
                      AND actor_id = $2
                      AND channel_key = $3
                    FOR UPDATE
                    """,
                    principal.tenant_id,
                    principal.actor_id,
                    channel_key,
                )
                if row is None:
                    return
                active = await connection.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM agent_runs
                        WHERE thread_id = $1
                          AND status IN ('queued', 'running')
                    )
                    """,
                    row["thread_id"],
                )
                if active:
                    raise RuntimeError("cannot clear a Thread with active work")
                await connection.execute(
                    """
                    UPDATE conversation_threads
                    SET context_generation = context_generation + 1,
                        processed_ingress_sequence = next_ingress_sequence,
                        updated_at = clock_timestamp()
                    WHERE thread_id = $1
                    """,
                    row["thread_id"],
                )

    async def claim_run(
        self,
        worker_id: str,
        lease_duration: timedelta,
    ) -> AgentRunLease | None:
        if not worker_id or len(worker_id) > 128:
            raise ValueError("worker_id must contain 1 to 128 characters")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                expired = await connection.fetch(
                    """
                    UPDATE agent_runs
                    SET status = 'failed',
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        completed_at = clock_timestamp()
                    WHERE status = 'running'
                      AND lease_expires_at <= clock_timestamp()
                      AND attempt_count >= 3
                    RETURNING thread_id, ingress_cutoff
                    """
                )
                for failed in expired:
                    thread = await connection.fetchrow(
                        """
                        SELECT *
                        FROM conversation_threads
                        WHERE thread_id = $1
                        FOR UPDATE
                        """,
                        failed["thread_id"],
                    )
                    if thread["next_ingress_sequence"] > failed["ingress_cutoff"]:
                        await self._ensure_queued_run(
                            connection,
                            failed["thread_id"],
                            thread["next_ingress_sequence"],
                            thread["next_context_sequence"],
                        )
                run = await connection.fetchrow(
                    """
                    SELECT *
                    FROM agent_runs
                    WHERE status = 'queued'
                       OR (
                            status = 'running'
                            AND lease_expires_at <= clock_timestamp()
                       )
                    ORDER BY created_at, run_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """
                )
                if run is None:
                    return None
                thread = await connection.fetchrow(
                    """
                    SELECT *
                    FROM conversation_threads
                    WHERE thread_id = $1
                    FOR UPDATE
                    """,
                    run["thread_id"],
                )
                pending = await connection.fetch(
                    """
                    SELECT message_id
                    FROM conversation_messages
                    WHERE thread_id = $1
                      AND ingress_sequence <= $2
                      AND context_sequence IS NULL
                    ORDER BY ingress_sequence
                    FOR UPDATE
                    """,
                    run["thread_id"],
                    run["ingress_cutoff"],
                )
                next_context = thread["next_context_sequence"]
                for message in pending:
                    next_context += 1
                    await connection.execute(
                        """
                        UPDATE conversation_messages
                        SET context_sequence = $2
                        WHERE message_id = $1
                        """,
                        message["message_id"],
                        next_context,
                    )
                if pending:
                    await connection.execute(
                        """
                        UPDATE conversation_threads
                        SET next_context_sequence = $2,
                            updated_at = clock_timestamp()
                        WHERE thread_id = $1
                        """,
                        run["thread_id"],
                        next_context,
                    )
                row = await connection.fetchrow(
                    """
                    UPDATE agent_runs
                    SET status = 'running',
                        context_cutoff = $2,
                        attempt_count = attempt_count + 1,
                        lease_generation = lease_generation + 1,
                        lease_owner = $3,
                        lease_expires_at = clock_timestamp() + $4::interval
                    WHERE run_id = $1
                    RETURNING *
                    """,
                    run["run_id"],
                    next_context,
                    worker_id,
                    lease_duration,
                )
                merged = dict(row)
                merged.update(
                    tenant_id=thread["tenant_id"],
                    actor_id=thread["actor_id"],
                    composio_user_id=thread["composio_user_id"],
                    input_source_kind=await connection.fetchval(
                        """
                        SELECT source_kind
                        FROM conversation_messages
                        WHERE thread_id = $1
                          AND ingress_sequence = $2
                        """,
                        run["thread_id"],
                        run["ingress_cutoff"],
                    ),
                )
        return _lease_from_row(merged)

    async def get_context(
        self,
        lease: AgentRunLease,
        *,
        limit: int = 50,
    ) -> list[dict]:
        if limit < 1 or limit > 50:
            raise ValueError("context limit must be between 1 and 50")
        rows = await self._pool.fetch(
            """
            SELECT message.sdk_item
            FROM conversation_messages AS message
            JOIN conversation_threads AS thread
              ON thread.thread_id = message.thread_id
            WHERE message.thread_id = $1
              AND message.context_generation = thread.context_generation
              AND message.context_sequence <= $2
              AND message.context_sequence IS NOT NULL
              AND message.committed
            ORDER BY message.context_sequence DESC
            LIMIT $3
            """,
            lease.thread_id,
            lease.context_cutoff,
            limit,
        )
        return [json.loads(row["sdk_item"]) for row in reversed(rows)]

    async def complete_run(
        self,
        lease: AgentRunLease,
        *,
        response: str | None,
    ) -> AgentRunRecord:
        normalized = response.strip() if response else ""
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                run = await self._lock_current_lease(connection, lease)
                thread = await connection.fetchrow(
                    """
                    SELECT *
                    FROM conversation_threads
                    WHERE thread_id = $1
                    FOR UPDATE
                    """,
                    lease.thread_id,
                )
                if thread is None:
                    raise RuntimeError("Agent Run Thread disappeared")
                thread = await self._commit_staged_items(
                    connection,
                    thread,
                    lease,
                )
                if normalized:
                    await self._append_context(
                        connection,
                        thread=thread,
                        source_kind="agent_run",
                        source_id=str(lease.run_id),
                        role="assistant",
                        content=normalized,
                        caused_by_run_id=lease.run_id,
                    )
                row = await connection.fetchrow(
                    """
                    UPDATE agent_runs
                    SET status = 'completed',
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        completed_at = clock_timestamp()
                    WHERE run_id = $1
                    RETURNING *
                    """,
                    lease.run_id,
                )
                await connection.execute(
                    """
                    UPDATE conversation_threads
                    SET processed_ingress_sequence = GREATEST(
                            processed_ingress_sequence,
                            $2
                        ),
                        updated_at = clock_timestamp()
                    WHERE thread_id = $1
                    """,
                    lease.thread_id,
                    lease.ingress_cutoff,
                )
                latest = await connection.fetchrow(
                    """
                    SELECT next_ingress_sequence, next_context_sequence
                    FROM conversation_threads
                    WHERE thread_id = $1
                    """,
                    lease.thread_id,
                )
                if latest["next_ingress_sequence"] > lease.ingress_cutoff:
                    await self._ensure_queued_run(
                        connection,
                        lease.thread_id,
                        latest["next_ingress_sequence"],
                        latest["next_context_sequence"],
                    )
        return _run_from_row(row or run)

    async def fail_run(self, lease: AgentRunLease) -> AgentRunRecord:
        """Release a failed attempt immediately, with a finite retry budget."""

        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await self._lock_current_lease(connection, lease)
                row = await connection.fetchrow(
                    """
                    UPDATE agent_runs
                    SET status = CASE
                            WHEN attempt_count < 3 THEN 'queued'
                            ELSE 'failed'
                        END,
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        completed_at = CASE
                            WHEN attempt_count >= 3
                            THEN clock_timestamp()
                            ELSE NULL
                        END
                    WHERE run_id = $1
                    RETURNING *
                    """,
                    lease.run_id,
                )
                thread = await connection.fetchrow(
                    """
                    SELECT *
                    FROM conversation_threads
                    WHERE thread_id = $1
                    FOR UPDATE
                    """,
                    lease.thread_id,
                )
                if (
                    row["status"] == "queued"
                    or thread["next_ingress_sequence"] > lease.ingress_cutoff
                ):
                    await self._ensure_queued_run(
                        connection,
                        lease.thread_id,
                        thread["next_ingress_sequence"],
                        thread["next_context_sequence"],
                    )
        return _run_from_row(row)

    async def submit_delegation(
        self,
        lease: AgentRunLease,
        principal: Principal,
        command: SubmitTask,
    ) -> TaskRecord:
        semantic_key = command.idempotency_key
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await self._lock_current_lease(connection, lease)
                if (lease.tenant_id, lease.actor_id) != (
                    principal.tenant_id,
                    principal.actor_id,
                ):
                    raise PermissionError("Agent Run principal does not match")
                existing = await connection.fetchrow(
                    """
                    SELECT task.*
                    FROM agent_run_delegations AS delegation
                    JOIN execution_tasks AS task
                      ON task.task_id = delegation.task_id
                    WHERE delegation.run_id = $1
                      AND delegation.semantic_key = $2
                    """,
                    lease.run_id,
                    semantic_key,
                )
                if existing is not None:
                    return _task_from_row(existing)
                count = await connection.fetchval(
                    """
                    SELECT delegation_count
                    FROM agent_runs
                    WHERE run_id = $1
                    FOR UPDATE
                    """,
                    lease.run_id,
                )
                if count >= 2:
                    raise DelegationLimitReached(
                        "Agent Run already used two execution delegations"
                    )
                await connection.execute(
                    """
                    SELECT pg_advisory_xact_lock(
                        $1,
                        hashtext($2)
                    )
                    """,
                    1_331_862_839,
                    principal.tenant_id,
                )
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
                    raise AdmissionRejected(
                        self._admission_retry_after_seconds
                    )
                serialized_input = canonical_json(command.input)
                fingerprint = _request_fingerprint(
                    principal,
                    command,
                    serialized_input,
                )
                task = await connection.fetchrow(
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
                    lease.thread_id,
                    lease.run_id,
                )
                await connection.execute(
                    """
                    INSERT INTO agent_run_delegations (
                        run_id,
                        semantic_key,
                        task_id
                    )
                    VALUES ($1, $2, $3)
                    """,
                    lease.run_id,
                    semantic_key,
                    task["task_id"],
                )
                await connection.execute(
                    """
                    UPDATE agent_runs
                    SET delegation_count = delegation_count + 1
                    WHERE run_id = $1
                    """,
                    lease.run_id,
                )
        return _task_from_row(task)

    async def append_run_items(
        self,
        lease: AgentRunLease,
        items: list[dict],
    ) -> None:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await self._lock_current_lease(connection, lease)
                thread = await connection.fetchrow(
                    """
                    SELECT * FROM conversation_threads
                    WHERE thread_id = $1
                    FOR UPDATE
                    """,
                    lease.thread_id,
                )
                start_index = (
                    await connection.fetchval(
                        """
                        SELECT COALESCE(max(producer_index), -1) + 1
                        FROM conversation_messages
                        WHERE caused_by_run_id = $1
                          AND producer_generation = $2
                        """,
                        lease.run_id,
                        lease.lease_generation,
                    )
                )
                for index, item in enumerate(items):
                    role = item.get("role")
                    content = item.get("content")
                    if role not in {"assistant", "tool"} or not isinstance(content, str):
                        raise ValueError(
                            "run-owned session items require assistant/tool text"
                        )
                    await connection.execute(
                        """
                        INSERT INTO conversation_messages (
                            thread_id,
                            context_generation,
                            source_kind,
                            source_id,
                            role,
                            content,
                            sdk_item,
                            caused_by_run_id,
                            producer_generation,
                            producer_index,
                            committed
                        )
                        VALUES (
                            $1, $2, 'sdk_run', $3, $4, $5,
                            $6::jsonb, $7, $8, $9, FALSE
                        )
                        ON CONFLICT (
                            thread_id,
                            source_kind,
                            source_id
                        ) DO NOTHING
                        """,
                        lease.thread_id,
                        thread["context_generation"],
                        (
                            f"{lease.run_id}:{lease.lease_generation}:"
                            f"{start_index + index}"
                        ),
                        role,
                        content,
                        canonical_json(item),
                        lease.run_id,
                        lease.lease_generation,
                        start_index + index,
                    )

    async def pop_run_item(self, lease: AgentRunLease) -> dict | None:
        raise RuntimeError("durable Thread audit items cannot be removed")

    async def _lock_thread(
        self,
        connection: asyncpg.Connection,
        principal: Principal,
        channel_key: str,
    ):
        await connection.execute(
            """
            INSERT INTO conversation_threads (
                tenant_id,
                actor_id,
                composio_user_id,
                channel_key
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (tenant_id, actor_id, channel_key) DO UPDATE
            SET composio_user_id = COALESCE(
                EXCLUDED.composio_user_id,
                conversation_threads.composio_user_id
            )
            """,
            principal.tenant_id,
            principal.actor_id,
            principal.composio_user_id,
            channel_key,
        )
        row = await connection.fetchrow(
            """
            SELECT *
            FROM conversation_threads
            WHERE tenant_id = $1
              AND actor_id = $2
              AND channel_key = $3
            FOR UPDATE
            """,
            principal.tenant_id,
            principal.actor_id,
            channel_key,
        )
        return row["thread_id"]

    async def _append_ingress(
        self,
        connection: asyncpg.Connection,
        *,
        thread_id,
        source_kind: str,
        source_id: str,
        role: str,
        content: str,
        caused_by_task_id=None,
        caused_by_run_id=None,
    ) -> ThreadMessage:
        existing = await connection.fetchrow(
            """
            SELECT *
            FROM conversation_messages
            WHERE thread_id = $1
              AND source_kind = $2
              AND source_id = $3
            """,
            thread_id,
            source_kind,
            source_id,
        )
        if existing is not None:
            if existing["content"] != content or existing["role"] != role:
                raise MessageConflict(
                    "message identity already identifies different content"
                )
            return _message_from_row(existing)
        thread = await connection.fetchrow(
            """
            UPDATE conversation_threads
            SET next_ingress_sequence = next_ingress_sequence + 1,
                updated_at = clock_timestamp()
            WHERE thread_id = $1
            RETURNING *
            """,
            thread_id,
        )
        item = {"role": role, "content": content}
        row = await connection.fetchrow(
            """
            INSERT INTO conversation_messages (
                thread_id,
                ingress_sequence,
                context_generation,
                source_kind,
                source_id,
                role,
                content,
                sdk_item,
                caused_by_run_id,
                caused_by_task_id
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10
            )
            RETURNING *
            """,
            thread_id,
            thread["next_ingress_sequence"],
            thread["context_generation"],
            source_kind,
            source_id,
            role,
            content,
            canonical_json(item),
            caused_by_run_id,
            caused_by_task_id,
        )
        await self._ensure_queued_run(
            connection,
            thread_id,
            thread["next_ingress_sequence"],
            thread["next_context_sequence"],
        )
        return _message_from_row(row)

    async def _commit_staged_items(
        self,
        connection: asyncpg.Connection,
        thread,
        lease: AgentRunLease,
    ):
        staged = await connection.fetch(
            """
            SELECT message_id
            FROM conversation_messages
            WHERE thread_id = $1
              AND caused_by_run_id = $2
              AND producer_generation = $3
              AND NOT committed
            ORDER BY created_at, message_id
            FOR UPDATE
            """,
            lease.thread_id,
            lease.run_id,
            lease.lease_generation,
        )
        next_context = thread["next_context_sequence"]
        for message in staged:
            next_context += 1
            await connection.execute(
                """
                UPDATE conversation_messages
                SET context_sequence = $2,
                    committed = TRUE
                WHERE message_id = $1
                """,
                message["message_id"],
                next_context,
            )
        if staged:
            await connection.execute(
                """
                UPDATE conversation_threads
                SET next_context_sequence = $2,
                    updated_at = clock_timestamp()
                WHERE thread_id = $1
                """,
                lease.thread_id,
                next_context,
            )
            updated = dict(thread)
            updated["next_context_sequence"] = next_context
            return updated
        return thread

    async def _append_context(
        self,
        connection: asyncpg.Connection,
        *,
        thread,
        source_kind: str,
        source_id: str,
        role: str,
        content: str,
        caused_by_run_id=None,
        sdk_item: dict | None = None,
    ) -> ThreadMessage:
        next_sequence = thread["next_context_sequence"] + 1
        await connection.execute(
            """
            UPDATE conversation_threads
            SET next_context_sequence = $2,
                updated_at = clock_timestamp()
            WHERE thread_id = $1
            """,
            thread["thread_id"],
            next_sequence,
        )
        row = await connection.fetchrow(
            """
            INSERT INTO conversation_messages (
                thread_id,
                context_sequence,
                context_generation,
                source_kind,
                source_id,
                role,
                content,
                sdk_item,
                caused_by_run_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)
            ON CONFLICT (thread_id, source_kind, source_id) DO UPDATE
            SET source_id = EXCLUDED.source_id
            RETURNING *
            """,
            thread["thread_id"],
            next_sequence,
            thread["context_generation"],
            source_kind,
            source_id,
            role,
            content,
            canonical_json(sdk_item or {"role": role, "content": content}),
            caused_by_run_id,
        )
        return _message_from_row(row)

    async def _ensure_queued_run(
        self,
        connection: asyncpg.Connection,
        thread_id,
        ingress_cutoff: int,
        context_cutoff: int,
    ) -> None:
        queued = await connection.execute(
            """
            UPDATE agent_runs
            SET ingress_cutoff = $2,
                context_cutoff = $3
            WHERE thread_id = $1
              AND status = 'queued'
            """,
            thread_id,
            ingress_cutoff,
            context_cutoff,
        )
        if queued != "UPDATE 0":
            return
        running = await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM agent_runs
                WHERE thread_id = $1 AND status = 'running'
            )
            """,
            thread_id,
        )
        if not running:
            await connection.execute(
                """
                INSERT INTO agent_runs (
                    thread_id,
                    ingress_cutoff,
                    context_cutoff
                )
                VALUES ($1, $2, $3)
                """,
                thread_id,
                ingress_cutoff,
                context_cutoff,
            )

    @staticmethod
    async def _lock_current_lease(
        connection: asyncpg.Connection,
        lease: AgentRunLease,
    ):
        row = await connection.fetchrow(
            """
            SELECT *
            FROM agent_runs
            WHERE run_id = $1
              AND status = 'running'
              AND lease_owner = $2
              AND lease_generation = $3
              AND lease_expires_at > clock_timestamp()
            FOR UPDATE
            """,
            lease.run_id,
            lease.worker_id,
            lease.lease_generation,
        )
        if row is None:
            raise StaleAgentRunLease("Agent Run lease is expired or superseded")
        return row


def _message_from_row(row: asyncpg.Record) -> ThreadMessage:
    return ThreadMessage(
        message_id=row["message_id"],
        thread_id=row["thread_id"],
        ingress_sequence=row["ingress_sequence"],
        context_sequence=row["context_sequence"],
        role=row["role"],
        content=row["content"],
        created_at=row["created_at"],
    )


def _run_from_row(row: asyncpg.Record) -> AgentRunRecord:
    return AgentRunRecord(
        run_id=row["run_id"],
        thread_id=row["thread_id"],
        status=AgentRunStatus(row["status"]),
        ingress_cutoff=row["ingress_cutoff"],
        context_cutoff=row["context_cutoff"],
        attempt_count=row["attempt_count"],
        delegation_count=row["delegation_count"],
    )


def _lease_from_row(row: asyncpg.Record) -> AgentRunLease:
    return AgentRunLease(
        run_id=row["run_id"],
        thread_id=row["thread_id"],
        tenant_id=row["tenant_id"],
        actor_id=row["actor_id"],
        composio_user_id=row["composio_user_id"],
        ingress_cutoff=row["ingress_cutoff"],
        context_cutoff=row["context_cutoff"],
        attempt_count=row["attempt_count"],
        lease_generation=row["lease_generation"],
        worker_id=row["lease_owner"],
        expires_at=row["lease_expires_at"],
        input_source_kind=row["input_source_kind"],
    )
