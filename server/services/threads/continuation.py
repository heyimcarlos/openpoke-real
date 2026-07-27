"""Connection-scoped execution-result transition used by the task ledger."""

from __future__ import annotations

import json

import asyncpg

from ..task_queue.models import canonical_json


async def append_execution_result(
    connection: asyncpg.Connection,
    task: asyncpg.Record,
) -> None:
    """Append and wake one Thread inside the fenced completion transaction."""

    result = json.loads(task["result"])
    response = result.get("response")
    if not isinstance(response, str) or not response.strip():
        raise ValueError("completed Agent task result has no response")
    thread = await connection.fetchrow(
        """
        SELECT *
        FROM conversation_threads
        WHERE thread_id = $1
        FOR UPDATE
        """,
        task["origin_thread_id"],
    )
    if thread is None:
        raise RuntimeError("originating Thread disappeared")
    source_id = str(task["task_id"])
    existing = await connection.fetchrow(
        """
        SELECT message_id
        FROM conversation_messages
        WHERE thread_id = $1
          AND source_kind = 'execution_result'
          AND source_id = $2
        """,
        thread["thread_id"],
        source_id,
    )
    if existing is not None:
        return
    next_ingress = thread["next_ingress_sequence"] + 1
    content = f"[SUCCESS] {task['agent_name']}: {response.strip()}"
    await connection.execute(
        """
        UPDATE conversation_threads
        SET next_ingress_sequence = $2,
            updated_at = clock_timestamp()
        WHERE thread_id = $1
        """,
        thread["thread_id"],
        next_ingress,
    )
    await connection.execute(
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
            $1, $2, $3, 'execution_result', $4, 'agent',
            $5, $6::jsonb, $7, $8
        )
        """,
        thread["thread_id"],
        next_ingress,
        thread["context_generation"],
        source_id,
        content,
        canonical_json({"role": "user", "content": content}),
        task["origin_agent_run_id"],
        task["task_id"],
    )
    queued = await connection.execute(
        """
        UPDATE agent_runs
        SET ingress_cutoff = $2
        WHERE thread_id = $1
          AND status = 'queued'
        """,
        thread["thread_id"],
        next_ingress,
    )
    if queued != "UPDATE 0":
        return
    running = await connection.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM agent_runs
            WHERE thread_id = $1
              AND status = 'running'
        )
        """,
        thread["thread_id"],
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
            thread["thread_id"],
            next_ingress,
            thread["next_context_sequence"],
        )
