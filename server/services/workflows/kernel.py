"""Serialized Workflow transitions driven by accepted task results."""

from __future__ import annotations

import json

import asyncpg


async def record_workflow_task_claimed(
    connection: asyncpg.Connection,
    task: asyncpg.Record,
) -> None:
    """Record one leased execution Attempt for a Workflow Step."""

    step = await _step_for_task(connection, task["task_id"])
    if step is None:
        return
    instance = await _lock_instance(connection, step["instance_id"])
    if instance["status"] != "active":
        raise RuntimeError("Workflow Instance is not active")
    updated = await connection.execute(
        """
        UPDATE workflow_steps
        SET status = 'running'
        WHERE step_id = $1
          AND status IN ('runnable', 'running')
        """,
        step["step_id"],
    )
    if updated != "UPDATE 1":
        raise RuntimeError("Workflow Step cannot accept a task claim")
    await _append_event(
        connection,
        instance_id=step["instance_id"],
        event_type="step_attempt_started",
        payload={
            "step_id": str(step["step_id"]),
            "task_id": str(task["task_id"]),
            "lease_generation": task["lease_generation"],
            "attempt_count": task["attempt_count"],
        },
    )


async def advance_workflow_for_task(
    connection: asyncpg.Connection,
    task: asyncpg.Record,
) -> bool | None:
    """Complete one Step and release every newly AND-ready dependent."""

    step = await _step_for_task(connection, task["task_id"])
    if step is None:
        return None
    instance = await _lock_instance(connection, step["instance_id"])
    completed = await connection.fetchrow(
        """
        UPDATE workflow_steps
        SET status = 'completed'
        WHERE step_id = $1
          AND status = 'running'
        RETURNING *
        """,
        step["step_id"],
    )
    if completed is None:
        raise RuntimeError("Workflow Step cannot accept this task result")
    if instance["status"] != "active":
        await _append_event(
            connection,
            instance_id=step["instance_id"],
            event_type="step_completed_after_workflow_failure",
            payload={
                "step_id": str(step["step_id"]),
                "task_id": str(task["task_id"]),
                "lease_generation": task["lease_generation"],
            },
        )
        return False

    released = await connection.fetch(
        """
        SELECT candidate.step_id, candidate.execution_task_id
        FROM workflow_steps AS candidate
        WHERE candidate.instance_id = $1
          AND candidate.status = 'blocked'
          AND NOT EXISTS (
              SELECT 1
              FROM workflow_step_dependencies AS dependency
              JOIN workflow_steps AS prerequisite
                ON prerequisite.step_id = dependency.prerequisite_step_id
              WHERE dependency.step_id = candidate.step_id
                AND prerequisite.status <> 'completed'
          )
        ORDER BY candidate.created_at, candidate.step_id
        FOR UPDATE OF candidate
        """,
        step["instance_id"],
    )
    for candidate in released:
        await connection.execute(
            """
            UPDATE workflow_steps
            SET status = 'runnable'
            WHERE step_id = $1 AND status = 'blocked'
            """,
            candidate["step_id"],
        )
        task_update = await connection.execute(
            """
            UPDATE execution_tasks
            SET status = 'queued'
            WHERE task_id = $1 AND status = 'blocked'
            """,
            candidate["execution_task_id"],
        )
        if task_update != "UPDATE 1":
            raise RuntimeError("blocked Workflow task could not be released")

    remaining = await connection.fetchval(
        """
        SELECT count(*)
        FROM workflow_steps
        WHERE instance_id = $1 AND status <> 'completed'
        """,
        step["instance_id"],
    )
    workflow_completed = remaining == 0
    if workflow_completed:
        await connection.execute(
            """
            UPDATE workflow_instances
            SET status = 'completed'
            WHERE instance_id = $1 AND status = 'active'
            """,
            step["instance_id"],
        )
    await _append_event(
        connection,
        instance_id=step["instance_id"],
        event_type="step_completed",
        payload={
            "step_id": str(step["step_id"]),
            "task_id": str(task["task_id"]),
            "lease_generation": task["lease_generation"],
            "released_step_ids": [
                str(candidate["step_id"]) for candidate in released
            ],
            "workflow_completed": workflow_completed,
        },
    )
    return workflow_completed


async def record_workflow_task_failed(
    connection: asyncpg.Connection,
    task: asyncpg.Record,
) -> None:
    """Return a retried Step to runnable or fail its Workflow terminally."""

    step = await _step_for_task(connection, task["task_id"])
    if step is None:
        return
    instance = await _lock_instance(connection, step["instance_id"])
    terminal = (
        task["status"] == "dead_lettered"
        or instance["status"] == "failed"
    )
    next_status = "failed" if terminal else "runnable"
    updated = await connection.execute(
        """
        UPDATE workflow_steps
        SET status = $2
        WHERE step_id = $1
          AND status IN ('runnable', 'running')
        """,
        step["step_id"],
        next_status,
    )
    if updated != "UPDATE 1":
        raise RuntimeError("Workflow Step cannot accept a task failure")
    if terminal:
        await connection.execute(
            """
            UPDATE workflow_instances
            SET status = 'failed'
            WHERE instance_id = $1 AND status = 'active'
            """,
            step["instance_id"],
        )
        cancelled = await connection.fetch(
            """
            UPDATE execution_tasks AS task
            SET status = 'cancelled',
                lease_owner = NULL,
                lease_expires_at = NULL
            FROM workflow_steps AS sibling
            WHERE sibling.instance_id = $1
              AND sibling.execution_task_id = task.task_id
              AND task.status IN ('blocked', 'queued')
            RETURNING task.task_id
            """,
            step["instance_id"],
        )
        await connection.execute(
            """
            UPDATE workflow_steps
            SET status = 'failed'
            WHERE instance_id = $1
              AND status IN ('blocked', 'runnable')
            """,
            step["instance_id"],
        )
    else:
        cancelled = []
    await _append_event(
        connection,
        instance_id=step["instance_id"],
        event_type="step_attempt_failed",
        payload={
            "step_id": str(step["step_id"]),
            "task_id": str(task["task_id"]),
            "lease_generation": task["lease_generation"],
            "attempt_count": task["attempt_count"],
            "failure_code": task["failure_code"],
            "terminal": terminal,
            "cancelled_task_ids": [
                str(item["task_id"]) for item in cancelled
            ],
        },
    )


async def _step_for_task(
    connection: asyncpg.Connection,
    task_id,
) -> asyncpg.Record | None:
    if await connection.fetchval(
        "SELECT to_regclass('workflow_steps')"
    ) is None:
        return None
    return await connection.fetchrow(
        """
        SELECT *
        FROM workflow_steps
        WHERE execution_task_id = $1
        """,
        task_id,
    )


async def _lock_instance(
    connection: asyncpg.Connection,
    instance_id,
) -> asyncpg.Record:
    locked = await connection.fetchrow(
        """
        SELECT *
        FROM workflow_instances
        WHERE instance_id = $1
        FOR UPDATE
        """,
        instance_id,
    )
    if locked is None:
        raise RuntimeError("Workflow Instance disappeared")
    return locked


async def _append_event(
    connection: asyncpg.Connection,
    *,
    instance_id,
    event_type: str,
    payload: dict,
) -> None:
    sequence = await connection.fetchval(
        """
        SELECT COALESCE(max(sequence), 0) + 1
        FROM workflow_events
        WHERE instance_id = $1
        """,
        instance_id,
    )
    await connection.execute(
        """
        INSERT INTO workflow_events (
            instance_id,
            sequence,
            event_type,
            payload
        )
        VALUES ($1, $2, $3, $4::jsonb)
        """,
        instance_id,
        sequence,
        event_type,
        json.dumps(payload, separators=(",", ":"), sort_keys=True),
    )
