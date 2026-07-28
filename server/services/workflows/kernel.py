"""Serialized Workflow transitions driven by accepted task results."""

from __future__ import annotations

import json
import hashlib

import asyncpg

from ..task_queue import (
    TaskSuspension,
    TaskSuspensionRecord,
    append_task_wake,
    canonical_json,
    suspension_record_from_row,
)


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

    opened_waits = await _open_ready_waits(
        connection,
        step["instance_id"],
    )
    released = await connection.fetch(
        """
        SELECT candidate.step_id,
               candidate.execution_task_id,
               task.executor_kind
        FROM workflow_steps AS candidate
        JOIN execution_tasks AS task
          ON task.task_id = candidate.execution_task_id
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
          AND NOT EXISTS (
              SELECT 1
              FROM workflow_wait_routes AS route
              LEFT JOIN workflow_waits AS wait
                ON wait.wait_id = route.wait_id
              WHERE route.step_id = candidate.step_id
                AND (
                    wait.wait_id IS NULL
                    OR wait.status <> 'satisfied'
                )
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
        await append_task_wake(
            connection,
            task_id=candidate["execution_task_id"],
            executor_kind=candidate["executor_kind"],
            source_transition="dependency_released",
        )

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
            "opened_wait_ids": [
                str(wait["wait_id"]) for wait in opened_waits
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
        await connection.execute(
            """
            UPDATE workflow_waits
            SET status = 'cancelled'
            WHERE instance_id = $1 AND status = 'open'
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


async def suspend_workflow_task(
    connection: asyncpg.Connection,
    task: asyncpg.Record,
    suspension: TaskSuspension,
) -> TaskSuspensionRecord:
    """Open a published interruption Wait and persist one immutable Attempt."""

    step = await _step_for_task(connection, task["task_id"])
    if step is None:
        raise RuntimeError("only Workflow Steps can suspend")
    instance = await _lock_instance(connection, step["instance_id"])
    if instance["status"] != "active":
        raise RuntimeError("Workflow Instance is not active")
    interruption = await connection.fetchrow(
        """
        SELECT mapping.wait_id
        FROM workflow_step_interruption_waits AS mapping
        JOIN workflow_wait_blueprints AS blueprint
          ON blueprint.wait_id = mapping.wait_id
        WHERE mapping.step_id = $1
          AND blueprint.wait_key = $2
        FOR UPDATE OF mapping
        """,
        step["step_id"],
        suspension.wait_key,
    )
    if interruption is None:
        raise RuntimeError(
            "Workflow Definition does not allow this Step interruption"
        )
    wait = await connection.fetchrow(
        """
        INSERT INTO workflow_waits (wait_id, instance_id)
        VALUES ($1, $2)
        ON CONFLICT (wait_id) DO NOTHING
        RETURNING *
        """,
        interruption["wait_id"],
        step["instance_id"],
    )
    if wait is None:
        raise RuntimeError("Workflow interruption Wait is already open or terminal")
    updated = await connection.execute(
        """
        UPDATE workflow_steps
        SET status = 'blocked'
        WHERE step_id = $1 AND status = 'running'
        """,
        step["step_id"],
    )
    if updated != "UPDATE 1":
        raise RuntimeError("Workflow Step cannot accept a suspension")

    serialized_state = canonical_json(suspension.state)
    snapshot = await connection.fetchrow(
        """
        INSERT INTO workflow_run_state_snapshots (
            instance_id,
            step_id,
            task_id,
            wait_id,
            attempt_count,
            lease_generation,
            codec_version,
            agents_sdk_version,
            agent_definition_version,
            model_requests_used,
            specialist_calls_used,
            state_json,
            state_sha256
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13
        )
        RETURNING *
        """,
        step["instance_id"],
        step["step_id"],
        task["task_id"],
        wait["wait_id"],
        task["attempt_count"],
        task["lease_generation"],
        suspension.compatibility.codec_version,
        suspension.compatibility.agents_sdk_version,
        suspension.compatibility.agent_definition_version,
        suspension.model_requests_used,
        suspension.specialist_calls_used,
        serialized_state,
        hashlib.sha256(serialized_state.encode("utf-8")).hexdigest(),
    )
    await _append_event(
        connection,
        instance_id=step["instance_id"],
        event_type="step_attempt_suspended",
        payload={
            "step_id": str(step["step_id"]),
            "task_id": str(task["task_id"]),
            "wait_id": str(wait["wait_id"]),
            "lease_generation": task["lease_generation"],
            "attempt_count": task["attempt_count"],
            "codec_version": suspension.compatibility.codec_version,
            "agents_sdk_version": suspension.compatibility.agents_sdk_version,
            "agent_definition_version": (
                suspension.compatibility.agent_definition_version
            ),
            "model_requests_used": suspension.model_requests_used,
            "specialist_calls_used": suspension.specialist_calls_used,
        },
    )
    return suspension_record_from_row(snapshot)


async def release_suspended_workflow_task(
    connection: asyncpg.Connection,
    wait_id,
) -> asyncpg.Record | None:
    """Make the suspended logical Step runnable after its exact Wait is satisfied."""

    candidate = await connection.fetchrow(
        """
        SELECT step.step_id,
               step.execution_task_id,
               task.executor_kind
        FROM workflow_run_state_snapshots AS snapshot
        JOIN workflow_steps AS step ON step.step_id = snapshot.step_id
        JOIN execution_tasks AS task
          ON task.task_id = step.execution_task_id
        WHERE snapshot.wait_id = $1
          AND step.status = 'blocked'
          AND task.status = 'blocked'
        FOR UPDATE OF step, task
        """,
        wait_id,
    )
    if candidate is None:
        return None
    step_update = await connection.execute(
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
    if step_update != "UPDATE 1" or task_update != "UPDATE 1":
        raise RuntimeError("suspended Workflow Step could not be released")
    await append_task_wake(
        connection,
        task_id=candidate["execution_task_id"],
        executor_kind=candidate["executor_kind"],
        source_transition="signal_released",
    )
    return candidate


async def _open_ready_waits(
    connection: asyncpg.Connection,
    instance_id,
) -> list[asyncpg.Record]:
    return list(
        await connection.fetch(
            """
            INSERT INTO workflow_waits (wait_id, instance_id)
            SELECT blueprint.wait_id, blueprint.instance_id
            FROM workflow_wait_blueprints AS blueprint
            WHERE blueprint.instance_id = $1
              AND NOT EXISTS (
                  SELECT 1
                  FROM workflow_waits AS existing
                  WHERE existing.wait_id = blueprint.wait_id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM workflow_step_interruption_waits AS interruption
                  WHERE interruption.wait_id = blueprint.wait_id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM workflow_wait_prerequisites AS prerequisite
                  JOIN workflow_steps AS step
                    ON step.step_id = prerequisite.prerequisite_step_id
                  WHERE prerequisite.wait_id = blueprint.wait_id
                    AND step.status <> 'completed'
              )
            ON CONFLICT (wait_id) DO NOTHING
            RETURNING *
            """,
            instance_id,
        )
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
