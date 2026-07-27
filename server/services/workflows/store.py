"""PostgreSQL authority for workflow publication and typed starts."""

from __future__ import annotations

import hashlib
import json
from typing import Protocol
from uuid import UUID

import asyncpg

from ..task_queue import (
    Principal,
    SubmitTask,
    TaskAdmission,
    TaskStatus,
    canonical_json,
)
from ..threads import AgentRunLease
from .models import (
    FieldContract,
    WaitTemplate,
    WorkflowSignalCommand,
    WorkflowSignalResult,
    WorkflowDefinition,
    WorkflowDefinitionRecord,
    WorkflowInstanceRecord,
    WorkflowInstanceStatus,
    WorkflowStartCommand,
    WorkflowStartResult,
    WorkflowStepRecord,
    WorkflowStepStatus,
    WorkflowWaitRecord,
    WorkflowWaitStatus,
    WorkflowWaitTarget,
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


class WaitNotFound(LookupError):
    """The exact Wait does not belong to the Principal's tenant."""


class SignalTooEarly(ValueError):
    """A Signal targeted a predefined Wait before it opened."""


class SignalAlreadySatisfied(ValueError):
    """A distinct Signal targeted a Wait that is already terminal."""


class SignalIdempotencyConflict(ValueError):
    """One Signal idempotency identity was reused for different work."""


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
        body = canonical_json(
            definition.model_dump(mode="json", exclude_none=True)
        )
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
                definition = definition_record.definition
                blocked_keys = {
                    dependency.step_key
                    for dependency in definition.dependencies or ()
                }
                blocked_keys.update(
                    route.step_key for route in definition.wait_routes or ()
                )
                tasks = []
                steps = []
                steps_by_key = {}
                for position, template in enumerate(
                    definition.step_templates
                ):
                    step_status = (
                        WorkflowStepStatus.BLOCKED
                        if template.key in blocked_keys
                        else WorkflowStepStatus.RUNNABLE
                    )
                    task = await self._admission.accept(
                        connection,
                        principal,
                        SubmitTask(
                            idempotency_key=(
                                f"workflow:{row['instance_id']}:{template.key}"
                            ),
                            origin_turn_id=command.origin_turn_id,
                            agent_name=template.agent_name,
                            executor_kind=template.executor_kind,
                            input=command.input,
                        ),
                        origin_thread_id=lease.thread_id if lease else None,
                        origin_agent_run_id=lease.run_id if lease else None,
                        initial_status=(
                            TaskStatus.BLOCKED
                            if step_status is WorkflowStepStatus.BLOCKED
                            else TaskStatus.QUEUED
                        ),
                    )
                    step = await connection.fetchrow(
                        """
                        INSERT INTO workflow_steps (
                            instance_id,
                            step_key,
                            execution_task_id,
                            step_position,
                            status
                        )
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING *
                        """,
                        row["instance_id"],
                        template.key,
                        task.task_id,
                        position,
                        step_status.value,
                    )
                    tasks.append(task)
                    steps.append(step)
                    steps_by_key[template.key] = step
                for dependency in definition.dependencies or ():
                    await connection.execute(
                        """
                        INSERT INTO workflow_step_dependencies (
                            instance_id,
                            step_id,
                            prerequisite_step_id
                        )
                        VALUES ($1, $2, $3)
                        """,
                        row["instance_id"],
                        steps_by_key[dependency.step_key]["step_id"],
                        steps_by_key[dependency.prerequisite_key]["step_id"],
                    )
                wait_blueprints = []
                for template in definition.waits or ():
                    wait_blueprints.append(
                        await connection.fetchrow(
                            """
                            INSERT INTO workflow_wait_blueprints (
                                instance_id,
                                wait_key,
                                signal_key,
                                input_contract
                            )
                            VALUES ($1, $2, $3, $4::jsonb)
                            RETURNING *
                            """,
                            row["instance_id"],
                            template.key,
                            template.signal_key,
                            canonical_json(
                                [
                                    field.model_dump(mode="json")
                                    for field in template.input_contract
                                ]
                            ),
                        )
                    )
                waits_by_key = {
                    item["wait_key"]: item for item in wait_blueprints
                }
                for prerequisite in definition.wait_prerequisites or ():
                    await connection.execute(
                        """
                        INSERT INTO workflow_wait_prerequisites (
                            instance_id,
                            wait_id,
                            prerequisite_step_id
                        )
                        VALUES ($1, $2, $3)
                        """,
                        row["instance_id"],
                        waits_by_key[prerequisite.wait_key]["wait_id"],
                        steps_by_key[
                            prerequisite.prerequisite_step_key
                        ]["step_id"],
                    )
                for route in definition.wait_routes or ():
                    await connection.execute(
                        """
                        INSERT INTO workflow_wait_routes (
                            instance_id,
                            wait_id,
                            step_id
                        )
                        VALUES ($1, $2, $3)
                        """,
                        row["instance_id"],
                        waits_by_key[route.wait_key]["wait_id"],
                        steps_by_key[route.step_key]["step_id"],
                    )
                open_waits = await connection.fetch(
                    """
                    INSERT INTO workflow_waits (wait_id, instance_id)
                    SELECT blueprint.wait_id, blueprint.instance_id
                    FROM workflow_wait_blueprints AS blueprint
                    WHERE blueprint.instance_id = $1
                      AND NOT EXISTS (
                          SELECT 1
                          FROM workflow_wait_prerequisites AS prerequisite
                          WHERE prerequisite.wait_id = blueprint.wait_id
                      )
                    RETURNING *
                    """,
                    row["instance_id"],
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
                            "step_id": str(steps[0]["step_id"]),
                            "task_id": str(tasks[0].task_id),
                            "step_ids": [
                                str(step["step_id"]) for step in steps
                            ],
                            "task_ids": [str(task.task_id) for task in tasks],
                            "open_wait_ids": [
                                str(wait["wait_id"]) for wait in open_waits
                            ],
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
            steps=tuple(_step_from_row(step) for step in steps),
            tasks=tuple(tasks),
            wait_targets=tuple(
                _wait_target_from_row(item) for item in wait_blueprints
            ),
            waits=tuple(
                _wait_from_rows(
                    wait,
                    next(
                        blueprint
                        for blueprint in wait_blueprints
                        if blueprint["wait_id"] == wait["wait_id"]
                    ),
                )
                for wait in open_waits
            ),
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

    async def get_wait(
        self,
        tenant_id: str,
        wait_id: UUID,
    ) -> WorkflowWaitRecord | None:
        row = await self._pool.fetchrow(
            """
            SELECT wait.*, blueprint.wait_key, blueprint.signal_key
            FROM workflow_waits AS wait
            JOIN workflow_wait_blueprints AS blueprint
              ON blueprint.wait_id = wait.wait_id
            JOIN workflow_instances AS instance
              ON instance.instance_id = wait.instance_id
            WHERE instance.tenant_id = $1 AND wait.wait_id = $2
            """,
            tenant_id,
            wait_id,
        )
        return _wait_from_joined_row(row) if row else None

    async def signal(
        self,
        principal: Principal,
        command: WorkflowSignalCommand,
        lease: AgentRunLease | None = None,
    ) -> WorkflowSignalResult:
        serialized_input = canonical_json(command.input)
        fingerprint = _signal_fingerprint(
            principal,
            command,
            serialized_input,
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
                replay = await connection.fetchrow(
                    """
                    SELECT *
                    FROM workflow_signals
                    WHERE tenant_id = $1 AND idempotency_key = $2
                    """,
                    principal.tenant_id,
                    command.idempotency_key,
                )
                if replay is not None:
                    if replay["request_fingerprint"] != fingerprint:
                        raise SignalIdempotencyConflict(
                            "idempotency key identifies a different Signal"
                        )
                    return await _load_signal_result(connection, replay)

                blueprint = await connection.fetchrow(
                    """
                    SELECT blueprint.*,
                           instance.status AS instance_status,
                           instance.actor_id AS instance_actor_id
                    FROM workflow_wait_blueprints AS blueprint
                    JOIN workflow_instances AS instance
                      ON instance.instance_id = blueprint.instance_id
                    WHERE blueprint.wait_id = $1
                      AND instance.tenant_id = $2
                    """,
                    command.wait_id,
                    principal.tenant_id,
                )
                if blueprint is None:
                    raise WaitNotFound("Wait was not found")
                instance = await connection.fetchrow(
                    """
                    SELECT *
                    FROM workflow_instances
                    WHERE instance_id = $1
                    FOR UPDATE
                    """,
                    blueprint["instance_id"],
                )
                if (
                    instance is None
                    or instance["actor_id"] != principal.actor_id
                ):
                    raise WaitNotFound("Wait was not found")
                replay = await connection.fetchrow(
                    """
                    SELECT *
                    FROM workflow_signals
                    WHERE tenant_id = $1 AND idempotency_key = $2
                    """,
                    principal.tenant_id,
                    command.idempotency_key,
                )
                if replay is not None:
                    if replay["request_fingerprint"] != fingerprint:
                        raise SignalIdempotencyConflict(
                            "idempotency key identifies a different Signal"
                        )
                    return await _load_signal_result(connection, replay)
                if instance["status"] != "active":
                    raise SignalAlreadySatisfied(
                        "Workflow Instance is already terminal"
                    )
                wait = await connection.fetchrow(
                    """
                    SELECT *
                    FROM workflow_waits
                    WHERE wait_id = $1
                    FOR UPDATE
                    """,
                    command.wait_id,
                )
                if wait is None:
                    raise SignalTooEarly("Wait is not open yet")
                if wait["status"] != "open":
                    raise SignalAlreadySatisfied("Wait is already terminal")
                if command.signal_key != blueprint["signal_key"]:
                    raise WaitNotFound("Signal does not target this Wait")
                if lease is not None:
                    run_created_at = await connection.fetchval(
                        """
                        SELECT created_at
                        FROM agent_runs
                        WHERE run_id = $1
                        """,
                        lease.run_id,
                    )
                    if (
                        lease.input_source_kind != "user_message"
                        or run_created_at is None
                        or run_created_at <= wait["created_at"]
                    ):
                        raise SignalTooEarly(
                            "Signal requires a later authenticated user turn"
                        )
                contract = tuple(
                    FieldContract.model_validate(item)
                    for item in json.loads(blueprint["input_contract"])
                )
                _validate_signal_input(contract, command.input)
                signal = await connection.fetchrow(
                    """
                    INSERT INTO workflow_signals (
                        wait_id,
                        tenant_id,
                        actor_id,
                        idempotency_key,
                        request_fingerprint,
                        signal_key,
                        input,
                        origin_thread_id,
                        origin_agent_run_id
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
                    RETURNING *
                    """,
                    command.wait_id,
                    principal.tenant_id,
                    principal.actor_id,
                    command.idempotency_key,
                    fingerprint,
                    command.signal_key,
                    serialized_input,
                    lease.thread_id if lease else None,
                    lease.run_id if lease else None,
                )
                wait = await connection.fetchrow(
                    """
                    UPDATE workflow_waits
                    SET status = 'satisfied',
                        satisfied_at = clock_timestamp(),
                        satisfied_by_signal_id = $2
                    WHERE wait_id = $1 AND status = 'open'
                    RETURNING *
                    """,
                    command.wait_id,
                    signal["signal_id"],
                )
                released = await _release_wait_routes(
                    connection,
                    command.wait_id,
                )
                await _append_workflow_event(
                    connection,
                    instance_id=blueprint["instance_id"],
                    event_type="wait_satisfied",
                    payload={
                        "wait_id": str(command.wait_id),
                        "signal_id": str(signal["signal_id"]),
                        "signal_key": command.signal_key,
                        "actor_id": principal.actor_id,
                        "released_step_ids": [
                            str(item["step_id"]) for item in released
                        ],
                    },
                )
                if lease is not None:
                    await self._run_authority.consume_submission(
                        connection,
                        lease.run_id,
                    )
                return WorkflowSignalResult(
                    signal_id=signal["signal_id"],
                    wait=_wait_from_rows(wait, blueprint),
                    released_step_ids=tuple(
                        item["step_id"] for item in released
                    ),
                    accepted_at=signal["accepted_at"],
                )

    async def _load_start(
        self,
        connection: asyncpg.Connection,
        instance_id: UUID,
    ) -> WorkflowStartResult:
        row = await connection.fetchrow(
            "SELECT * FROM workflow_instances WHERE instance_id = $1",
            instance_id,
        )
        steps = await connection.fetch(
            """
            SELECT *
            FROM workflow_steps
            WHERE instance_id = $1
            ORDER BY step_position
            """,
            instance_id,
        )
        tasks = []
        for step in steps:
            tasks.append(
                await self._admission.get(
                    connection,
                    step["execution_task_id"],
                )
            )
        wait_targets = await connection.fetch(
            """
            SELECT *
            FROM workflow_wait_blueprints
            WHERE instance_id = $1
            ORDER BY wait_key
            """,
            instance_id,
        )
        waits = await connection.fetch(
            """
            SELECT wait.*, blueprint.wait_key, blueprint.signal_key
            FROM workflow_waits AS wait
            JOIN workflow_wait_blueprints AS blueprint
              ON blueprint.wait_id = wait.wait_id
            WHERE wait.instance_id = $1
            ORDER BY blueprint.wait_key
            """,
            instance_id,
        )
        return WorkflowStartResult(
            instance=_instance_from_row(row),
            steps=tuple(_step_from_row(step) for step in steps),
            tasks=tuple(tasks),
            wait_targets=tuple(
                _wait_target_from_row(item) for item in wait_targets
            ),
            waits=tuple(
                _wait_from_joined_row(item) for item in waits
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


def _signal_fingerprint(
    principal: Principal,
    command: WorkflowSignalCommand,
    serialized_input: str,
) -> str:
    value = canonical_json(
        {
            "actor_id": principal.actor_id,
            "wait_id": str(command.wait_id),
            "signal_key": command.signal_key,
            "input_json": serialized_input,
        }
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _validate_signal_input(
    contract: tuple[FieldContract, ...],
    value: dict,
) -> None:
    WaitTemplate(
        key="validation",
        signal_key="validation",
        input_contract=contract,
    ).validate_input(value)


async def _release_wait_routes(
    connection: asyncpg.Connection,
    wait_id: UUID,
) -> list[asyncpg.Record]:
    candidates = await connection.fetch(
        """
        SELECT step.step_id,
               step.execution_task_id,
               task.executor_kind
        FROM workflow_wait_routes AS route
        JOIN workflow_steps AS step ON step.step_id = route.step_id
        JOIN execution_tasks AS task
          ON task.task_id = step.execution_task_id
        WHERE route.wait_id = $1
          AND step.status = 'blocked'
          AND NOT EXISTS (
              SELECT 1
              FROM workflow_step_dependencies AS dependency
              JOIN workflow_steps AS prerequisite
                ON prerequisite.step_id = dependency.prerequisite_step_id
              WHERE dependency.step_id = step.step_id
                AND prerequisite.status <> 'completed'
          )
          AND NOT EXISTS (
              SELECT 1
              FROM workflow_wait_routes AS required_route
              LEFT JOIN workflow_waits AS required_wait
                ON required_wait.wait_id = required_route.wait_id
              WHERE required_route.step_id = step.step_id
                AND (
                    required_wait.wait_id IS NULL
                    OR required_wait.status <> 'satisfied'
                )
          )
        ORDER BY step.created_at, step.step_id
        FOR UPDATE OF step
        """,
        wait_id,
    )
    for candidate in candidates:
        updated_step = await connection.execute(
            """
            UPDATE workflow_steps
            SET status = 'runnable'
            WHERE step_id = $1 AND status = 'blocked'
            """,
            candidate["step_id"],
        )
        updated_task = await connection.execute(
            """
            UPDATE execution_tasks
            SET status = 'queued'
            WHERE task_id = $1 AND status = 'blocked'
            """,
            candidate["execution_task_id"],
        )
        if updated_step != "UPDATE 1" or updated_task != "UPDATE 1":
            raise RuntimeError("Wait route could not release Workflow Step")
        from ..task_queue.outbox import append_task_wake

        await append_task_wake(
            connection,
            task_id=candidate["execution_task_id"],
            executor_kind=candidate["executor_kind"],
            source_transition="signal_released",
        )
    return list(candidates)


async def _append_workflow_event(
    connection: asyncpg.Connection,
    *,
    instance_id: UUID,
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
        canonical_json(payload),
    )


async def _load_signal_result(
    connection: asyncpg.Connection,
    signal: asyncpg.Record,
) -> WorkflowSignalResult:
    wait = await connection.fetchrow(
        """
        SELECT wait.*, blueprint.wait_key, blueprint.signal_key
        FROM workflow_waits AS wait
        JOIN workflow_wait_blueprints AS blueprint
          ON blueprint.wait_id = wait.wait_id
        WHERE wait.wait_id = $1
        """,
        signal["wait_id"],
    )
    event = await connection.fetchrow(
        """
        SELECT payload
        FROM workflow_events
        WHERE event_type = 'wait_satisfied'
          AND payload->>'signal_id' = $1
        """,
        str(signal["signal_id"]),
    )
    payload = json.loads(event["payload"])
    return WorkflowSignalResult(
        signal_id=signal["signal_id"],
        wait=_wait_from_joined_row(wait),
        released_step_ids=tuple(
            UUID(item) for item in payload["released_step_ids"]
        ),
        accepted_at=signal["accepted_at"],
    )


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
        position=row.get("step_position", 0),
        execution_task_id=row["execution_task_id"],
        status=WorkflowStepStatus(row["status"]),
        created_at=row["created_at"],
    )


def _wait_target_from_row(row: asyncpg.Record) -> WorkflowWaitTarget:
    return WorkflowWaitTarget(
        wait_id=row["wait_id"],
        instance_id=row["instance_id"],
        key=row["wait_key"],
        signal_key=row["signal_key"],
    )


def _wait_from_rows(
    wait: asyncpg.Record,
    blueprint: asyncpg.Record,
) -> WorkflowWaitRecord:
    return WorkflowWaitRecord(
        wait_id=wait["wait_id"],
        instance_id=wait["instance_id"],
        key=blueprint["wait_key"],
        signal_key=blueprint["signal_key"],
        status=WorkflowWaitStatus(wait["status"]),
        created_at=wait["created_at"],
        satisfied_at=wait["satisfied_at"],
        satisfied_by_signal_id=wait["satisfied_by_signal_id"],
    )


def _wait_from_joined_row(row: asyncpg.Record) -> WorkflowWaitRecord:
    return _wait_from_rows(row, row)
