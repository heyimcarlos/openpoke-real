from __future__ import annotations

import asyncio
from datetime import timedelta

import asyncpg
import pytest
from pydantic import ValidationError

from server.services.task_queue import (
    AdmissionRejected,
    FailureCode,
    IdempotencyConflict,
    PostgresTaskLedger,
    Principal,
    StaleLease,
    SubmitTask,
    TaskAdmission,
    TaskFailure,
    TaskResultConflict,
    TaskStatus,
)
from server.services.workflows import (
    FieldContract,
    FieldType,
    PostgresWorkflowStore,
    StepDependency,
    StepTemplate,
    WorkflowDefinition,
    WorkflowDefinitionRegistry,
    WorkflowService,
    WorkflowStartCommand,
    WorkflowStepStatus,
)


PRINCIPAL = Principal(
    actor_id="user-7",
    tenant_id="tenant-a",
    scopes=frozenset(
        {"workflows:publish", "workflows:start", "workflows:read"}
    ),
)


def _parallel_definition() -> WorkflowDefinition:
    return WorkflowDefinition(
        key="openpoke.parallel_demo",
        version=1,
        input_contract=(
            FieldContract(name="mode", value_type=FieldType.STRING),
            FieldContract(name="duration_ms", value_type=FieldType.INTEGER),
        ),
        steps=(
            StepTemplate(
                key="extract_a",
                agent_name="extract-a",
                executor_kind="synthetic",
            ),
            StepTemplate(
                key="extract_b",
                agent_name="extract-b",
                executor_kind="synthetic",
            ),
            StepTemplate(
                key="validate",
                agent_name="validate",
                executor_kind="synthetic",
            ),
        ),
        dependencies=(
            StepDependency(step_key="validate", prerequisite_key="extract_a"),
            StepDependency(step_key="validate", prerequisite_key="extract_b"),
        ),
    )


async def _started(
    pool: asyncpg.Pool,
):
    ledger = PostgresTaskLedger(pool)
    await ledger.migrate()
    store = PostgresWorkflowStore(pool)
    await WorkflowDefinitionRegistry(store).publish(
        PRINCIPAL,
        _parallel_definition(),
    )
    started = await WorkflowService(store).start(
        PRINCIPAL,
        WorkflowStartCommand(
            idempotency_key="parallel-1",
            origin_turn_id="turn-1",
            definition_key="openpoke.parallel_demo",
            definition_version=1,
            input={"mode": "success", "duration_ms": 0},
        ),
    )
    return ledger, started


@pytest.mark.asyncio
async def test_start_materializes_static_dag_and_reserves_blocked_work(
    postgres_pool: asyncpg.Pool,
) -> None:
    _, started = await _started(postgres_pool)
    replay = await WorkflowService(
        PostgresWorkflowStore(postgres_pool)
    ).start(
        PRINCIPAL,
        WorkflowStartCommand(
            idempotency_key="parallel-1",
            origin_turn_id="turn-1",
            definition_key="openpoke.parallel_demo",
            definition_version=1,
            input={"mode": "success", "duration_ms": 0},
        ),
    )

    assert replay == started
    assert {step.key: step.status for step in started.steps} == {
        "extract_a": WorkflowStepStatus.RUNNABLE,
        "extract_b": WorkflowStepStatus.RUNNABLE,
        "validate": WorkflowStepStatus.BLOCKED,
    }
    assert {task.agent_name: task.status for task in started.tasks} == {
        "extract-a": TaskStatus.QUEUED,
        "extract-b": TaskStatus.QUEUED,
        "validate": TaskStatus.BLOCKED,
    }
    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM workflow_step_dependencies"
    ) == 2


@pytest.mark.asyncio
async def test_fan_in_releases_only_after_every_predecessor_succeeds(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, started = await _started(postgres_pool)

    first = await ledger.claim("worker-1", timedelta(seconds=30))
    assert first is not None
    await ledger.complete(first, {"response": f"{first.agent_name} done"})
    blocked = next(task for task in started.tasks if task.agent_name == "validate")
    assert (await ledger.get("tenant-a", blocked.task_id)).status is TaskStatus.BLOCKED

    restarted_ledger = PostgresTaskLedger(postgres_pool)
    second = await restarted_ledger.claim(
        "worker-2",
        timedelta(seconds=30),
    )
    assert second is not None
    await restarted_ledger.complete(
        second,
        {"response": f"{second.agent_name} done"},
    )
    released = await restarted_ledger.get("tenant-a", blocked.task_id)

    assert released is not None
    assert released.status is TaskStatus.QUEUED
    final_lease = await restarted_ledger.claim(
        "worker-3",
        timedelta(seconds=30),
    )
    assert final_lease is not None
    assert final_lease.task_id == blocked.task_id
    final = await restarted_ledger.complete(
        final_lease,
        {"response": "workflow completed"},
    )
    assert final.status is TaskStatus.COMPLETED
    assert await postgres_pool.fetchval(
        """
        SELECT status
        FROM workflow_instances
        WHERE instance_id = $1
        """,
        started.instance.instance_id,
    ) == "completed"


@pytest.mark.asyncio
async def test_exact_completion_replays_but_conflicts_do_not_mutate_kernel(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, _ = await _started(postgres_pool)
    lease = await ledger.claim("worker-1", timedelta(seconds=30))
    assert lease is not None

    accepted = await ledger.complete(lease, {"response": "accepted"})
    replay = await ledger.complete(lease, {"response": "accepted"})
    with pytest.raises(TaskResultConflict):
        await ledger.complete(lease, {"response": "different"})

    assert replay == accepted
    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM workflow_events WHERE event_type = 'step_completed'"
    ) == 1
    assert await postgres_pool.fetchval(
        """
        SELECT (payload->>'lease_generation')::bigint
        FROM workflow_events
        WHERE event_type = 'step_completed'
        """
    ) == lease.lease_generation


@pytest.mark.asyncio
async def test_concurrent_predecessors_serialize_one_release(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, started = await _started(postgres_pool)
    first = await ledger.claim("worker-1", timedelta(seconds=30))
    second = await ledger.claim("worker-2", timedelta(seconds=30))
    assert first is not None
    assert second is not None

    await asyncio.gather(
        ledger.complete(first, {"response": "first"}),
        ledger.complete(second, {"response": "second"}),
    )

    blocked = next(task for task in started.tasks if task.agent_name == "validate")
    released = await ledger.get("tenant-a", blocked.task_id)
    assert released is not None
    assert released.status is TaskStatus.QUEUED
    assert await postgres_pool.fetchval(
        """
        SELECT count(*)
        FROM workflow_events
        WHERE event_type = 'step_completed'
          AND payload->'released_step_ids' <> '[]'::jsonb
        """
    ) == 1


@pytest.mark.asyncio
async def test_start_rolls_back_when_full_dag_exceeds_tenant_budget(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    store = PostgresWorkflowStore(
        postgres_pool,
        tenant_outstanding_limit=2,
    )

    with pytest.raises(AdmissionRejected):
        await WorkflowService(store).start(
            PRINCIPAL,
            WorkflowStartCommand(
                idempotency_key="too-large",
                origin_turn_id="turn-1",
                definition_key="openpoke.parallel_demo",
                definition_version=1,
                input={"mode": "success", "duration_ms": 0},
            ),
        )

    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM workflow_instances"
    ) == 0
    assert await postgres_pool.fetchval("SELECT count(*) FROM execution_tasks") == 0


@pytest.mark.asyncio
async def test_stale_attempt_cannot_complete_step_or_release_downstream(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, started = await _started(postgres_pool)
    stale = await ledger.claim("worker-old", timedelta(milliseconds=1))
    assert stale is not None
    await asyncio.sleep(0.01)
    current = await ledger.claim("worker-current", timedelta(seconds=30))
    assert current is not None
    assert current.task_id == stale.task_id

    with pytest.raises(StaleLease):
        await ledger.complete(stale, {"response": "stale"})

    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM workflow_events WHERE event_type = 'step_completed'"
    ) == 0
    blocked = next(task for task in started.tasks if task.agent_name == "validate")
    assert (await ledger.get("tenant-a", blocked.task_id)).status is TaskStatus.BLOCKED

    await ledger.complete(current, {"response": "current"})
    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM workflow_events WHERE event_type = 'step_completed'"
    ) == 1


@pytest.mark.asyncio
async def test_terminal_step_failure_fails_workflow_without_releasing_dependents(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, started = await _started(postgres_pool)
    lease = await ledger.claim("worker-1", timedelta(seconds=30))
    assert lease is not None

    failed = await ledger.fail(
        lease,
        TaskFailure(code=FailureCode.SYNTHETIC_NON_RETRYABLE),
    )

    assert failed.status is TaskStatus.DEAD_LETTERED
    assert await postgres_pool.fetchval(
        "SELECT status FROM workflow_instances WHERE instance_id = $1",
        started.instance.instance_id,
    ) == "failed"
    assert await postgres_pool.fetchval(
        """
        SELECT status
        FROM workflow_steps
        WHERE execution_task_id = $1
        """,
        lease.task_id,
    ) == "failed"
    assert await postgres_pool.fetchval(
        """
        SELECT count(*)
        FROM execution_tasks AS task
        JOIN workflow_steps AS step
          ON step.execution_task_id = task.task_id
        WHERE step.instance_id = $1
          AND task.status IN ('blocked', 'queued', 'running')
        """,
        started.instance.instance_id,
    ) == 0
    assert await postgres_pool.fetchval(
        """
        SELECT count(*)
        FROM workflow_steps
        WHERE instance_id = $1 AND status <> 'failed'
        """,
        started.instance.instance_id,
    ) == 0

    replacement = await WorkflowService(
        PostgresWorkflowStore(
            postgres_pool,
            tenant_outstanding_limit=3,
        )
    ).start(
        PRINCIPAL,
        WorkflowStartCommand(
            idempotency_key="after-failure",
            origin_turn_id="turn-2",
            definition_key="openpoke.parallel_demo",
            definition_version=1,
            input={"mode": "success", "duration_ms": 0},
        ),
    )
    assert len(replacement.tasks) == 3


@pytest.mark.asyncio
async def test_running_sibling_can_finish_after_workflow_failure_without_release(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, started = await _started(postgres_pool)
    failed_lease = await ledger.claim("worker-1", timedelta(seconds=30))
    sibling_lease = await ledger.claim("worker-2", timedelta(seconds=30))
    assert failed_lease is not None
    assert sibling_lease is not None

    await ledger.fail(
        failed_lease,
        TaskFailure(code=FailureCode.SYNTHETIC_NON_RETRYABLE),
    )
    completed = await ledger.complete(
        sibling_lease,
        {"response": "late sibling result"},
    )

    assert completed.status is TaskStatus.COMPLETED
    assert await postgres_pool.fetchval(
        "SELECT status FROM workflow_instances WHERE instance_id = $1",
        started.instance.instance_id,
    ) == "failed"
    assert await postgres_pool.fetchval(
        """
        SELECT count(*)
        FROM workflow_events
        WHERE instance_id = $1
          AND event_type = 'step_completed_after_workflow_failure'
        """,
        started.instance.instance_id,
    ) == 1
    validate = next(
        task for task in started.tasks if task.agent_name == "validate"
    )
    assert (
        await ledger.get("tenant-a", validate.task_id)
    ).status is TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_expired_sibling_of_failed_workflow_does_not_poison_claims(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, _ = await _started(postgres_pool)
    failed_lease = await ledger.claim("worker-1", timedelta(seconds=30))
    abandoned = await ledger.claim("worker-2", timedelta(milliseconds=1))
    assert failed_lease is not None
    assert abandoned is not None

    await ledger.fail(
        failed_lease,
        TaskFailure(code=FailureCode.SYNTHETIC_NON_RETRYABLE),
    )
    await ledger.submit(
        PRINCIPAL,
        SubmitTask(
            idempotency_key="unrelated",
            origin_turn_id="turn-unrelated",
            agent_name="synthetic",
            executor_kind="synthetic",
            input={"mode": "success", "duration_ms": 0},
        ),
    )
    await asyncio.sleep(0.01)

    unrelated = await ledger.claim("worker-3", timedelta(seconds=30))

    assert unrelated is not None
    assert unrelated.agent_name == "synthetic"
    abandoned_record = await ledger.get("tenant-a", abandoned.task_id)
    assert abandoned_record is not None
    assert abandoned_record.status is TaskStatus.CANCELLED
    assert await postgres_pool.fetchval(
        """
        SELECT status
        FROM workflow_steps
        WHERE execution_task_id = $1
        """,
        abandoned.task_id,
    ) == "failed"


@pytest.mark.asyncio
async def test_claim_and_terminal_sibling_failure_use_one_lock_order(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, started = await _started(postgres_pool)
    queued_again = await ledger.claim("worker-1", timedelta(seconds=30))
    terminal = await ledger.claim("worker-2", timedelta(seconds=30))
    assert queued_again is not None
    assert terminal is not None
    await ledger.fail(
        queued_again,
        TaskFailure(code=FailureCode.SYNTHETIC_RETRYABLE),
    )
    await postgres_pool.execute(
        """
        CREATE FUNCTION pause_racing_claim() RETURNS trigger AS $$
        BEGIN
            IF NEW.lease_owner = 'racing-claim'
               AND OLD.status = 'queued'
               AND NEW.status = 'running' THEN
                PERFORM pg_sleep(0.1);
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER pause_racing_claim
        BEFORE UPDATE ON execution_tasks
        FOR EACH ROW EXECUTE FUNCTION pause_racing_claim();
        """
    )

    claim = asyncio.create_task(
        ledger.claim("racing-claim", timedelta(seconds=30))
    )
    await asyncio.sleep(0.02)
    failure = asyncio.create_task(
        ledger.fail(
            terminal,
            TaskFailure(code=FailureCode.SYNTHETIC_NON_RETRYABLE),
        )
    )
    claimed, failed = await asyncio.wait_for(
        asyncio.gather(claim, failure),
        timeout=2,
    )

    assert claimed is not None
    assert claimed.task_id == queued_again.task_id
    assert failed.status is TaskStatus.DEAD_LETTERED
    assert await postgres_pool.fetchval(
        "SELECT status FROM workflow_instances WHERE instance_id = $1",
        started.instance.instance_id,
    ) == "failed"


@pytest.mark.asyncio
async def test_blocked_and_queued_acceptance_are_not_idempotent_equivalents(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    admission = TaskAdmission()
    command = SubmitTask(
        idempotency_key="status-sensitive",
        origin_turn_id="turn-1",
        agent_name="synthetic",
        executor_kind="synthetic",
        input={"mode": "success", "duration_ms": 0},
    )
    async with postgres_pool.acquire() as connection:
        async with connection.transaction():
            await admission.accept(
                connection,
                PRINCIPAL,
                command,
                initial_status=TaskStatus.BLOCKED,
            )
            with pytest.raises(IdempotencyConflict):
                await admission.accept(
                    connection,
                    PRINCIPAL,
                    command,
                    initial_status=TaskStatus.QUEUED,
                )


def test_definition_rejects_cycles_unknown_edges_and_duplicate_step_keys() -> None:
    base = _parallel_definition().model_dump(mode="json", exclude_none=True)

    with pytest.raises(ValidationError, match="acyclic"):
        WorkflowDefinition.model_validate(
            {
                **base,
                "dependencies": [
                    {"step_key": "extract_a", "prerequisite_key": "validate"},
                    {"step_key": "validate", "prerequisite_key": "extract_a"},
                ],
            }
        )
    with pytest.raises(ValidationError, match="unknown"):
        WorkflowDefinition.model_validate(
            {
                **base,
                "dependencies": [
                    {"step_key": "missing", "prerequisite_key": "extract_a"}
                ],
            }
        )
    with pytest.raises(ValidationError, match="unique"):
        WorkflowDefinition.model_validate(
            {
                **base,
                "steps": [base["steps"][0], base["steps"][0]],
                "dependencies": [],
            }
        )
