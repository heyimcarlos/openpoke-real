from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from datetime import timedelta

import asyncpg
import pytest
from pydantic import ValidationError

from server.agents.interaction_agent.tools import (
    InteractionToolContext,
    get_tool_schemas,
    handle_tool_call,
)
from server.services.task_queue import ExecutorKind, PostgresTaskLedger, Principal
from server.services.threads import PostgresThreadLedger
from server.services.workflows import (
    FieldContract,
    FieldType,
    MissingWorkflowScope,
    PostgresWorkflowStore,
    SignalAlreadySatisfied,
    SignalIdempotencyConflict,
    SignalTooEarly,
    StepTemplate,
    WaitPrerequisite,
    WaitRoute,
    WaitTemplate,
    WaitNotFound,
    WorkflowDefinition,
    WorkflowDefinitionRegistry,
    WorkflowService,
    WorkflowSignalCommand,
    WorkflowStartCommand,
)


PRINCIPAL = Principal(
    actor_id="approver-1",
    tenant_id="tenant-a",
    scopes=frozenset(
        {
            "workflows:publish",
            "workflows:start",
            "workflows:read",
            "workflows:signal",
        }
    ),
)


def _approval_definition() -> WorkflowDefinition:
    return WorkflowDefinition(
        key="openpoke.approval_test",
        version=1,
        input_contract=(
            FieldContract(name="subject", value_type=FieldType.STRING),
        ),
        steps=(
            StepTemplate(
                key="prepare",
                agent_name="prepare",
                executor_kind=ExecutorKind.SYNTHETIC,
            ),
            StepTemplate(
                key="send",
                agent_name="send",
                executor_kind=ExecutorKind.SYNTHETIC,
            ),
        ),
        waits=(
            WaitTemplate(
                key="approval",
                signal_key="approve",
                input_contract=(
                    FieldContract(
                        name="approved",
                        value_type=FieldType.BOOLEAN,
                    ),
                ),
            ),
        ),
        wait_prerequisites=(
            WaitPrerequisite(
                wait_key="approval",
                prerequisite_step_key="prepare",
            ),
        ),
        wait_routes=(WaitRoute(wait_key="approval", step_key="send"),),
    )


async def _started(pool: asyncpg.Pool):
    ledger = PostgresTaskLedger(pool)
    await ledger.migrate()
    store = PostgresWorkflowStore(pool)
    await WorkflowDefinitionRegistry(store).publish(
        PRINCIPAL,
        _approval_definition(),
    )
    started = await WorkflowService(store).start(
        PRINCIPAL,
        WorkflowStartCommand(
            idempotency_key="approval-1",
            origin_turn_id="turn-1",
            definition_key="openpoke.approval_test",
            definition_version=1,
            input={"subject": "Send the update"},
        ),
    )
    return ledger, started


@pytest.mark.asyncio
async def test_step_completion_opens_wait_without_worker_lease(
    postgres_pool: asyncpg.Pool,
    database_schema: str,
) -> None:
    ledger, started = await _started(postgres_pool)
    wait_target = started.wait_targets[0]

    with pytest.raises(SignalTooEarly):
        await WorkflowService(PostgresWorkflowStore(postgres_pool)).signal(
            PRINCIPAL,
            WorkflowSignalCommand(
                idempotency_key="early",
                wait_id=wait_target.wait_id,
                signal_key="approve",
                input={"approved": True},
            ),
        )

    lease = await ledger.claim("worker-1", timedelta(seconds=30))
    assert lease is not None
    assert lease.agent_name == "prepare"
    await ledger.complete(lease, {"response": "prepared"})

    restarted_pool = await asyncpg.create_pool(
        os.getenv(
            "OPENPOKE_TEST_DATABASE_URL",
            "postgresql://postgres@127.0.0.1:55432/openpoke_test",
        ),
        min_size=1,
        max_size=2,
        server_settings={"search_path": database_schema},
    )
    try:
        wait = await WorkflowService(
            PostgresWorkflowStore(restarted_pool)
        ).get_wait(PRINCIPAL, wait_target.wait_id)
    finally:
        await restarted_pool.close()
    assert wait is not None
    assert wait.status.value == "open"
    assert await ledger.claim("worker-2", timedelta(seconds=30)) is None
    assert await postgres_pool.fetchval(
        """
        SELECT count(*)
        FROM execution_tasks AS task
        JOIN workflow_steps AS step
          ON step.execution_task_id = task.task_id
        WHERE step.instance_id = $1 AND task.status = 'running'
        """,
        started.instance.instance_id,
    ) == 0


@pytest.mark.asyncio
async def test_exact_signal_releases_only_published_route_and_replays(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, started = await _started(postgres_pool)
    prepare = await ledger.claim("worker-1", timedelta(seconds=30))
    assert prepare is not None
    await ledger.complete(prepare, {"response": "prepared"})
    wait_id = started.wait_targets[0].wait_id
    command = WorkflowSignalCommand(
        idempotency_key="approval-signal-1",
        wait_id=wait_id,
        signal_key="approve",
        input={"approved": True},
    )

    accepted = await WorkflowService(
        PostgresWorkflowStore(postgres_pool)
    ).signal(PRINCIPAL, command)
    replayed = await WorkflowService(
        PostgresWorkflowStore(postgres_pool)
    ).signal(PRINCIPAL, command)

    assert replayed == accepted
    assert accepted.wait.status.value == "satisfied"
    assert accepted.wait.satisfied_by_signal_id == accepted.signal_id
    assert len(accepted.released_step_ids) == 1
    assert await postgres_pool.fetchval(
        """
        SELECT count(*)
        FROM task_wake_outbox AS wake
        JOIN workflow_steps AS step
          ON step.execution_task_id = wake.task_id
        WHERE step.step_id = $1
          AND wake.source_transition = 'signal_released'
        """,
        accepted.released_step_ids[0],
    ) == 1
    send = await ledger.claim("worker-2", timedelta(seconds=30))
    assert send is not None
    assert send.agent_name == "send"
    await ledger.complete(send, {"response": "sent"})
    terminal_replay = await WorkflowService(
        PostgresWorkflowStore(postgres_pool)
    ).signal(PRINCIPAL, command)
    assert terminal_replay == accepted
    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM workflow_signals"
    ) == 1


@pytest.mark.asyncio
async def test_signal_conflict_wrong_input_and_cross_tenant_are_rejected(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, started = await _started(postgres_pool)
    prepare = await ledger.claim("worker-1", timedelta(seconds=30))
    assert prepare is not None
    await ledger.complete(prepare, {"response": "prepared"})
    wait_id = started.wait_targets[0].wait_id
    service = WorkflowService(PostgresWorkflowStore(postgres_pool))
    command = WorkflowSignalCommand(
        idempotency_key="same-signal",
        wait_id=wait_id,
        signal_key="approve",
        input={"approved": True},
    )
    await service.signal(PRINCIPAL, command)

    with pytest.raises(SignalIdempotencyConflict):
        await service.signal(
            PRINCIPAL,
            command.model_copy(update={"input": {"approved": False}}),
        )
    with pytest.raises(SignalAlreadySatisfied):
        await service.signal(
            PRINCIPAL,
            command.model_copy(update={"idempotency_key": "second-signal"}),
        )
    with pytest.raises(LookupError):
        await service.signal(
            Principal(
                actor_id="other",
                tenant_id="tenant-b",
                scopes=frozenset({"workflows:signal"}),
            ),
            command.model_copy(update={"idempotency_key": "cross-tenant"}),
        )


@pytest.mark.asyncio
async def test_signal_requires_owner_scope_and_exact_published_contract(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, started = await _started(postgres_pool)
    prepare = await ledger.claim("worker-1", timedelta(seconds=30))
    assert prepare is not None
    await ledger.complete(prepare, {"response": "prepared"})
    wait_id = started.wait_targets[0].wait_id
    service = WorkflowService(PostgresWorkflowStore(postgres_pool))
    command = WorkflowSignalCommand(
        idempotency_key="signal-auth",
        wait_id=wait_id,
        signal_key="approve",
        input={"approved": True},
    )

    with pytest.raises(MissingWorkflowScope):
        await service.signal(
            Principal(
                actor_id=PRINCIPAL.actor_id,
                tenant_id=PRINCIPAL.tenant_id,
                scopes=frozenset(),
            ),
            command,
        )
    with pytest.raises(WaitNotFound):
        await service.signal(
            Principal(
                actor_id="other-actor",
                tenant_id=PRINCIPAL.tenant_id,
                scopes=frozenset({"workflows:signal"}),
            ),
            command,
        )
    with pytest.raises(WaitNotFound):
        await service.signal(
            PRINCIPAL,
            command.model_copy(update={"signal_key": "reject"}),
        )
    with pytest.raises(ValueError, match="Signal input"):
        await service.signal(
            PRINCIPAL,
            command.model_copy(update={"input": {"approved": "yes"}}),
        )


@pytest.mark.asyncio
async def test_concurrent_duplicate_signals_converge_on_one_transition(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger, started = await _started(postgres_pool)
    prepare = await ledger.claim("worker-1", timedelta(seconds=30))
    assert prepare is not None
    await ledger.complete(prepare, {"response": "prepared"})
    command = WorkflowSignalCommand(
        idempotency_key="concurrent-signal",
        wait_id=started.wait_targets[0].wait_id,
        signal_key="approve",
        input={"approved": True},
    )

    results = await asyncio.gather(
        *(
            WorkflowService(PostgresWorkflowStore(postgres_pool)).signal(
                PRINCIPAL,
                command,
            )
            for _ in range(8)
        )
    )

    assert len({item.signal_id for item in results}) == 1
    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM workflow_signals"
    ) == 1
    assert await postgres_pool.fetchval(
        """
        SELECT count(*)
        FROM workflow_events
        WHERE event_type = 'wait_satisfied'
        """
    ) == 1


def test_signal_tool_does_not_expose_identity_or_route_authority() -> None:
    schema = next(
        item["function"]
        for item in get_tool_schemas()
        if item["function"]["name"] == "signal_workflow_wait"
    )

    assert set(schema["parameters"]["properties"]) == {
        "wait_id",
        "signal_key",
        "input",
    }
    assert schema["parameters"]["additionalProperties"] is False


@pytest.mark.asyncio
async def test_seeded_approval_demo_is_reproducible_through_interaction_tools(
    postgres_pool: asyncpg.Pool,
) -> None:
    await PostgresTaskLedger(postgres_pool).migrate()
    context = InteractionToolContext(
        principal=PRINCIPAL,
        origin_turn_id="approval-message-1",
        task_service=object(),
        workflow_service=WorkflowService(
            PostgresWorkflowStore(postgres_pool)
        ),
    )

    started = await handle_tool_call(
        "start_workflow",
        {
            "definition_key": "openpoke.approval_demo",
            "definition_version": 1,
            "input": {"mode": "success", "duration_ms": 0},
        },
        context=context,
    )
    wait = started.payload["waits"][0]
    signalled = await handle_tool_call(
        "signal_workflow_wait",
        {
            "wait_id": wait["wait_id"],
            "signal_key": wait["signal_key"],
            "input": {"approval_note": "Approved in browser QA"},
        },
        context=InteractionToolContext(
            principal=PRINCIPAL,
            origin_turn_id="approval-message-2",
            task_service=object(),
            workflow_service=WorkflowService(
                PostgresWorkflowStore(postgres_pool)
            ),
        ),
    )
    replayed_start = await handle_tool_call(
        "start_workflow",
        {
            "definition_key": "openpoke.approval_demo",
            "definition_version": 1,
            "input": {"mode": "success", "duration_ms": 0},
        },
        context=context,
    )

    assert started.success
    assert wait["status"] == "open"
    assert signalled.success
    assert replayed_start.payload["waits"][0]["status"] == "satisfied"
    assert len(signalled.payload["released_step_ids"]) == 1
    claim = await PostgresTaskLedger(postgres_pool).claim(
        "worker-1",
        timedelta(seconds=30),
    )
    assert claim is not None
    assert claim.agent_name == "approved-action"
    await PostgresTaskLedger(postgres_pool).complete(
        claim,
        {"response": "approved action completed"},
    )
    assert await postgres_pool.fetchval(
        "SELECT status FROM workflow_instances WHERE instance_id = $1::uuid",
        started.payload["instance_id"],
    ) == "completed"


@pytest.mark.asyncio
async def test_agent_signal_requires_a_later_authenticated_user_turn(
    postgres_pool: asyncpg.Pool,
) -> None:
    await PostgresTaskLedger(postgres_pool).migrate()
    threads = PostgresThreadLedger(postgres_pool)
    await threads.append_message(
        PRINCIPAL,
        message_id="approval-request",
        content="Start the approval workflow",
    )
    first_run = await threads.claim_run(
        "orchestrator-1",
        timedelta(seconds=30),
    )
    assert first_run is not None
    service = WorkflowService(
        PostgresWorkflowStore(
            postgres_pool,
            run_authority=threads,
        )
    )
    started = await service.start_for_run(
        PRINCIPAL,
        WorkflowStartCommand(
            idempotency_key="run-owned-approval",
            origin_turn_id="approval-request",
            definition_key="openpoke.approval_demo",
            definition_version=1,
            input={"mode": "success", "duration_ms": 0},
        ),
        first_run,
    )
    command = WorkflowSignalCommand(
        idempotency_key="user-approval",
        wait_id=started.wait_targets[0].wait_id,
        signal_key="approve",
        input={"approval_note": "Approved"},
    )

    with pytest.raises(SignalTooEarly, match="later authenticated user turn"):
        await service.signal_for_run(PRINCIPAL, command, first_run)
    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM workflow_signals"
    ) == 0
    assert await postgres_pool.fetchval(
        "SELECT delegation_count FROM agent_runs WHERE run_id = $1",
        first_run.run_id,
    ) == 1
    assert await postgres_pool.fetchval(
        """
        SELECT task.status
        FROM execution_tasks AS task
        JOIN workflow_steps AS step
          ON step.execution_task_id = task.task_id
        WHERE step.instance_id = $1
        """,
        started.instance.instance_id,
    ) == "blocked"

    await threads.complete_run(first_run, response="Awaiting approval.")
    await threads.append_message(
        PRINCIPAL,
        message_id="approval-response",
        content="I approve.",
    )
    later_run = await threads.claim_run(
        "orchestrator-2",
        timedelta(seconds=30),
    )
    assert later_run is not None
    with pytest.raises(SignalTooEarly, match="later authenticated user turn"):
        await service.signal_for_run(
            PRINCIPAL,
            command,
            replace(later_run, input_source_kind="execution_result"),
        )
    accepted = await service.signal_for_run(
        PRINCIPAL,
        command,
        later_run,
    )

    assert accepted.wait.status.value == "satisfied"
    cause = await postgres_pool.fetchrow(
        """
        SELECT origin_thread_id, origin_agent_run_id
        FROM workflow_signals
        WHERE signal_id = $1
        """,
        accepted.signal_id,
    )
    assert cause["origin_thread_id"] == later_run.thread_id
    assert cause["origin_agent_run_id"] == later_run.run_id


def test_definition_and_signal_commands_cannot_invent_wait_routes() -> None:
    cyclic = _approval_definition().model_dump(mode="json")
    cyclic["wait_routes"] = [
        {"wait_key": "approval", "step_key": "prepare"}
    ]

    with pytest.raises(ValidationError, match="acyclic"):
        WorkflowDefinition.model_validate(cyclic)
    with pytest.raises(ValidationError):
        WorkflowSignalCommand.model_validate(
            {
                "idempotency_key": "signal-1",
                "wait_id": "16b7ef80-c84b-4442-a3d7-e16cf1f7ca03",
                "signal_key": "approve",
                "input": {"approved": True},
                "route": "invented",
            }
        )
