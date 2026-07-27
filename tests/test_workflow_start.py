from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest
from pydantic import ValidationError

from server.agents.interaction_agent.tools import (
    InteractionToolContext,
    get_tool_schemas,
    handle_tool_call,
)
from server.services.task_queue import (
    AdmissionRejected,
    ExecutorKind,
    PostgresTaskLedger,
    Principal,
)
from server.services.threads import PostgresThreadLedger, StaleAgentRunLease
from server.services.workflows import (
    DefinitionConflict,
    FieldContract,
    FieldType,
    MissingWorkflowScope,
    PostgresWorkflowStore,
    StepTemplate,
    WorkflowDefinition,
    WorkflowDefinitionRegistry,
    WorkflowIdempotencyConflict,
    WorkflowService,
    WorkflowStartCommand,
)


def _principal(
    *,
    tenant_id: str = "tenant-a",
    scopes: frozenset[str] = frozenset(
        {"workflows:publish", "workflows:start", "workflows:read"}
    ),
) -> Principal:
    return Principal(
        actor_id="user-7",
        tenant_id=tenant_id,
        scopes=scopes,
    )


def _definition() -> WorkflowDefinition:
    return WorkflowDefinition(
        key="openpoke.email_search",
        version=1,
        input_contract=(
            FieldContract(name="query", value_type=FieldType.STRING),
        ),
        entry_step=StepTemplate(
            key="search",
            agent_name="email-search",
            executor_kind=ExecutorKind.SYNTHETIC,
        ),
    )


async def _services(
    pool: asyncpg.Pool,
    *,
    tenant_outstanding_limit: int = 50,
) -> tuple[WorkflowDefinitionRegistry, WorkflowService]:
    task_ledger = PostgresTaskLedger(
        pool,
        tenant_outstanding_limit=tenant_outstanding_limit,
    )
    await task_ledger.migrate()
    store = PostgresWorkflowStore(
        pool,
        tenant_outstanding_limit=tenant_outstanding_limit,
        run_authority=PostgresThreadLedger(pool),
    )
    return WorkflowDefinitionRegistry(store), WorkflowService(store)


@pytest.mark.asyncio
async def test_publish_and_start_create_one_version_pinned_durable_task(
    postgres_pool: asyncpg.Pool,
) -> None:
    registry, service = await _services(postgres_pool)
    definition = await registry.publish(_principal(), _definition())
    command = WorkflowStartCommand(
        idempotency_key="message-42:email-search",
        origin_turn_id="message-42",
        definition_key=definition.key,
        definition_version=definition.version,
        input={"query": "latest invoice"},
    )

    started = await service.start(_principal(), command)
    restarted_service = WorkflowService(PostgresWorkflowStore(postgres_pool))
    replay = await restarted_service.start(_principal(), command)

    assert replay == started
    assert started.instance.definition_key == "openpoke.email_search"
    assert started.instance.definition_version == 1
    assert started.step.key == "search"
    assert started.step.execution_task_id == started.task.task_id
    assert started.task.status.value == "queued"
    assert started.task.executor_kind is ExecutorKind.SYNTHETIC
    assert started.task.input == {"query": "latest invoice"}
    assert await service.get(_principal(), started.instance.instance_id) == (
        started.instance
    )

    event = await postgres_pool.fetchrow(
        """
        SELECT sequence, event_type, payload
        FROM workflow_events
        WHERE instance_id = $1
        """,
        started.instance.instance_id,
    )
    assert event["sequence"] == 1
    assert event["event_type"] == "workflow_started"
    assert json.loads(event["payload"])["task_id"] == str(started.task.task_id)
    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM workflow_events WHERE instance_id = $1",
        started.instance.instance_id,
    ) == 1


@pytest.mark.asyncio
async def test_concurrent_start_replays_converge_on_one_instance(
    postgres_pool: asyncpg.Pool,
) -> None:
    registry, service = await _services(postgres_pool)
    await registry.publish(_principal(), _definition())
    command = WorkflowStartCommand(
        idempotency_key="same-command",
        origin_turn_id="message-1",
        definition_key="openpoke.email_search",
        definition_version=1,
        input={"query": "invoice"},
    )

    results = await asyncio.gather(
        *(service.start(_principal(), command) for _ in range(8))
    )

    assert len({result.instance.instance_id for result in results}) == 1
    assert await postgres_pool.fetchval("SELECT count(*) FROM workflow_instances") == 1
    assert await postgres_pool.fetchval("SELECT count(*) FROM workflow_steps") == 1


@pytest.mark.asyncio
async def test_start_rejects_conflicts_invalid_input_and_cross_tenant_reads(
    postgres_pool: asyncpg.Pool,
) -> None:
    registry, service = await _services(postgres_pool)
    await registry.publish(_principal(), _definition())
    command = WorkflowStartCommand(
        idempotency_key="same-command",
        origin_turn_id="message-1",
        definition_key="openpoke.email_search",
        definition_version=1,
        input={"query": "invoice"},
    )
    started = await service.start(_principal(), command)

    with pytest.raises(WorkflowIdempotencyConflict):
        await service.start(
            _principal(),
            command.model_copy(update={"input": {"query": "receipt"}}),
        )
    with pytest.raises(ValueError, match="input fields"):
        await service.start(
            _principal(),
            command.model_copy(
                update={
                    "idempotency_key": "invalid-input",
                    "input": {"invented_step": "send_email"},
                }
            ),
        )
    assert (
        await service.get(
            _principal(tenant_id="tenant-b"),
            started.instance.instance_id,
        )
        is None
    )


@pytest.mark.asyncio
async def test_definition_identity_is_immutable_and_start_rolls_back_on_admission(
    postgres_pool: asyncpg.Pool,
) -> None:
    registry, service = await _services(
        postgres_pool,
        tenant_outstanding_limit=1,
    )
    await registry.publish(_principal(), _definition())
    await service.start(
        _principal(),
        WorkflowStartCommand(
            idempotency_key="first",
            origin_turn_id="message-1",
            definition_key="openpoke.email_search",
            definition_version=1,
            input={"query": "first"},
        ),
    )
    changed = _definition().model_copy(
        update={
            "entry_step": StepTemplate(
                key="search",
                agent_name="changed",
                executor_kind=ExecutorKind.AGENT,
            )
        }
    )

    with pytest.raises(DefinitionConflict):
        await registry.publish(_principal(), changed)
    with pytest.raises(AdmissionRejected):
        await service.start(
            _principal(),
            WorkflowStartCommand(
                idempotency_key="rejected",
                origin_turn_id="message-2",
                definition_key="openpoke.email_search",
                definition_version=1,
                input={"query": "second"},
            ),
        )

    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM workflow_instances WHERE idempotency_key = 'rejected'"
    ) == 0


def test_commands_and_definitions_fail_closed() -> None:
    with pytest.raises(ValidationError):
        WorkflowStartCommand.model_validate(
            {
                "idempotency_key": "bad",
                "origin_turn_id": "message-1",
                "definition_key": "openpoke.email_search",
                "definition_version": 1,
                "input": {},
                "steps": [{"key": "invented"}],
            }
        )
    with pytest.raises(ValidationError):
        WorkflowDefinition.model_validate(
            {
                **_definition().model_dump(),
                "routes": [{"to": "invented"}],
            }
        )


@pytest.mark.asyncio
async def test_workflow_service_enforces_scopes(
    postgres_pool: asyncpg.Pool,
) -> None:
    registry, service = await _services(postgres_pool)
    unauthorized = _principal(scopes=frozenset())

    with pytest.raises(MissingWorkflowScope):
        await registry.publish(unauthorized, _definition())
    with pytest.raises(MissingWorkflowScope):
        await service.start(
            unauthorized,
            WorkflowStartCommand(
                idempotency_key="missing-scope",
                origin_turn_id="message-1",
                definition_key="openpoke.email_search",
                definition_version=1,
                input={"query": "invoice"},
            ),
        )


@pytest.mark.asyncio
async def test_interaction_tool_selects_definition_but_cannot_supply_structure(
    postgres_pool: asyncpg.Pool,
) -> None:
    registry, service = await _services(postgres_pool)
    await registry.publish(_principal(), _definition())
    thread_ledger = PostgresThreadLedger(postgres_pool)
    await thread_ledger.append_message(
        _principal(),
        message_id="message-7",
        content="find my invoice",
    )
    lease = await thread_ledger.claim_run(
        "orchestrator-1",
        timedelta(seconds=30),
    )
    assert lease is not None
    context = InteractionToolContext(
        principal=_principal(),
        origin_turn_id="message-7",
        task_service=object(),
        workflow_service=service,
        thread_ledger=thread_ledger,
        run_lease=lease,
    )

    result = await handle_tool_call(
        "start_workflow",
        {
            "definition_key": "openpoke.email_search",
            "definition_version": 1,
            "input": {"query": "invoice"},
        },
        context=context,
    )
    schema = next(
        item["function"]
        for item in get_tool_schemas()
        if item["function"]["name"] == "start_workflow"
    )

    assert result.success
    assert result.payload["status"] == "started"
    assert set(schema["parameters"]["properties"]) == {
        "definition_key",
        "definition_version",
        "input",
    }
    assert "steps" not in schema["parameters"]["properties"]
    task = await PostgresTaskLedger(postgres_pool).get(
        "tenant-a",
        UUID(result.payload["task_id"]),
    )
    assert task is not None
    assert task.origin_thread_id == lease.thread_id
    assert task.origin_agent_run_id == lease.run_id
    assert await postgres_pool.fetchval(
        "SELECT delegation_count FROM agent_runs WHERE run_id = $1",
        lease.run_id,
    ) == 1

    await postgres_pool.execute(
        """
        UPDATE agent_runs
        SET lease_expires_at = clock_timestamp() - interval '1 second'
        WHERE run_id = $1
        """,
        lease.run_id,
    )
    with pytest.raises(StaleAgentRunLease):
        await service.start_for_run(
            _principal(),
            WorkflowStartCommand(
                idempotency_key="stale-run-start",
                origin_turn_id="message-7",
                definition_key="openpoke.email_search",
                definition_version=1,
                input={"query": "other invoice"},
            ),
            lease,
        )
    assert await postgres_pool.fetchval(
        """
        SELECT count(*)
        FROM workflow_instances
        WHERE idempotency_key = 'stale-run-start'
        """
    ) == 0
