from __future__ import annotations

import asyncio

import pytest

from server.agents.interaction_agent.tools import (
    InteractionToolContext,
    get_tool_schemas,
    send_message_to_agent,
)
from server.services.task_queue import (
    ExecutorKind,
    PostgresTaskLedger,
    Principal,
    TaskService,
)


@pytest.mark.asyncio
async def test_delegation_awaits_durable_tenant_owned_acceptance(
    ledger: PostgresTaskLedger,
) -> None:
    principal = Principal(
        actor_id="user-7",
        tenant_id="tenant-a",
        scopes=frozenset({"tasks:create"}),
    )
    context = InteractionToolContext(
        principal=principal,
        origin_turn_id="turn-client-stable-7",
        task_service=TaskService(ledger),
    )

    result = await send_message_to_agent(
        "Invoice Search Team",
        "find the latest invoice",
        context=context,
    )
    replay = await send_message_to_agent(
        "Invoice Search Team",
        "find the latest invoice",
        context=context,
    )

    assert result.success
    assert result.payload["status"] == "submitted"
    assert result.payload["task_id"] == replay.payload["task_id"]
    accepted = await ledger.get(
        "tenant-a",
        result.payload["task_id"],
    )
    assert accepted is not None
    assert accepted.actor_id == "user-7"
    assert accepted.origin_turn_id == "turn-client-stable-7"
    assert accepted.executor_kind is ExecutorKind.AGENT
    assert accepted.input == {
        "instructions": "find the latest invoice",
        "composio_user_id": None,
    }


@pytest.mark.asyncio
async def test_delegation_does_not_report_submitted_before_acceptance() -> None:
    release = asyncio.Event()
    started = asyncio.Event()

    class DelayedTaskService:
        async def submit(self, principal, command):
            started.set()
            await release.wait()
            return type("Accepted", (), {"task_id": "task-7"})()

    context = InteractionToolContext(
        principal=Principal(
            actor_id="user-7",
            tenant_id="tenant-a",
            scopes=frozenset({"tasks:create"}),
        ),
        origin_turn_id="turn-7",
        task_service=DelayedTaskService(),
    )

    submission = asyncio.create_task(
        send_message_to_agent(
            "invoice-search",
            "find invoices",
            context=context,
        )
    )
    await started.wait()

    assert not submission.done()
    release.set()
    result = await submission
    assert result.payload["status"] == "submitted"


def test_model_visible_delegation_schema_cannot_choose_executor_policy() -> None:
    delegation = next(
        schema["function"]
        for schema in get_tool_schemas()
        if schema["function"]["name"] == "send_message_to_agent"
    )

    assert set(delegation["parameters"]["properties"]) == {
        "agent_name",
        "instructions",
    }
