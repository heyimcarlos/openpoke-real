from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from server.agents.interaction_agent.agent import (
    build_system_prompt,
    prepare_message_with_history,
)
from server.agents.interaction_agent.runtime import (
    InteractionAgentRuntime,
    _LoopSummary,
)
from server.agents.interaction_agent.tools import (
    InteractionToolContext,
    delegate_execution,
    get_tool_schemas,
    handle_tool_call,
)
from server.services.execution import AgentRoster
from server.services.task_queue import (
    AdmissionRejected,
    ExecutorKind,
    PostgresTaskLedger,
    Principal,
    TaskService,
)

PRINCIPAL = Principal(
    actor_id="user-7",
    tenant_id="tenant-a",
    scopes=frozenset({"tasks:create"}),
)


@pytest.fixture
def isolated_roster(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> AgentRoster:
    roster = AgentRoster(tmp_path / "roster.json")
    monkeypatch.setattr(
        "server.agents.interaction_agent.tools.get_agent_roster",
        lambda: roster,
    )
    monkeypatch.setattr(
        "server.agents.interaction_agent.agent.get_agent_roster",
        lambda: roster,
    )
    return roster


def _tool_call(identifier: str, **arguments: str) -> dict:
    return {
        "id": identifier,
        "function": {
            "name": "delegate_execution",
            "arguments": json.dumps(arguments),
        },
    }


def _provider_response(*tool_calls: dict, content: str = "") -> dict:
    return {
        "choices": [
            {
                "message": {
                    "content": content,
                    "tool_calls": list(tool_calls),
                }
            }
        ]
    }


@pytest.mark.asyncio
async def test_delegation_awaits_durable_tenant_owned_acceptance(
    ledger: PostgresTaskLedger,
    isolated_roster: AgentRoster,
) -> None:
    context = InteractionToolContext(
        principal=PRINCIPAL,
        origin_turn_id="turn-client-stable-7",
        task_service=TaskService(ledger),
    )

    result = await delegate_execution(
        "Invoice Search Team",
        "find the latest invoice",
        context=context,
    )
    await delegate_execution(
        "Calendar",
        "find the next meeting",
        context=context,
    )
    replay = await delegate_execution(
        "Invoice Search Team",
        "find the latest invoice",
        context=context,
    )

    assert result.success
    assert result.payload["status"] == "submitted"
    assert result.payload["new_context_created"] is True
    assert result.payload["task_id"] == replay.payload["task_id"]
    assert replay.payload["new_context_created"] is False
    assert isolated_roster.get_agents() == ["Calendar", "Invoice Search Team"]
    accepted = await ledger.get("tenant-a", result.payload["task_id"])
    assert accepted is not None
    assert accepted.actor_id == "user-7"
    assert accepted.origin_turn_id == "turn-client-stable-7"
    assert accepted.executor_kind is ExecutorKind.AGENT
    assert accepted.input == {
        "instructions": "find the latest invoice",
        "composio_user_id": None,
    }


def test_prompt_roster_prefers_relevant_context_then_recent_contexts(
    isolated_roster: AgentRoster,
) -> None:
    for name in ["Alice travel", *[f"Context {index}" for index in range(25)]]:
        isolated_roster.add_agent(name)

    prompt = prepare_message_with_history(
        "What did Alice decide?",
        "",
    )[0]["content"]
    visible_names = [
        line for line in prompt.splitlines() if line.startswith('<agent name="')
    ]

    assert len(visible_names) == 20
    assert '<agent name="Alice travel" />' in visible_names
    assert '<agent name="Context 24" />' in visible_names
    assert '<agent name="Context 0" />' not in visible_names


def test_model_contract_describes_two_bounded_delegations() -> None:
    delegation = next(
        schema["function"]
        for schema in get_tool_schemas()
        if schema["function"]["name"] == "delegate_execution"
    )

    assert set(delegation["parameters"]["properties"]) == {
        "agent_name",
        "instructions",
    }
    assert "logical context" in delegation["description"]
    assert "process" in delegation["description"]
    prompt = build_system_prompt()
    assert "at most two durable delegations per interaction turn" in prompt
    assert "complete objective" in prompt
    assert "parallel as much as possible" not in prompt
    assert "send_message_to_agent" not in prompt


def test_wait_control_message_is_not_exposed_as_a_reply() -> None:
    runtime = object.__new__(InteractionAgentRuntime)
    summary = _LoopSummary(
        last_assistant_text="<wait>User already received this</wait>",
        recorded_reply=True,
    )

    assert runtime._finalize_response(summary) == ""


@pytest.mark.asyncio
async def test_dispatcher_does_not_update_roster_when_admission_fails(
    isolated_roster: AgentRoster,
) -> None:
    class RejectingTaskService:
        async def submit(self, _principal, _command):
            raise AdmissionRejected(retry_after_seconds=10)

    result = await handle_tool_call(
        "delegate_execution",
        {
            "agent_name": "invoice-search",
            "instructions": "find invoices",
        },
        context=InteractionToolContext(
            principal=PRINCIPAL,
            origin_turn_id="turn-7",
            task_service=RejectingTaskService(),
        ),
    )

    assert not result.success
    assert result.payload == {
        "error": "Execution backlog is full",
        "retry_after_seconds": 10,
    }
    assert isolated_roster.get_agents() == []


@pytest.mark.asyncio
async def test_interaction_turn_accepts_two_delegations_and_rejects_third(
    ledger: PostgresTaskLedger,
    monkeypatch: pytest.MonkeyPatch,
    isolated_roster: AgentRoster,
) -> None:
    class CountingTaskService:
        def __init__(self) -> None:
            self.submissions = 0
            self.service = TaskService(ledger)

        async def submit(self, principal, command):
            self.submissions += 1
            return await self.service.submit(principal, command)

    responses = [
        _provider_response(
            _tool_call(
                "first",
                agent_name="invoices",
                instructions="find the invoice",
            ),
            _tool_call(
                "second",
                agent_name="calendar",
                instructions="book a follow-up",
            ),
            _tool_call(
                "third",
                agent_name="contacts",
                instructions="find Alice's phone number",
            ),
        ),
        _provider_response(content="I started that."),
        _provider_response(
            _tool_call(
                "next-turn",
                agent_name="calendar",
                instructions="book a follow-up",
            )
        ),
        _provider_response(content="I started that."),
    ]
    provider_messages: list[list[dict]] = []

    async def fake_request_chat_completion(**kwargs):
        provider_messages.append(kwargs["messages"])
        return responses.pop(0)

    log = SimpleNamespace(
        record_user_message=lambda _message: None,
        record_reply=lambda _message: None,
        load_transcript=lambda: "",
    )
    monkeypatch.setattr(
        "server.agents.interaction_agent.runtime.get_settings",
        lambda: SimpleNamespace(
            openrouter_api_key="test-only",
            interaction_agent_model="test-model",
            summarization_enabled=False,
        ),
    )
    for dependency in ("get_conversation_log", "get_working_memory_log"):
        monkeypatch.setattr(
            f"server.agents.interaction_agent.runtime.{dependency}",
            lambda: log,
        )
    monkeypatch.setattr(
        "server.agents.interaction_agent.runtime.request_chat_completion",
        fake_request_chat_completion,
    )

    task_service = CountingTaskService()
    runtime = InteractionAgentRuntime(
        tool_context=InteractionToolContext(
            principal=PRINCIPAL,
            origin_turn_id="turn-7",
            task_service=task_service,
        )
    )

    result = await runtime.execute("Find the invoice and book a follow-up")

    assert result.success
    assert result.execution_agents_used == 2
    assert task_service.submissions == 2
    tool_results = [
        json.loads(message["content"])
        for message in provider_messages[1]
        if message["role"] == "tool"
    ]
    assert tool_results[0]["status"] == "success"
    assert tool_results[1]["status"] == "success"
    assert tool_results[2]["status"] == "error"
    assert (
        tool_results[2]["error"]["error"]
        == "At most two execution delegations are allowed per interaction turn"
    )

    next_result = await runtime.execute("Now book the follow-up")

    assert next_result.success
    assert task_service.submissions == 3
