from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pytest

import server.services.gmail as gmail_service
from server.agents.execution_agent.tasks.search_email import tool as email_search
from server.agents.execution_agent.tools import gmail
from server.agents.execution_agent.tools.registry import get_tool_registry
from server.agents.execution_agent.agent import execution_storage_key


def test_gmail_tool_uses_only_task_bound_composio_identity(
    monkeypatch,
) -> None:
    calls = []

    def fake_execute(tool_name, user_id, *, arguments):
        calls.append((tool_name, user_id, arguments))
        return {"ok": True}

    monkeypatch.setattr(gmail, "execute_gmail_tool", fake_execute)
    monkeypatch.setattr(
        gmail,
        "get_active_gmail_user_id",
        lambda: "wrong-process-global-user",
    )

    bound = gmail.build_registry("agent-key", "bound-user")
    result = bound["gmail_create_draft"](
        recipient_email="recipient@example.com",
        subject="subject",
        body="body",
    )
    unbound = gmail.build_registry("agent-key", None)
    rejected = unbound["gmail_create_draft"](
        recipient_email="recipient@example.com",
        subject="subject",
        body="body",
    )

    assert result == {"ok": True}
    assert calls[0][1] == "bound-user"
    assert rejected == {
        "error": "Gmail not connected. Please connect Gmail in settings first."
    }
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_nested_email_search_uses_only_task_bound_composio_identity(
    monkeypatch,
) -> None:
    identities = []

    async def fake_run_email_search(*, composio_user_id, **_kwargs):
        identities.append(composio_user_id)
        return []

    monkeypatch.setattr(
        gmail_service,
        "get_active_gmail_user_id",
        lambda: pytest.fail("process-global Gmail identity was used"),
    )
    monkeypatch.setattr(email_search, "_run_email_search", fake_run_email_search)
    monkeypatch.setattr(
        email_search,
        "_validate_openrouter_config",
        lambda: ("test-api-key", "test-model"),
    )

    bound = get_tool_registry(
        "agent-key",
        "bound-user",
    )["task_email_search"]
    unbound = get_tool_registry(
        "agent-key",
        None,
    )["task_email_search"]

    assert await bound(search_query="find invoices") == []
    assert await unbound(search_query="find invoices") == {
        "error": "Gmail not connected. Please connect Gmail in settings first."
    }
    assert identities == ["bound-user"]


@pytest.mark.asyncio
async def test_nested_email_search_does_not_log_search_text(
    monkeypatch,
    caplog,
) -> None:
    source_query = "confidential acquisition invoices"
    generated_query = "from:legal acquisition invoice"
    provider_responses = iter(
        [
            {
                "choices": [
                    {
                        "message": {
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "search-1",
                                    "function": {
                                        "name": "gmail_fetch_emails",
                                        "arguments": json.dumps(
                                            {"query": generated_query}
                                        ),
                                    },
                                }
                            ],
                        }
                    }
                ]
            },
            {
                "choices": [
                    {
                        "message": {
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "complete-1",
                                    "function": {
                                        "name": "return_search_results",
                                        "arguments": json.dumps(
                                            {"message_ids": []}
                                        ),
                                    },
                                }
                            ],
                        }
                    }
                ]
            },
        ]
    )
    action_descriptions = []

    async def fake_request_chat_completion(**_kwargs):
        return next(provider_responses)

    def fake_execute_gmail_tool(_tool_name, user_id, *, arguments):
        assert user_id == "bound-user"
        assert arguments["query"] == generated_query
        return {"data": {"messages": []}}

    class FakeLogStore:
        def record_action(self, _agent_name, description):
            action_descriptions.append(description)

    monkeypatch.setattr(
        email_search,
        "get_settings",
        lambda: SimpleNamespace(
            openrouter_api_key="test-api-key",
            execution_agent_search_model="test-model",
        ),
    )
    monkeypatch.setattr(
        email_search,
        "request_chat_completion",
        fake_request_chat_completion,
    )
    monkeypatch.setattr(
        email_search,
        "execute_gmail_tool",
        fake_execute_gmail_tool,
    )
    monkeypatch.setattr(email_search, "_LOG_STORE", FakeLogStore())
    caplog.set_level(logging.INFO, logger="openpoke.server")

    search = get_tool_registry(
        "agent-key",
        "bound-user",
    )["task_email_search"]
    assert await search(search_query=source_query) == []

    emitted = "\n".join(
        [record.getMessage() for record in caplog.records]
        + action_descriptions
    )
    assert source_query not in emitted
    assert generated_query not in emitted


def test_execution_history_key_is_tenant_and_actor_scoped() -> None:
    first = execution_storage_key("tenant-a", "user-1", "Invoice Agent")
    other_tenant = execution_storage_key(
        "tenant-b",
        "user-1",
        "Invoice Agent",
    )
    other_actor = execution_storage_key(
        "tenant-a",
        "user-2",
        "Invoice Agent",
    )

    assert len({first, other_tenant, other_actor}) == 3
    assert first.startswith("agent-")
