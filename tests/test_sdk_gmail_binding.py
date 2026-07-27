from __future__ import annotations

from server.agents.execution_agent.tools import gmail
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
