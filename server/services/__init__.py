"""Service-layer public exports, loaded only when requested."""

from __future__ import annotations

from importlib import import_module
from typing import Any


_EXPORTS = {
    "AgentRoster": (".execution", "AgentRoster"),
    "ConversationLog": (".conversation", "ConversationLog"),
    "ExecutionAgentLogStore": (".execution", "ExecutionAgentLogStore"),
    "GmailSeenStore": (".gmail", "GmailSeenStore"),
    "ImportantEmailWatcher": (".gmail", "ImportantEmailWatcher"),
    "SummaryState": (".conversation", "SummaryState"),
    "TimezoneStore": (".timezone_store", "TimezoneStore"),
    "classify_email_importance": (".gmail", "classify_email_importance"),
    "disconnect_account": (".gmail", "disconnect_account"),
    "execute_gmail_tool": (".gmail", "execute_gmail_tool"),
    "fetch_status": (".gmail", "fetch_status"),
    "get_active_gmail_user_id": (".gmail", "get_active_gmail_user_id"),
    "get_agent_roster": (".execution", "get_agent_roster"),
    "get_conversation_log": (".conversation", "get_conversation_log"),
    "get_execution_agent_logs": (".execution", "get_execution_agent_logs"),
    "get_important_email_watcher": (".gmail", "get_important_email_watcher"),
    "get_timezone_store": (".timezone_store", "get_timezone_store"),
    "get_trigger_scheduler": (".trigger_scheduler", "get_trigger_scheduler"),
    "get_trigger_service": (".triggers", "get_trigger_service"),
    "get_working_memory_log": (".conversation", "get_working_memory_log"),
    "handle_chat_request": (
        ".conversation.chat_handler",
        "handle_chat_request",
    ),
    "initiate_connect": (".gmail", "initiate_connect"),
    "schedule_summarization": (".conversation", "schedule_summarization"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    try:
        module_name, attribute_name = _EXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None

    value = getattr(import_module(module_name, __name__), attribute_name)
    globals()[name] = value
    return value
