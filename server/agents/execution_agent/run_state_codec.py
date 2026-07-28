"""Safe serialization for one versioned Agents SDK approval flow."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence
from importlib.metadata import version
from typing import Any
from uuid import UUID

from agents import Agent, RunState
from pydantic import BaseModel, ConfigDict, JsonValue, TypeAdapter

from ...services.task_queue import (
    RunStateCompatibility,
    RunStateIncompatible,
    TaskLease,
    TaskSuspension,
    TaskSuspensionRecord,
)


RUN_STATE_CODEC_VERSION = 1
_JSON_OBJECT = TypeAdapter(dict[str, JsonValue])
_SENSITIVE_FIELD_NAMES = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "bearer_token",
        "client_secret",
        "credential",
        "credentials",
        "password",
        "private_key",
        "provider_api_key",
        "refresh_token",
        "secret",
        "token",
    }
)
_SENSITIVE_ENVIRONMENT_NAMES = frozenset(
    {
        "DATABASE_URL",
        "POSTGRES_DSN",
        "RABBITMQ_URL",
        "REDIS_URL",
    }
)
_SENSITIVE_ENVIRONMENT_NAME = re.compile(
    r"(?:^|_)(?:AUTH|CREDENTIAL|KEY|PASSWORD|SECRET|TOKEN)(?:_|$)",
    re.IGNORECASE,
)
_SENSITIVE_TEXT_PATTERNS = (
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{8,}", re.IGNORECASE),
    re.compile(r"\bsk-(?:proj-|or-v1-)?[A-Za-z0-9_-]{8,}"),
    re.compile(r"\bAKIA[A-Z0-9]{16}\b"),
    re.compile(r"\bAIza[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{8,}\b"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{8,}\b"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(
        r"""(?ix)
        ["']?
        (?:api[_-]?key|access[_-]?token|authorization|bearer[_-]?token|
           client[_-]?secret|password|private[_-]?key|refresh[_-]?token)
        ["']?\s*[:=]\s*["']?[^\s"',}]{8,}
        """
    ),
)


class ReasoningRunContext(BaseModel):
    """The complete allowlist of application context persisted with RunState."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_instance_id: UUID
    workflow_step_id: UUID
    execution_task_id: UUID

    @classmethod
    def from_lease(cls, lease: TaskLease) -> ReasoningRunContext:
        if lease.workflow_instance_id is None or lease.workflow_step_id is None:
            raise RunStateIncompatible(
                "RunState requires a trusted Workflow Step lease"
            )
        return cls(
            workflow_instance_id=lease.workflow_instance_id,
            workflow_step_id=lease.workflow_step_id,
            execution_task_id=lease.task_id,
        )


class AgentsRunStateCodec:
    """Hide SDK serialization, context filtering, and approval restoration."""

    def __init__(self, *, agent_definition_version: str) -> None:
        self.compatibility = RunStateCompatibility(
            codec_version=RUN_STATE_CODEC_VERSION,
            agents_sdk_version=version("openai-agents"),
            agent_definition_version=agent_definition_version,
        )

    @staticmethod
    def context_for(lease: TaskLease) -> ReasoningRunContext:
        return ReasoningRunContext.from_lease(lease)

    def suspend(
        self,
        result: Any,
        lease: TaskLease,
        *,
        wait_key: str,
        approval_tool_name: str,
        model_requests_used: int,
        specialist_calls_used: int,
    ) -> TaskSuspension:
        _require_exact_interruption(
            result.interruptions,
            approval_tool_name,
        )
        state = result.to_state()
        serialized = state.to_json(
            context_serializer=_serialize_context,
            strict_context=True,
            include_tracing_api_key=False,
        )
        safe_state = _JSON_OBJECT.validate_python(serialized)
        _assert_no_sensitive_state(safe_state)
        return TaskSuspension(
            wait_key=wait_key,
            compatibility=self.compatibility,
            model_requests_used=model_requests_used,
            specialist_calls_used=specialist_calls_used,
            state=safe_state,
        )

    async def resume(
        self,
        snapshot: TaskSuspensionRecord,
        initial_agent: Agent[Any],
        lease: TaskLease,
        *,
        approval_tool_name: str,
    ) -> RunState[Any]:
        if snapshot.compatibility != self.compatibility:
            raise RunStateIncompatible(
                "persisted RunState version is incompatible"
            )
        expected_context = ReasoningRunContext.from_lease(lease)

        def deserialize_context(value: dict[str, Any]) -> ReasoningRunContext:
            restored = ReasoningRunContext.model_validate(value)
            if restored != expected_context:
                raise RunStateIncompatible(
                    "persisted RunState belongs to a different Workflow Step"
                )
            return restored

        state = await RunState.from_json(
            initial_agent=initial_agent,
            state_json=snapshot.state,
            context_deserializer=deserialize_context,
            strict_context=True,
        )
        interruption = _require_exact_interruption(
            state.get_interruptions(),
            approval_tool_name,
        )
        state.approve(interruption)
        return state


def _serialize_context(value: Any) -> dict[str, JsonValue]:
    context = ReasoningRunContext.model_validate(value)
    return context.model_dump(mode="json")


def _assert_no_sensitive_state(value: JsonValue) -> None:
    """Reject a snapshot instead of persisting secret-shaped data."""

    environment_values = tuple(
        environment_value
        for environment_name, environment_value in os.environ.items()
        if len(environment_value) >= 8
        and (
            environment_name in _SENSITIVE_ENVIRONMENT_NAMES
            or _SENSITIVE_ENVIRONMENT_NAME.search(environment_name)
        )
    )

    def visit(item: JsonValue) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                normalized_key = re.sub(
                    r"[^a-z0-9]+",
                    "_",
                    str(key).lower(),
                ).strip("_")
                if normalized_key in _SENSITIVE_FIELD_NAMES:
                    raise RunStateIncompatible(
                        "Agents RunState contains sensitive data"
                    )
                visit(child)
            return
        if isinstance(item, Sequence) and not isinstance(
            item,
            (str, bytes, bytearray),
        ):
            for child in item:
                visit(child)
            return
        if not isinstance(item, str):
            return
        if any(pattern.search(item) for pattern in _SENSITIVE_TEXT_PATTERNS):
            raise RunStateIncompatible(
                "Agents RunState contains sensitive data"
            )
        if any(secret in item for secret in environment_values):
            raise RunStateIncompatible(
                "Agents RunState contains sensitive data"
            )

    visit(value)


def _require_exact_interruption(
    interruptions: list[Any],
    approval_tool_name: str,
) -> Any:
    if len(interruptions) != 1:
        raise RunStateIncompatible(
            "approval run must contain exactly one interruption"
        )
    interruption = interruptions[0]
    if getattr(interruption, "tool_name", None) != approval_tool_name:
        raise RunStateIncompatible(
            "approval run contains an unexpected interruption"
        )
    return interruption
