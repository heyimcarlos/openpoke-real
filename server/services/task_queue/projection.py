"""Best-effort projection from durable completion into current conversation storage."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ...agents.interaction_agent.runtime import InteractionAgentRuntime
from ...agents.interaction_agent.tools import InteractionToolContext
from ...agents.execution_agent.agent import ExecutionAgent, execution_storage_key
from .models import ExecutorKind, Principal, TaskRecord, TaskStatus
from .service import TaskService


class InteractionResultSink:
    """Forward a fenced result through the existing interaction-agent path."""

    def __init__(
        self,
        task_service: TaskService,
        *,
        runtime_factory: Callable[..., Any] = InteractionAgentRuntime,
    ) -> None:
        self._task_service = task_service
        self._runtime_factory = runtime_factory

    async def __call__(self, record: TaskRecord) -> None:
        if record.status is not TaskStatus.COMPLETED or record.result is None:
            raise ValueError("only completed tasks can be projected")
        response = record.result.get("response")
        if not isinstance(response, str) or not response.strip():
            raise ValueError("completed task result has no response")
        if record.executor_kind is ExecutorKind.AGENT:
            execution_agent = ExecutionAgent(
                record.agent_name,
                storage_key=(
                    record.input.get("execution_storage_key")
                    if isinstance(
                        record.input.get("execution_storage_key"),
                        str,
                    )
                    else execution_storage_key(
                        record.tenant_id,
                        record.actor_id,
                        record.agent_name,
                    )
                ),
            )
            instructions = record.input.get("instructions")
            if isinstance(instructions, str):
                execution_agent.record_request(instructions)
            execution_agent.record_response(response)

        context = InteractionToolContext(
            principal=Principal(
                actor_id=record.actor_id,
                tenant_id=record.tenant_id,
                scopes=frozenset({"tasks:create"}),
                composio_user_id=(
                    record.input.get("composio_user_id")
                    if isinstance(
                        record.input.get("composio_user_id"),
                        str,
                    )
                    else None
                ),
            ),
            origin_turn_id=record.origin_turn_id,
            task_service=self._task_service,
        )
        runtime = self._runtime_factory(tool_context=context)
        projection = await runtime.handle_agent_message(
            f"[SUCCESS] {record.agent_name}: {response}"
        )
        if getattr(projection, "success", True) is False:
            raise RuntimeError("conversation projection failed")
