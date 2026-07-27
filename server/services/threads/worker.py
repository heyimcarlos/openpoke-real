"""One disposable, fenced interaction Agent Run."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from html import escape
from typing import Callable, Protocol
from uuid import UUID

from ...agents.interaction_agent.runtime import (
    InteractionAgentRuntime,
    InteractionResult,
)
from ...agents.interaction_agent.tools import InteractionToolContext
from ..task_queue import Principal, TaskService
from .ledger import PostgresThreadLedger, StaleAgentRunLease
from .models import AgentRunLease


class AgentRunOutcomeStatus(str, Enum):
    IDLE = "idle"
    COMPLETED = "completed"
    RETRIED = "retried"
    FAILED = "failed"
    STALE = "stale"


@dataclass(frozen=True)
class AgentRunOutcome:
    status: AgentRunOutcomeStatus
    run_id: UUID | None = None


class _Runtime(Protocol):
    async def execute(self, user_message: str) -> InteractionResult: ...

    async def handle_agent_message(
        self,
        agent_message: str,
    ) -> InteractionResult: ...


class AgentRunWorker:
    """Claim, reconstruct, execute, and commit one interaction turn."""

    def __init__(
        self,
        ledger: PostgresThreadLedger,
        task_service: TaskService,
        *,
        worker_id: str,
        lease_duration: timedelta = timedelta(seconds=120),
        execution_timeout_seconds: float = 90,
        runtime_factory: Callable[..., _Runtime] = InteractionAgentRuntime,
    ) -> None:
        self._ledger = ledger
        self._task_service = task_service
        self._worker_id = worker_id
        self._lease_duration = lease_duration
        if execution_timeout_seconds <= 0:
            raise ValueError("execution timeout must be positive")
        if execution_timeout_seconds >= lease_duration.total_seconds():
            raise ValueError("execution timeout must be shorter than the lease")
        self._execution_timeout_seconds = execution_timeout_seconds
        self._runtime_factory = runtime_factory

    async def run_once(self) -> AgentRunOutcome:
        lease = await self._ledger.claim_run(
            self._worker_id,
            self._lease_duration,
        )
        if lease is None:
            return AgentRunOutcome(status=AgentRunOutcomeStatus.IDLE)
        context = await self._ledger.get_context(lease)
        if not context:
            return await self._record_failure(lease)
        latest = context[-1]
        latest_content = latest.get("content")
        if not isinstance(latest_content, str):
            return await self._record_failure(lease)
        principal = Principal(
            actor_id=lease.actor_id,
            tenant_id=lease.tenant_id,
            scopes=frozenset({"chat:send", "tasks:create"}),
            composio_user_id=lease.composio_user_id,
        )
        try:
            runtime = self._runtime_factory(
                tool_context=InteractionToolContext(
                    principal=principal,
                    origin_turn_id=str(lease.run_id),
                    task_service=self._task_service,
                    thread_ledger=self._ledger,
                    run_lease=lease,
                    persist_locally=False,
                ),
                transcript=_render_transcript(context[:-1]),
                persist_locally=False,
            )
            if lease.input_source_kind == "execution_result":
                result = await asyncio.wait_for(
                    runtime.handle_agent_message(latest_content),
                    timeout=self._execution_timeout_seconds,
                )
            else:
                result = await asyncio.wait_for(
                    runtime.execute(latest_content),
                    timeout=self._execution_timeout_seconds,
                )
        except Exception:
            return await self._record_failure(lease)
        if not result.success:
            return await self._record_failure(lease)
        try:
            await self._ledger.complete_run(
                lease,
                response=result.response or None,
            )
        except StaleAgentRunLease:
            return AgentRunOutcome(
                status=AgentRunOutcomeStatus.STALE,
                run_id=lease.run_id,
            )
        return AgentRunOutcome(
            status=AgentRunOutcomeStatus.COMPLETED,
            run_id=lease.run_id,
        )

    async def _record_failure(
        self,
        lease: AgentRunLease,
    ) -> AgentRunOutcome:
        try:
            run = await self._ledger.fail_run(lease)
        except StaleAgentRunLease:
            return AgentRunOutcome(
                status=AgentRunOutcomeStatus.STALE,
                run_id=lease.run_id,
            )
        status = (
            AgentRunOutcomeStatus.RETRIED
            if run.status.value == "queued"
            else AgentRunOutcomeStatus.FAILED
        )
        return AgentRunOutcome(status=status, run_id=lease.run_id)


def _render_transcript(items: list[dict]) -> str:
    rendered: list[str] = []
    tags = {
        "user": "user_message",
        "assistant": "poke_reply",
        "agent": "agent_message",
    }
    for item in items:
        role = item.get("role")
        content = item.get("content")
        if role in tags and isinstance(content, str):
            rendered.append(
                f"<{tags[role]}>{escape(content, quote=False)}</{tags[role]}>"
            )
    return "\n".join(rendered)
