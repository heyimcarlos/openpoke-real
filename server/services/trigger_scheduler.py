"""Background scheduler that watches trigger definitions and executes them."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional, Set

from ..logging_config import logger
from .task_queue import (
    AdmissionRejected,
    ExecutorKind,
    Principal,
    SubmitTask,
    TaskStatus,
)
from .task_queue.provider import get_shared_task_service
from .triggers import TriggerRecord, get_trigger_service


UTC = timezone.utc


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _isoformat(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


class TriggerScheduler:
    """Polls stored triggers and launches execution agents when due."""

    def __init__(self, poll_interval_seconds: float = 10.0) -> None:
        self._poll_interval = poll_interval_seconds
        self._service = get_trigger_service()
        self._task: Optional[asyncio.Task[None]] = None
        self._running = False
        self._in_flight: Set[int] = set()
        self._execution_tasks: Set[asyncio.Task[None]] = set()
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if self._task and not self._task.done():
                return
            loop = asyncio.get_running_loop()
            self._running = True
            self._task = loop.create_task(self._run(), name="trigger-scheduler")
            logger.info("Trigger scheduler started", extra={"interval": self._poll_interval})

    async def stop(self) -> None:
        async with self._lock:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
                self._task = None
                logger.info("Trigger scheduler stopped")
            execution_tasks = list(self._execution_tasks)
            for task in execution_tasks:
                task.cancel()
            if execution_tasks:
                await asyncio.gather(
                    *execution_tasks,
                    return_exceptions=True,
                )

    async def _run(self) -> None:
        try:
            while self._running:
                await self._poll_once()
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            raise
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Trigger scheduler loop crashed", extra={"error": str(exc)})

    async def _poll_once(self) -> None:
        now = _utc_now()
        due_triggers = self._service.get_due_triggers(before=now)
        if not due_triggers:
            return

        for trigger in due_triggers:
            if trigger.id in self._in_flight:
                continue
            self._in_flight.add(trigger.id)
            task = asyncio.create_task(
                self._execute_trigger(trigger),
                name=f"trigger-{trigger.id}",
            )
            self._execution_tasks.add(task)
            task.add_done_callback(self._execution_tasks.discard)

    async def _execute_trigger(self, trigger: TriggerRecord) -> None:
        try:
            fired_at = _utc_now()
            instructions = self._format_instructions(trigger, fired_at)
            logger.info(
                "Dispatching trigger",
                extra={
                    "trigger_id": trigger.id,
                    "agent": trigger.agent_name,
                    "scheduled_for": trigger.next_trigger,
                },
            )
            scheduled_for = trigger.next_trigger or _isoformat(fired_at)
            task_service = await get_shared_task_service()
            principal = Principal(
                actor_id=trigger.actor_id or "trigger-scheduler",
                tenant_id=trigger.tenant_id or "local",
                scopes=frozenset({"tasks:create", "tasks:read"}),
                composio_user_id=trigger.composio_user_id,
            )
            accepted = await task_service.submit(
                principal,
                SubmitTask(
                    idempotency_key=f"trigger:{trigger.id}:{scheduled_for}",
                    origin_turn_id=f"trigger:{trigger.id}:{scheduled_for}",
                    agent_name=(
                        trigger.display_agent_name
                        or trigger.agent_name
                    ),
                    executor_kind=ExecutorKind.AGENT,
                    input={
                        "instructions": instructions,
                        "composio_user_id": trigger.composio_user_id,
                        "execution_storage_key": (
                            trigger.agent_name
                            if (
                                trigger.agent_name.startswith("agent-")
                                or (
                                    trigger.tenant_id is None
                                    and trigger.actor_id is None
                                )
                            )
                            else None
                        ),
                    },
                ),
            )
        except asyncio.CancelledError:
            self._in_flight.discard(trigger.id)
            raise
        except AdmissionRejected as exc:
            logger.info(
                "Trigger task admission deferred",
                extra={
                    "trigger_id": trigger.id,
                    "agent": trigger.agent_name,
                    "retry_after_seconds": exc.retry_after_seconds,
                },
            )
            self._in_flight.discard(trigger.id)
            return
        except Exception:  # pragma: no cover - defensive
            self._handle_failure(
                trigger,
                _utc_now(),
                "trigger task submission failed",
            )
            logger.exception(
                "Trigger task submission failed",
                extra={"trigger_id": trigger.id, "agent": trigger.agent_name},
            )
            self._in_flight.discard(trigger.id)
            return

        try:
            while True:
                record = await task_service.get(principal, accepted.task_id)
                if record is None:
                    raise RuntimeError("accepted trigger task disappeared")
                if record.status is TaskStatus.COMPLETED:
                    self._handle_success(trigger, fired_at)
                    break
                if record.status in {
                    TaskStatus.DEAD_LETTERED,
                    TaskStatus.CANCELLED,
                }:
                    failure = record.failure.value if record.failure else "cancelled"
                    self._handle_failure(trigger, fired_at, failure)
                    break
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive
            logger.exception(
                "Trigger task observation failed",
                extra={"trigger_id": trigger.id, "agent": trigger.agent_name},
            )
        finally:
            self._in_flight.discard(trigger.id)

    def _handle_success(self, trigger: TriggerRecord, fired_at: datetime) -> None:
        logger.info(
            "Trigger completed",
            extra={"trigger_id": trigger.id, "agent": trigger.agent_name},
        )
        self._service.schedule_next_occurrence(trigger, fired_at=fired_at)

    def _handle_failure(self, trigger: TriggerRecord, fired_at: datetime, error: str) -> None:
        logger.warning(
            "Trigger execution failed",
            extra={
                "trigger_id": trigger.id,
                "agent": trigger.agent_name,
                "error": error,
            },
        )
        self._service.record_failure(trigger, error)
        if trigger.recurrence_rule:
            self._service.schedule_next_occurrence(trigger, fired_at=fired_at)
        else:
            self._service.clear_next_fire(trigger.id, agent_name=trigger.agent_name)

    def _format_instructions(self, trigger: TriggerRecord, fired_at: datetime) -> str:
        scheduled_for = trigger.next_trigger or _isoformat(fired_at)
        metadata_lines = [f"Trigger ID: {trigger.id}"]
        if trigger.recurrence_rule:
            metadata_lines.append(f"Recurrence: {trigger.recurrence_rule}")
        if trigger.timezone:
            metadata_lines.append(f"Timezone: {trigger.timezone}")
        if trigger.start_time:
            metadata_lines.append(f"Start Time (UTC): {trigger.start_time}")

        metadata = "\n".join(f"- {line}" for line in metadata_lines)
        return (
            f"Trigger fired at {_isoformat(fired_at)} (UTC).\n"
            f"Scheduled occurrence time: {scheduled_for}.\n\n"
            f"Metadata:\n{metadata}\n\n"
            f"Payload:\n{trigger.payload}"
        )


_scheduler_instance: Optional[TriggerScheduler] = None


def get_trigger_scheduler() -> TriggerScheduler:
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = TriggerScheduler()
    return _scheduler_instance


__all__ = ["TriggerScheduler", "get_trigger_scheduler"]
