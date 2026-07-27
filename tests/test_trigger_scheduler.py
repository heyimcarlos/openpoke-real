from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from server.services import trigger_scheduler
from server.services.task_queue import AdmissionRejected
from server.services.trigger_scheduler import TriggerScheduler
from server.services.triggers.service import TriggerService
from server.services.triggers.store import TriggerStore


@pytest.mark.asyncio
async def test_submission_cancellation_releases_in_flight_trigger(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    acquisition_started = asyncio.Event()

    async def blocked_task_service() -> None:
        acquisition_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(
        trigger_scheduler,
        "get_shared_task_service",
        blocked_task_service,
    )
    service = TriggerService(TriggerStore(tmp_path / "triggers.db"))
    trigger = service.create_trigger(
        agent_name="agent-key",
        payload="find invoices",
        start_time=(
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).isoformat(),
    )
    scheduler = TriggerScheduler()
    scheduler._service = service
    scheduler._in_flight.add(trigger.id)

    execution = asyncio.create_task(scheduler._execute_trigger(trigger))
    await acquisition_started.wait()
    execution.cancel()

    with pytest.raises(asyncio.CancelledError):
        await execution
    assert trigger.id not in scheduler._in_flight


@pytest.mark.asyncio
async def test_admission_rejection_leaves_trigger_occurrence_due(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class RejectingTaskService:
        async def submit(self, _principal, _command):
            raise AdmissionRejected(retry_after_seconds=5)

    async def rejecting_task_service() -> RejectingTaskService:
        return RejectingTaskService()

    monkeypatch.setattr(
        trigger_scheduler,
        "get_shared_task_service",
        rejecting_task_service,
    )
    service = TriggerService(TriggerStore(tmp_path / "triggers.db"))
    due_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    trigger = service.create_trigger(
        agent_name="agent-key",
        display_agent_name="invoice-agent",
        tenant_id="tenant-a",
        actor_id="user-7",
        payload="find invoices",
        start_time=due_at.isoformat(),
    )
    assert trigger.next_trigger is not None

    scheduler = TriggerScheduler()
    scheduler._service = service

    await scheduler._execute_trigger(trigger)

    preserved = service.list_triggers(agent_name=trigger.agent_name)[0]
    assert preserved.status == "active"
    assert preserved.next_trigger == trigger.next_trigger
    assert preserved.last_error is None
