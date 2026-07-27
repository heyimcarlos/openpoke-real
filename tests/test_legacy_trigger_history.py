from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from server.agents.execution_agent.sdk_executor import AgentsSdkExecutor
from server.services import trigger_scheduler
from server.services.task_queue import TaskLease, TaskStatus
from server.services.trigger_scheduler import TriggerScheduler
from server.services.triggers.service import TriggerService
from server.services.triggers.store import TriggerStore


@pytest.mark.asyncio
async def test_legacy_trigger_reuses_its_existing_execution_history_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    submissions = []

    class RecordingTaskService:
        async def submit(self, principal, command):
            submissions.append((principal, command))
            return SimpleNamespace(task_id="task-1")

        async def get(self, _principal, _task_id):
            return SimpleNamespace(status=TaskStatus.COMPLETED)

    task_service = RecordingTaskService()

    async def get_task_service() -> RecordingTaskService:
        return task_service

    monkeypatch.setattr(
        trigger_scheduler,
        "get_shared_task_service",
        get_task_service,
    )
    service = TriggerService(TriggerStore(tmp_path / "triggers.db"))
    trigger = service.create_trigger(
        agent_name="Invoice Search Team",
        payload="find the newest invoice",
        start_time=(
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).isoformat(),
    )
    assert trigger.tenant_id is None
    assert trigger.actor_id is None

    scheduler = TriggerScheduler()
    scheduler._service = service

    await scheduler._execute_trigger(trigger)

    principal, command = submissions[0]
    assert principal.tenant_id == "local"
    assert principal.actor_id == "trigger-scheduler"
    assert command.agent_name == "Invoice Search Team"
    assert command.input["execution_storage_key"] == "Invoice Search Team"

    class FakeRunner:
        @staticmethod
        async def run(_agent, _instructions, **_kwargs):
            return SimpleNamespace(final_output="legacy trigger completed")

    executor = AgentsSdkExecutor(
        api_key="test-key-never-sent",
        model_name="provider/test-model",
        runner=FakeRunner(),
        tool_schemas=[],
        tool_registry_factory=lambda *_args, **_kwargs: {},
    )
    result = await executor.execute(
        TaskLease(
            task_id=uuid4(),
            tenant_id=principal.tenant_id,
            actor_id=principal.actor_id,
            origin_turn_id=command.origin_turn_id,
            agent_name=command.agent_name,
            executor_kind=command.executor_kind,
            input=command.input,
            attempt_count=1,
            lease_generation=1,
            worker_id="worker-1",
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
    )

    assert result == {"response": "legacy trigger completed"}
