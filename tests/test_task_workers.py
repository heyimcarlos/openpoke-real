from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from server.services.task_queue import (
    ExecutorKind,
    FailureCode,
    PostgresTaskLedger,
    Principal,
    SubmitTask,
    TaskLease,
    TaskRecord,
    TaskService,
    TaskStatus,
)
from server.services.task_queue.execution import (
    ExecutionFailure,
    ExecutorRegistry,
    SyntheticExecutor,
    UnknownExecutor,
)
from server.services.task_queue.worker import (
    TaskWorker,
    WorkerOutcomeStatus,
)
from server.agents.execution_agent import sdk_executor
from server.agents.execution_agent.sdk_executor import AgentsSdkExecutor
from server.agents.interaction_agent.runtime import InteractionResult
from server.services.task_queue.projection import InteractionResultSink


@pytest.mark.asyncio
async def test_executor_policy_is_persisted_by_trusted_submission(
    ledger: PostgresTaskLedger,
) -> None:
    principal = Principal(
        actor_id="user-7",
        tenant_id="tenant-a",
        scopes=frozenset({"tasks:create"}),
    )
    accepted = await TaskService(ledger).submit(
        principal,
        SubmitTask(
            idempotency_key="synthetic-success",
            origin_turn_id="turn-7",
            agent_name="load-probe",
            executor_kind=ExecutorKind.SYNTHETIC,
            input={"mode": "success"},
        ),
    )

    lease = await ledger.claim("worker-1", timedelta(seconds=30))

    assert accepted.executor_kind is ExecutorKind.SYNTHETIC
    assert lease is not None
    assert lease.executor_kind is ExecutorKind.SYNTHETIC
    assert lease.actor_id == "user-7"
    assert lease.origin_turn_id == "turn-7"


def test_unknown_executor_is_rejected_by_fixed_registry() -> None:
    registry = ExecutorRegistry({})

    with pytest.raises(UnknownExecutor):
        registry.resolve("model-selected-module")


def _lease(
    *,
    mode: str,
    attempt_count: int = 1,
    duration_ms: int = 0,
) -> TaskLease:
    return TaskLease(
        task_id=uuid4(),
        tenant_id="tenant-a",
        actor_id="user-7",
        origin_turn_id="turn-7",
        agent_name="load-probe",
        executor_kind=ExecutorKind.SYNTHETIC,
        input={"mode": mode, "duration_ms": duration_ms},
        attempt_count=attempt_count,
        lease_generation=attempt_count,
        worker_id="worker-1",
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "attempt_count", "expected_failure"),
    [
        ("success", 1, None),
        ("fail_once", 1, FailureCode.SYNTHETIC_RETRYABLE),
        ("fail_once", 2, None),
        ("fail_always", 1, FailureCode.SYNTHETIC_RETRYABLE),
        ("non_retryable", 1, FailureCode.SYNTHETIC_NON_RETRYABLE),
    ],
)
async def test_synthetic_executor_modes_are_deterministic(
    mode: str,
    attempt_count: int,
    expected_failure: FailureCode | None,
) -> None:
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    executor = SyntheticExecutor(sleep=fake_sleep)
    lease = _lease(mode=mode, attempt_count=attempt_count, duration_ms=25)

    if expected_failure is None:
        result = await executor.execute(lease)
        assert result == {"response": "synthetic task completed"}
    else:
        with pytest.raises(ExecutionFailure) as caught:
            await executor.execute(lease)
        assert caught.value.failure.code is expected_failure

    assert slept == [0.025]


async def _submit_synthetic(
    ledger: PostgresTaskLedger,
    *,
    key: str,
    mode: str,
) -> None:
    await TaskService(ledger).submit(
        Principal(
            actor_id="user-7",
            tenant_id="tenant-a",
            scopes=frozenset({"tasks:create"}),
        ),
        SubmitTask(
            idempotency_key=key,
            origin_turn_id="turn-7",
            agent_name="load-probe",
            executor_kind=ExecutorKind.SYNTHETIC,
            input={"mode": mode},
        ),
    )


@pytest.mark.asyncio
async def test_worker_claims_executes_and_projects_only_completed_result(
    ledger: PostgresTaskLedger,
) -> None:
    await _submit_synthetic(ledger, key="worker-success", mode="success")
    projected = []

    async def capture_projection(record: TaskRecord) -> None:
        projected.append(record)

    worker = TaskWorker(
        ledger,
        ExecutorRegistry({ExecutorKind.SYNTHETIC: SyntheticExecutor()}),
        worker_id="worker-1",
        result_sink=capture_projection,
    )

    outcome = await worker.run_once()

    assert outcome.status is WorkerOutcomeStatus.COMPLETED
    assert outcome.attempt_count == 1
    assert len(projected) == 1
    assert projected[0].status is TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_projection_timeout_is_reported_after_durable_completion(
    ledger: PostgresTaskLedger,
) -> None:
    await _submit_synthetic(ledger, key="projection-timeout", mode="success")

    async def stalled_projection(_record: TaskRecord) -> None:
        await asyncio.Event().wait()

    worker = TaskWorker(
        ledger,
        ExecutorRegistry({ExecutorKind.SYNTHETIC: SyntheticExecutor()}),
        worker_id="worker-1",
        projection_timeout_seconds=0.01,
        result_sink=stalled_projection,
    )

    outcome = await worker.run_once()
    assert outcome.task_id is not None
    record = await ledger.get("tenant-a", outcome.task_id)

    assert outcome.status is WorkerOutcomeStatus.PROJECTION_FAILED
    assert record is not None
    assert record.status is TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_worker_retries_then_dead_letters_deterministic_failure(
    ledger: PostgresTaskLedger,
) -> None:
    await _submit_synthetic(ledger, key="worker-retries", mode="fail_always")
    worker = TaskWorker(
        ledger,
        ExecutorRegistry({ExecutorKind.SYNTHETIC: SyntheticExecutor()}),
        worker_id="worker-1",
    )

    outcomes = [await worker.run_once() for _ in range(3)]

    assert [outcome.status for outcome in outcomes] == [
        WorkerOutcomeStatus.RETRIED,
        WorkerOutcomeStatus.RETRIED,
        WorkerOutcomeStatus.DEAD_LETTERED,
    ]
    assert [outcome.attempt_count for outcome in outcomes] == [1, 2, 3]


@pytest.mark.asyncio
async def test_stale_worker_cannot_project_result(
    ledger: PostgresTaskLedger,
) -> None:
    await _submit_synthetic(ledger, key="stale-worker", mode="success")
    release = asyncio.Event()

    class DelayedExecutor:
        async def execute(self, lease: TaskLease) -> dict[str, str]:
            await release.wait()
            return {"response": "late"}

    projected = []

    async def capture_projection(record: TaskRecord) -> None:
        projected.append(record)

    registry = ExecutorRegistry({ExecutorKind.SYNTHETIC: DelayedExecutor()})
    stale_worker = TaskWorker(
        ledger,
        registry,
        worker_id="worker-stale",
        lease_duration=timedelta(milliseconds=20),
        result_sink=capture_projection,
    )
    replacement = TaskWorker(
        ledger,
        registry,
        worker_id="worker-current",
        lease_duration=timedelta(seconds=5),
    )

    stale_run = asyncio.create_task(stale_worker.run_once())
    await asyncio.sleep(0.03)
    replacement_lease = await ledger.claim(
        "worker-current",
        timedelta(seconds=5),
    )
    assert replacement_lease is not None
    release.set()

    outcome = await stale_run

    assert outcome.status is WorkerOutcomeStatus.STALE
    assert projected == []


@pytest.mark.asyncio
async def test_agents_sdk_executor_uses_typed_bounded_openrouter_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRunResult:
        final_output = "invoice found"

    class FakeRunner:
        def __init__(self) -> None:
            self.calls = []

        async def run(self, agent, instructions, **kwargs):
            self.calls.append((agent, instructions, kwargs))
            return FakeRunResult()

    openai_client_arguments = {}
    async_openai = sdk_executor.AsyncOpenAI

    def capture_openai_client(**kwargs):
        openai_client_arguments.update(kwargs)
        return async_openai(**kwargs)

    monkeypatch.setattr(
        sdk_executor,
        "AsyncOpenAI",
        capture_openai_client,
    )
    fake_runner = FakeRunner()
    executor = AgentsSdkExecutor(
        api_key="test-key-never-sent",
        model_name="provider/test-model",
        runner=fake_runner,
        tool_schemas=[],
        tool_registry_factory=lambda _agent, _gmail_user, **_context: {},
    )
    lease = TaskLease(
        task_id=uuid4(),
        tenant_id="tenant-a",
        actor_id="user-7",
        origin_turn_id="turn-7",
        agent_name="invoice-search",
        executor_kind=ExecutorKind.AGENT,
        input={"instructions": "find the invoice"},
        attempt_count=1,
        lease_generation=1,
        worker_id="worker-1",
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
    )

    result = await executor.execute(lease)

    assert result == {"response": "invoice found"}
    agent, instructions, kwargs = fake_runner.calls[0]
    assert instructions == "find the invoice"
    assert agent.output_type is None
    assert agent.model.model == "provider/test-model"
    assert openai_client_arguments["base_url"] == "https://openrouter.ai/api/v1"
    assert kwargs["max_turns"] == 8
    assert kwargs["run_config"].tracing_disabled is True
    assert kwargs["run_config"].trace_include_sensitive_data is False


@pytest.mark.asyncio
async def test_agents_sdk_tool_failure_is_non_retryable() -> None:
    class FakeRunResult:
        final_output = "ignored"

    class FakeRunner:
        async def run(self, agent, _instructions, **_kwargs):
            await agent.tools[0].on_invoke_tool(None, '{"value": 7}')
            return FakeRunResult()

    def failing_tool(*, value: int) -> None:
        raise RuntimeError(f"sensitive provider detail {value}")

    executor = AgentsSdkExecutor(
        api_key="test-key-never-sent",
        model_name="provider/test-model",
        runner=FakeRunner(),
        tool_schemas=[
            {
                "function": {
                    "name": "failing_tool",
                    "description": "Always fails",
                    "parameters": {
                        "type": "object",
                        "properties": {"value": {"type": "integer"}},
                        "required": ["value"],
                        "additionalProperties": False,
                    },
                }
            }
        ],
        tool_registry_factory=lambda _agent, _gmail_user, **_context: {
            "failing_tool": failing_tool
        },
    )
    lease = TaskLease(
        task_id=uuid4(),
        tenant_id="tenant-a",
        actor_id="user-7",
        origin_turn_id="turn-7",
        agent_name="invoice-search",
        executor_kind=ExecutorKind.AGENT,
        input={"instructions": "fail safely"},
        attempt_count=1,
        lease_generation=1,
        worker_id="worker-1",
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
    )

    with pytest.raises(ExecutionFailure) as caught:
        await executor.execute(lease)

    assert caught.value.failure.code is FailureCode.AGENT_NON_RETRYABLE


@pytest.mark.asyncio
async def test_result_sink_preserves_original_actor_and_turn_cause() -> None:
    captured = []

    class FakeRuntime:
        def __init__(self, *, tool_context) -> None:
            captured.append(tool_context)

        async def handle_agent_message(self, message: str) -> InteractionResult:
            captured.append(message)
            return InteractionResult(success=True, response="")

    task_service = object()
    sink = InteractionResultSink(
        task_service,
        runtime_factory=FakeRuntime,
    )
    record = TaskRecord(
        task_id=uuid4(),
        tenant_id="tenant-a",
        actor_id="user-7",
        idempotency_key="delegation:7",
        origin_turn_id="turn-7",
        agent_name="invoice-search",
        executor_kind=ExecutorKind.AGENT,
        input={"instructions": "find invoice"},
        status=TaskStatus.COMPLETED,
        result={"response": "invoice found"},
        attempt_count=1,
        failure=None,
        created_at=datetime.now(timezone.utc),
    )

    await sink(record)

    context, message = captured
    assert context.principal.actor_id == "user-7"
    assert context.principal.tenant_id == "tenant-a"
    assert context.origin_turn_id == "turn-7"
    assert context.task_service is task_service
    assert message == "[SUCCESS] invoice-search: invoice found"


@pytest.mark.asyncio
async def test_result_sink_ignores_synthetic_task_results() -> None:
    runtime_calls = []

    class UnexpectedRuntime:
        def __init__(self, *, tool_context) -> None:
            runtime_calls.append(tool_context)

        async def handle_agent_message(self, message: str) -> InteractionResult:
            runtime_calls.append(message)
            return InteractionResult(success=True, response="")

    sink = InteractionResultSink(
        object(),
        runtime_factory=UnexpectedRuntime,
    )
    record = TaskRecord(
        task_id=uuid4(),
        tenant_id="tenant-a",
        actor_id="load-test",
        idempotency_key="synthetic:7",
        origin_turn_id="load:7",
        agent_name="load-probe",
        executor_kind=ExecutorKind.SYNTHETIC,
        input={"mode": "success"},
        status=TaskStatus.COMPLETED,
        result={"response": "synthetic task completed"},
        attempt_count=1,
        failure=None,
        created_at=datetime.now(timezone.utc),
    )

    await sink(record)

    assert runtime_calls == []
