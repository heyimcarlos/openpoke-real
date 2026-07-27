from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import pytest
from agents import Runner
from agents.items import ModelResponse
from agents.models.interface import Model
from agents.usage import Usage
from openai.types.responses import (
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)

from server.agents.execution_agent import (
    BoundedReasoningExecutor,
    ReasoningLimits,
    WorkflowAwareAgentExecutor,
)
from server.config import Settings
from server.services.task_queue import ExecutionFailure
from server.services.task_queue.execution import ExecutorRegistry
from server.services.task_queue.worker import TaskWorker, WorkerOutcomeStatus
from server.services.threads import PostgresThreadLedger
from server.services.task_queue import (
    ExecutorKind,
    FailureCode,
    Principal,
    SubmitTask,
    TaskLease,
)
from server.services.workflows import (
    FieldContract,
    FieldType,
    PostgresWorkflowStore,
    StepTemplate,
    WorkflowDefinition,
    WorkflowStartCommand,
)


def _message(text: str, message_id: str) -> ResponseOutputMessage:
    return ResponseOutputMessage(
        id=message_id,
        content=[
            ResponseOutputText(
                annotations=[],
                text=text,
                type="output_text",
                logprobs=None,
            )
        ],
        role="assistant",
        status="completed",
        type="message",
    )


class ScriptedCoordinationModel(Model):
    """Provider-free model that drives the real SDK manager loop."""

    def __init__(
        self,
        *,
        extra_specialist_calls: int = 0,
        replay_specialist_call: bool = False,
        failing_specialist: str | None = None,
        malformed_specialist_input: bool = False,
    ) -> None:
        self.extra_specialist_calls = extra_specialist_calls
        self.replay_specialist_call = replay_specialist_call
        self.failing_specialist = failing_specialist
        self.malformed_specialist_input = malformed_specialist_input
        self.manager_requests = 0
        self.request_settings = []
        self.specialist_requests = 0
        self.evidence_requests = 0
        self.risk_requests = 0
        self.active_specialists = 0
        self.max_active_specialists = 0

    async def get_response(
        self,
        system_instructions,
        input,
        model_settings,
        tools,
        output_schema,
        handoffs,
        tracing,
        *,
        previous_response_id,
        conversation_id,
        prompt,
    ) -> ModelResponse:
        del (
            input,
            output_schema,
            handoffs,
            tracing,
            previous_response_id,
            conversation_id,
            prompt,
        )
        self.request_settings.append(model_settings)
        if system_instructions and "ROLE: evidence specialist" in system_instructions:
            self.specialist_requests += 1
            self.evidence_requests += 1
            if self.failing_specialist == "evidence":
                raise RuntimeError("sensitive provider failure")
            self.active_specialists += 1
            self.max_active_specialists = max(
                self.max_active_specialists,
                self.active_specialists,
            )
            await asyncio.sleep(0.01)
            self.active_specialists -= 1
            return ModelResponse(
                output=[
                    _message(
                        json.dumps(
                            {
                                "findings": ["The ledger survives worker restarts."],
                                "confidence": "high",
                            }
                        ),
                        "evidence-result",
                    )
                ],
                usage=Usage(requests=1),
                response_id="evidence-response",
            )
        if system_instructions and "ROLE: risk specialist" in system_instructions:
            self.specialist_requests += 1
            self.risk_requests += 1
            if self.failing_specialist == "risk":
                raise RuntimeError("sensitive provider failure")
            self.active_specialists += 1
            self.max_active_specialists = max(
                self.max_active_specialists,
                self.active_specialists,
            )
            await asyncio.sleep(0.01)
            self.active_specialists -= 1
            return ModelResponse(
                output=[
                    _message(
                        json.dumps(
                            {
                                "risks": ["A provider call can replay."],
                                "mitigation": "Use idempotent side-effect tools.",
                            }
                        ),
                        "risk-result",
                    )
                ],
                usage=Usage(requests=1),
                response_id="risk-response",
            )

        assert {tool.name for tool in tools} == {
            "analyze_evidence",
            "review_risks",
        }
        self.manager_requests += 1
        if self.manager_requests == 1:
            calls = [
                ResponseFunctionToolCall(
                    arguments=json.dumps(
                        (
                            {"question": "Should we ship the durable queue?"}
                            if self.malformed_specialist_input
                            else {
                                "question": "Should we ship the durable queue?",
                                "available_evidence": "Crash recovery tests pass.",
                            }
                        )
                    ),
                    call_id="evidence-call",
                    name="analyze_evidence",
                    type="function_call",
                ),
                ResponseFunctionToolCall(
                    arguments=json.dumps(
                        {
                            "proposal": "Ship the durable queue.",
                            "constraints": "One-day implementation.",
                        }
                    ),
                    call_id="risk-call",
                    name="review_risks",
                    type="function_call",
                ),
            ]
            calls.extend(
                ResponseFunctionToolCall(
                    arguments=json.dumps(
                        {
                            "question": "Should we ship the durable queue?",
                            "available_evidence": "More evidence.",
                        }
                    ),
                    call_id=f"extra-evidence-call-{index}",
                    name="analyze_evidence",
                    type="function_call",
                )
                for index in range(self.extra_specialist_calls)
            )
            if self.replay_specialist_call:
                calls.append(
                    ResponseFunctionToolCall(
                        arguments=json.dumps(
                            {
                                "question": "Should we ship the durable queue?",
                                "available_evidence": "Crash recovery tests pass.",
                            }
                        ),
                        call_id="evidence-call",
                        name="analyze_evidence",
                        type="function_call",
                    )
                )
            return ModelResponse(
                output=calls,
                usage=Usage(requests=1),
                response_id="manager-tools",
            )
        return ModelResponse(
            output=[
                _message(
                    json.dumps(
                        {
                            "response": "Ship the bounded durable slice.",
                            "evidence": ["The ledger survives worker restarts."],
                            "risks": ["A provider call can replay."],
                            "confidence": "high",
                        }
                    ),
                    "manager-result",
                )
            ],
            usage=Usage(requests=1),
            response_id="manager-final",
        )

    def stream_response(self, *args, **kwargs) -> AsyncIterator[Any]:
        del args, kwargs

        async def empty() -> AsyncIterator[Any]:
            if False:
                yield None

        return empty()


class CapturingRunner:
    call: dict[str, Any] | None = None

    @classmethod
    async def run(cls, agent, task_input, **kwargs):
        cls.call = {
            "agent": agent,
            "task_input": task_input,
            **kwargs,
        }
        return await Runner.run(agent, task_input, **kwargs)


def _workflow_lease() -> TaskLease:
    return TaskLease(
        task_id=uuid4(),
        tenant_id="tenant-a",
        actor_id="user-7",
        origin_turn_id="turn-7",
        agent_name="bounded-reasoning-manager",
        executor_kind=ExecutorKind.AGENT,
        input={
            "question": "Should we ship the durable queue?",
            "evidence": "Crash recovery tests pass.",
            "constraints": "One-day implementation.",
        },
        attempt_count=2,
        lease_generation=3,
        worker_id="worker-1",
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
        workflow_instance_id=uuid4(),
        workflow_step_id=uuid4(),
    )


def test_reasoning_uses_a_structured_output_capable_default_model() -> None:
    settings = Settings()

    assert settings.reasoning_agent_model == "openai/gpt-4.1-mini"
    assert settings.reasoning_agent_model != settings.execution_agent_model


@pytest.mark.asyncio
async def test_reasoning_workflow_step_uses_bounded_typed_sdk_coordination() -> None:
    model = ScriptedCoordinationModel()
    lease = _workflow_lease()
    executor = BoundedReasoningExecutor(
        model=model,
        runner=CapturingRunner,
        limits=ReasoningLimits(
            manager_max_turns=3,
            specialist_max_turns=2,
            max_model_requests=6,
            max_output_tokens=400,
            max_local_tool_concurrency=1,
        ),
    )

    result = await executor.execute(lease)

    assert result == {
        "response": "Ship the bounded durable slice.",
        "evidence": ["The ledger survives worker restarts."],
        "risks": ["A provider call can replay."],
        "confidence": "high",
    }
    assert model.manager_requests == 2
    assert len(model.request_settings) == 4
    assert {settings.max_tokens for settings in model.request_settings} == {400}
    assert model.max_active_specialists == 1

    assert CapturingRunner.call is not None
    call = CapturingRunner.call
    assert call["max_turns"] == 3
    assert call["agent"].output_type.__name__ == "ReasoningStepResult"
    assert call["agent"].handoffs == []
    assert {
        tool.name: set(tool.params_json_schema["required"])
        for tool in call["agent"].tools
    } == {
        "analyze_evidence": {"question", "available_evidence"},
        "review_risks": {"proposal", "constraints"},
    }
    run_config = call["run_config"]
    assert run_config.tracing_disabled is True
    assert run_config.trace_include_sensitive_data is False
    assert run_config.workflow_name == "openpoke.reasoning_step"
    assert run_config.trace_metadata == {
        "workflow_instance_id": str(lease.workflow_instance_id),
        "workflow_step_id": str(lease.workflow_step_id),
        "execution_task_id": str(lease.task_id),
        "task_attempt_id": f"{lease.task_id}:{lease.attempt_count}",
        "task_lease_id": f"{lease.task_id}:{lease.lease_generation}",
    }
    assert run_config.tool_execution.max_function_tool_concurrency == 1


@pytest.mark.asyncio
async def test_default_specialist_concurrency_is_two_and_bounded() -> None:
    model = ScriptedCoordinationModel()
    executor = BoundedReasoningExecutor(model=model)

    await executor.execute(_workflow_lease())

    assert model.specialist_requests == 2
    assert model.max_active_specialists == 2


@pytest.mark.asyncio
async def test_third_specialist_call_fails_before_starting_child() -> None:
    model = ScriptedCoordinationModel(extra_specialist_calls=1)
    executor = BoundedReasoningExecutor(model=model)

    with pytest.raises(ExecutionFailure):
        await executor.execute(_workflow_lease())

    assert model.specialist_requests == 2


@pytest.mark.asyncio
async def test_replayed_sdk_tool_call_id_is_rejected_without_child_cost() -> None:
    model = ScriptedCoordinationModel(replay_specialist_call=True)
    executor = BoundedReasoningExecutor(model=model)

    with pytest.raises(ExecutionFailure):
        await executor.execute(_workflow_lease())

    assert model.specialist_requests == 2


@pytest.mark.asyncio
async def test_specialist_failure_fails_the_whole_durable_step() -> None:
    model = ScriptedCoordinationModel(failing_specialist="risk")
    executor = BoundedReasoningExecutor(model=model)

    with pytest.raises(ExecutionFailure) as caught:
        await executor.execute(_workflow_lease())

    assert caught.value.failure.code.value == "agent_retryable"
    assert caught.value.failure.retryable
    assert model.manager_requests == 1


@pytest.mark.asyncio
async def test_model_request_budget_is_shared_across_manager_and_children() -> None:
    model = ScriptedCoordinationModel()
    executor = BoundedReasoningExecutor(
        model=model,
        limits=ReasoningLimits(max_model_requests=3),
    )

    with pytest.raises(ExecutionFailure):
        await executor.execute(_workflow_lease())

    assert len(model.request_settings) == 3


@pytest.mark.asyncio
async def test_invalid_typed_specialist_input_never_starts_child() -> None:
    model = ScriptedCoordinationModel(malformed_specialist_input=True)
    executor = BoundedReasoningExecutor(model=model)

    with pytest.raises(ExecutionFailure):
        await executor.execute(_workflow_lease())

    assert model.evidence_requests == 0


@pytest.mark.asyncio
async def test_workflow_aware_executor_preserves_independent_task_semantics() -> None:
    calls: list[tuple[str, TaskLease]] = []

    class RecordingExecutor:
        def __init__(self, name: str) -> None:
            self.name = name

        async def execute(self, lease: TaskLease):
            calls.append((self.name, lease))
            return {"response": self.name}

    router = WorkflowAwareAgentExecutor(
        independent=RecordingExecutor("independent"),
        bounded_reasoning=RecordingExecutor("workflow"),
    )
    workflow_lease = _workflow_lease()
    independent_lease = TaskLease(
        task_id=uuid4(),
        tenant_id="tenant-a",
        actor_id="user-7",
        origin_turn_id="turn-8",
        agent_name="existing-agent",
        executor_kind=ExecutorKind.AGENT,
        input={"instructions": "keep existing behavior"},
        attempt_count=1,
        lease_generation=1,
        worker_id="worker-1",
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
    )

    assert await router.execute(independent_lease) == {"response": "independent"}
    assert await router.execute(workflow_lease) == {"response": "workflow"}
    assert calls == [
        ("independent", independent_lease),
        ("workflow", workflow_lease),
    ]


@pytest.mark.asyncio
async def test_claim_adds_trusted_workflow_context_only_to_workflow_tasks(
    ledger,
    postgres_pool,
) -> None:
    principal = Principal(
        actor_id="user-7",
        tenant_id="tenant-a",
        scopes=frozenset(),
    )
    store = PostgresWorkflowStore(postgres_pool)
    await store.publish(
        WorkflowDefinition(
            key="test.bounded_reasoning",
            version=1,
            input_contract=(
                FieldContract(name="question", value_type=FieldType.STRING),
                FieldContract(name="evidence", value_type=FieldType.STRING),
                FieldContract(name="constraints", value_type=FieldType.STRING),
            ),
            entry_step=StepTemplate(
                key="decide",
                agent_name="bounded-reasoning-manager",
                executor_kind=ExecutorKind.AGENT,
            ),
        )
    )
    started = await store.start(
        principal,
        WorkflowStartCommand(
            idempotency_key="reasoning-workflow-1",
            origin_turn_id="turn-7",
            definition_key="test.bounded_reasoning",
            definition_version=1,
            input={
                "question": "Ship?",
                "evidence": "Tests pass.",
                "constraints": "Keep it small.",
            },
        ),
    )

    workflow_lease = await ledger.claim(
        "worker-1",
        timedelta(seconds=30),
        executor_kind=ExecutorKind.AGENT,
    )

    assert workflow_lease is not None
    assert workflow_lease.workflow_instance_id == started.instance.instance_id
    assert workflow_lease.workflow_step_id == started.step.step_id

    await ledger.complete(
        workflow_lease,
        {
            "response": "Ship.",
            "evidence": ["Tests pass."],
            "risks": [],
            "confidence": "high",
        },
    )
    await ledger.submit(
        principal,
        SubmitTask(
            idempotency_key="independent-1",
            origin_turn_id="turn-8",
            agent_name="existing-agent",
            executor_kind=ExecutorKind.AGENT,
            input={"instructions": "keep existing behavior"},
        ),
    )

    independent_lease = await ledger.claim(
        "worker-1",
        timedelta(seconds=30),
        executor_kind=ExecutorKind.AGENT,
    )

    assert independent_lease is not None
    assert independent_lease.workflow_instance_id is None
    assert independent_lease.workflow_step_id is None


@pytest.mark.asyncio
async def test_seeded_reasoning_workflow_completes_as_one_typed_step(
    ledger,
    postgres_pool,
) -> None:
    principal = Principal(
        actor_id="user-7",
        tenant_id="tenant-a",
        scopes=frozenset(),
    )
    store = PostgresWorkflowStore(postgres_pool)
    started = await store.start(
        principal,
        WorkflowStartCommand(
            idempotency_key="seeded-reasoning-1",
            origin_turn_id="turn-9",
            definition_key="openpoke.reasoning_demo",
            definition_version=1,
            input={
                "question": "Should we ship the durable queue?",
                "evidence": "Crash recovery tests pass.",
                "constraints": "One-day implementation.",
            },
        ),
    )

    class UnexpectedIndependentExecutor:
        async def execute(self, _lease):
            raise AssertionError("reasoning Workflow used legacy execution")

    executor = WorkflowAwareAgentExecutor(
        independent=UnexpectedIndependentExecutor(),
        bounded_reasoning=BoundedReasoningExecutor(model=ScriptedCoordinationModel()),
    )
    worker = TaskWorker(
        ledger,
        ExecutorRegistry({ExecutorKind.AGENT: executor}),
        worker_id="worker-1",
        lease_duration=timedelta(seconds=30),
    )

    outcome = await worker.run_once(executor_kind=ExecutorKind.AGENT)

    assert outcome.status is WorkerOutcomeStatus.COMPLETED
    task = await ledger.get(
        principal.tenant_id,
        started.task.task_id,
    )
    assert task is not None
    assert task.result == {
        "response": "Ship the bounded durable slice.",
        "evidence": ["The ledger survives worker restarts."],
        "risks": ["A provider call can replay."],
        "confidence": "high",
    }
    assert (
        await postgres_pool.fetchval(
            """
        SELECT status
        FROM workflow_instances
        WHERE instance_id = $1
        """,
            started.instance.instance_id,
        )
        == "completed"
    )


@pytest.mark.asyncio
async def test_conversation_origin_receives_typed_reasoning_result(
    ledger,
    postgres_pool,
) -> None:
    principal = Principal(
        actor_id="user-7",
        tenant_id="tenant-a",
        scopes=frozenset(),
    )
    threads = PostgresThreadLedger(postgres_pool)
    await threads.append_message(
        principal,
        message_id="message-reasoning-1",
        content="Analyze whether we should ship.",
    )
    run_lease = await threads.claim_run(
        "orchestrator-1",
        timedelta(seconds=30),
    )
    assert run_lease is not None
    store = PostgresWorkflowStore(
        postgres_pool,
        run_authority=threads,
    )
    started = await store.start(
        principal,
        WorkflowStartCommand(
            idempotency_key="conversation-reasoning-1",
            origin_turn_id="message-reasoning-1",
            definition_key="openpoke.reasoning_demo",
            definition_version=1,
            input={
                "question": "Should we ship the durable queue?",
                "evidence": "Crash recovery tests pass.",
                "constraints": "One-day implementation.",
            },
        ),
        run_lease,
    )
    await threads.complete_run(
        run_lease,
        response="I started the reasoning workflow.",
    )

    class UnexpectedIndependentExecutor:
        async def execute(self, _lease):
            raise AssertionError("reasoning Workflow used legacy execution")

    worker = TaskWorker(
        ledger,
        ExecutorRegistry(
            {
                ExecutorKind.AGENT: WorkflowAwareAgentExecutor(
                    independent=UnexpectedIndependentExecutor(),
                    bounded_reasoning=BoundedReasoningExecutor(
                        model=ScriptedCoordinationModel()
                    ),
                )
            }
        ),
        worker_id="worker-1",
        lease_duration=timedelta(seconds=30),
    )

    outcome = await worker.run_once(executor_kind=ExecutorKind.AGENT)

    assert outcome.status is WorkerOutcomeStatus.COMPLETED
    completed = await ledger.get(
        principal.tenant_id,
        started.task.task_id,
    )
    assert completed is not None
    assert completed.result is not None
    assert completed.result["response"] == (
        "Ship the bounded durable slice."
    )
    messages = await threads.list_messages(principal)
    assert messages[-1].content == (
        "[SUCCESS] bounded-reasoning-manager: "
        "Ship the bounded durable slice."
    )
    assert (
        await postgres_pool.fetchval(
            """
            SELECT count(*)
            FROM agent_runs
            WHERE thread_id = $1 AND status = 'queued'
            """,
            run_lease.thread_id,
        )
        == 1
    )


@pytest.mark.asyncio
async def test_transient_child_failure_replays_under_three_attempt_policy(
    ledger,
    postgres_pool,
) -> None:
    principal = Principal(
        actor_id="user-7",
        tenant_id="tenant-a",
        scopes=frozenset(),
    )
    started = await PostgresWorkflowStore(postgres_pool).start(
        principal,
        WorkflowStartCommand(
            idempotency_key="retry-reasoning-1",
            origin_turn_id="turn-retry",
            definition_key="openpoke.reasoning_demo",
            definition_version=1,
            input={
                "question": "Should we ship the durable queue?",
                "evidence": "Crash recovery tests pass.",
                "constraints": "One-day implementation.",
            },
        ),
    )

    class UnexpectedIndependentExecutor:
        async def execute(self, _lease):
            raise AssertionError("reasoning Workflow used legacy execution")

    def worker_for(model: Model, attempt: int) -> TaskWorker:
        return TaskWorker(
            ledger,
            ExecutorRegistry(
                {
                    ExecutorKind.AGENT: WorkflowAwareAgentExecutor(
                        independent=UnexpectedIndependentExecutor(),
                        bounded_reasoning=BoundedReasoningExecutor(model=model),
                    )
                }
            ),
            worker_id=f"worker-{attempt}",
            lease_duration=timedelta(seconds=30),
        )

    first = await worker_for(
        ScriptedCoordinationModel(failing_specialist="risk"),
        1,
    ).run_once(executor_kind=ExecutorKind.AGENT)
    second = await worker_for(
        ScriptedCoordinationModel(failing_specialist="risk"),
        2,
    ).run_once(executor_kind=ExecutorKind.AGENT)
    third = await worker_for(
        ScriptedCoordinationModel(),
        3,
    ).run_once(executor_kind=ExecutorKind.AGENT)

    assert first.status is WorkerOutcomeStatus.RETRIED
    assert first.failure is FailureCode.AGENT_RETRYABLE
    assert second.status is WorkerOutcomeStatus.RETRIED
    assert third.status is WorkerOutcomeStatus.COMPLETED
    completed = await ledger.get(
        principal.tenant_id,
        started.task.task_id,
    )
    assert completed is not None
    assert completed.attempt_count == 3
    assert (
        await postgres_pool.fetchval(
            """
            SELECT status
            FROM workflow_instances
            WHERE instance_id = $1
            """,
            started.instance.instance_id,
        )
        == "completed"
    )
