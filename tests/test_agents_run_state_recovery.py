from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import timedelta
from importlib.metadata import version
from typing import Any

import asyncpg
import pytest
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
)
from server.services.task_queue import (
    ExecutionFailure,
    ExecutorKind,
    FailureCode,
    Principal,
    RunStateCompatibility,
    RunStateIncompatible,
    StaleLease,
    TaskSuspension,
)
from server.services.task_queue.execution import ExecutorRegistry
from server.services.task_queue.worker import TaskWorker, WorkerOutcomeStatus
from server.services.workflows import (
    FieldContract,
    FieldType,
    PostgresWorkflowStore,
    StepTemplate,
    WaitTemplate,
    WorkflowDefinition,
    WorkflowSignalCommand,
    WorkflowStartCommand,
)


PRINCIPAL = Principal(
    actor_id="user-7",
    tenant_id="tenant-a",
    scopes=frozenset({"workflows:start", "workflows:signal"}),
)
COMPATIBILITY = RunStateCompatibility(
    codec_version=1,
    agents_sdk_version=version("openai-agents"),
    agent_definition_version="approval-manager:v1",
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


class ApprovalScriptedModel(Model):
    """Provider-free model for a pause followed by a process-style resume."""

    def __init__(
        self,
        *,
        resumed: bool = False,
        resumed_specialist_call: bool = False,
        approval_summary: str = "Ship the durable recovery path.",
        manager_preamble: str | None = None,
    ) -> None:
        self.resumed = resumed
        self.resumed_specialist_call = resumed_specialist_call
        self.approval_summary = approval_summary
        self.manager_preamble = manager_preamble
        self.manager_requests = 0

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
            model_settings,
            output_schema,
            handoffs,
            tracing,
            previous_response_id,
            conversation_id,
            prompt,
        )
        if system_instructions and "ROLE: evidence specialist" in system_instructions:
            return ModelResponse(
                output=[
                    _message(
                        json.dumps(
                            {
                                "findings": ["The state is durable."],
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
            return ModelResponse(
                output=[
                    _message(
                        json.dumps(
                            {
                                "risks": ["A stale worker may return late."],
                                "mitigation": "Fence completion by lease generation.",
                            }
                        ),
                        "risk-result",
                    )
                ],
                usage=Usage(requests=1),
                response_id="risk-response",
            )

        self.manager_requests += 1
        tool_names = {tool.name for tool in tools}
        assert tool_names == {
            "analyze_evidence",
            "review_risks",
            "commit_recommendation",
        }
        if self.resumed:
            if self.resumed_specialist_call:
                return ModelResponse(
                    output=[
                        ResponseFunctionToolCall(
                            arguments=json.dumps(
                                {
                                    "question": "Recheck the decision.",
                                    "available_evidence": "Nothing changed.",
                                }
                            ),
                            call_id="extra-evidence-call",
                            name="analyze_evidence",
                            type="function_call",
                        ),
                    ],
                    usage=Usage(requests=1),
                    response_id="manager-extra-specialist",
                )
            return ModelResponse(
                output=[
                    _message(
                        json.dumps(
                            {
                                "response": "Approved recommendation committed.",
                                "evidence": ["The state is durable."],
                                "risks": ["A stale worker may return late."],
                                "confidence": "high",
                            }
                        ),
                        "manager-result",
                    )
                ],
                usage=Usage(requests=1),
                response_id="manager-final",
            )
        if self.manager_requests == 1:
            return ModelResponse(
                output=[
                    ResponseFunctionToolCall(
                        arguments=json.dumps(
                            {
                                "question": "Should we ship?",
                                "available_evidence": "Recovery tests pass.",
                            }
                        ),
                        call_id="evidence-call",
                        name="analyze_evidence",
                        type="function_call",
                    ),
                    ResponseFunctionToolCall(
                        arguments=json.dumps(
                            {
                                "proposal": "Ship the recovery path.",
                                "constraints": "Preserve lease fencing.",
                            }
                        ),
                        call_id="risk-call",
                        name="review_risks",
                        type="function_call",
                    ),
                ],
                usage=Usage(requests=1),
                response_id="manager-tools",
            )
        output = []
        if self.manager_preamble is not None:
            output.append(_message(self.manager_preamble, "manager-preamble"))
        output.append(
            ResponseFunctionToolCall(
                arguments=json.dumps({"summary": self.approval_summary}),
                call_id="approval-call",
                name="commit_recommendation",
                type="function_call",
            )
        )
        return ModelResponse(
            output=output,
            usage=Usage(requests=1),
            response_id="manager-approval",
        )

    def stream_response(self, *args, **kwargs) -> AsyncIterator[Any]:
        del args, kwargs

        async def empty() -> AsyncIterator[Any]:
            if False:
                yield None

        return empty()


async def _start_demo(
    store: PostgresWorkflowStore,
    suffix: str,
    *,
    question: str = "Should we ship?",
) -> Any:
    return await store.start(
        PRINCIPAL,
        WorkflowStartCommand(
            idempotency_key=f"sdk-{suffix}",
            origin_turn_id=f"turn-{suffix}",
            definition_key="openpoke.reasoning_approval_demo",
            definition_version=1,
            input={
                "question": question,
                "evidence": "Recovery tests pass.",
                "constraints": "Preserve lease fencing.",
            },
        ),
    )


@pytest.mark.asyncio
async def test_signal_releases_suspended_step_to_a_new_fenced_attempt(
    ledger,
    postgres_pool: asyncpg.Pool,
) -> None:
    store = PostgresWorkflowStore(postgres_pool)
    await store.publish(
        WorkflowDefinition(
            key="test.run_state_recovery",
            version=1,
            input_contract=(
                FieldContract(name="question", value_type=FieldType.STRING),
            ),
            entry_step=StepTemplate(
                key="decide",
                agent_name="bounded-reasoning-approval-manager",
                executor_kind=ExecutorKind.AGENT,
                interruption_wait_key="approval",
            ),
            waits=(
                WaitTemplate(
                    key="approval",
                    signal_key="approve",
                    input_contract=(
                        FieldContract(
                            name="approval_note",
                            value_type=FieldType.STRING,
                        ),
                    ),
                ),
            ),
        )
    )
    started = await store.start(
        PRINCIPAL,
        WorkflowStartCommand(
            idempotency_key="recover-1",
            origin_turn_id="turn-1",
            definition_key="test.run_state_recovery",
            definition_version=1,
            input={"question": "Ship it?"},
        ),
    )
    first = await ledger.claim("worker-1", timedelta(seconds=30))
    assert first is not None

    suspended = await ledger.suspend(
        first,
        TaskSuspension(
            wait_key="approval",
            compatibility=COMPATIBILITY,
            model_requests_used=0,
            specialist_calls_used=0,
            state={"$schemaVersion": "test", "context": {}},
        ),
    )

    assert suspended.task_id == first.task_id
    assert suspended.step_id == started.step.step_id
    assert suspended.attempt_count == 1
    assert suspended.lease_generation == 1
    assert suspended.compatibility == COMPATIBILITY
    assert await ledger.claim("worker-2", timedelta(seconds=30)) is None
    with pytest.raises(StaleLease):
        await ledger.complete(first, {"response": "stale"})

    with pytest.raises(ValueError):
        await store.signal(
            PRINCIPAL,
            WorkflowSignalCommand(
                idempotency_key="reject-is-not-approval",
                wait_id=suspended.wait_id,
                signal_key="approve",
                input={"approved": False},
            ),
        )
    assert (
        await store.get_wait(PRINCIPAL.tenant_id, suspended.wait_id)
    ).status.value == "open"

    signalled = await store.signal(
        PRINCIPAL,
        WorkflowSignalCommand(
            idempotency_key="approve-1",
            wait_id=suspended.wait_id,
            signal_key="approve",
            input={"approval_note": "Approved"},
        ),
    )
    second = await ledger.claim("worker-2", timedelta(seconds=30))

    assert signalled.released_step_ids == (started.step.step_id,)
    assert second is not None
    assert second.task_id == first.task_id
    assert second.attempt_count == 2
    assert second.lease_generation == 2
    with pytest.raises(StaleLease):
        await ledger.complete(first, {"response": "stale after reclaim"})
    restored = await ledger.load_suspension(second, COMPATIBILITY)
    assert restored == suspended


@pytest.mark.asyncio
async def test_real_sdk_run_state_survives_worker_restart_without_provider_calls(
    ledger,
    postgres_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-be-serialized")
    store = PostgresWorkflowStore(postgres_pool)
    started = await store.start(
        PRINCIPAL,
        WorkflowStartCommand(
            idempotency_key="sdk-restart-1",
            origin_turn_id="turn-sdk-1",
            definition_key="openpoke.reasoning_approval_demo",
            definition_version=1,
            input={
                "question": "Should we ship?",
                "evidence": "Recovery tests pass.",
                "constraints": "Preserve lease fencing.",
            },
        ),
    )
    first_worker = TaskWorker(
        ledger,
        ExecutorRegistry(
            {
                ExecutorKind.AGENT: BoundedReasoningExecutor(
                    model=ApprovalScriptedModel(),
                    run_state_store=ledger,
                )
            }
        ),
        worker_id="worker-before-restart",
    )

    first_outcome = await first_worker.run_once()

    assert first_outcome.status is WorkerOutcomeStatus.SUSPENDED
    wait_id = started.wait_targets[0].wait_id
    wait = await store.get_wait(PRINCIPAL.tenant_id, wait_id)
    assert wait is not None
    assert wait.status.value == "open"
    persisted = await postgres_pool.fetchrow(
        """
        SELECT state_json::text AS state_json,
               codec_version,
               agents_sdk_version,
               agent_definition_version,
               attempt_count,
               lease_generation,
               model_requests_used,
               specialist_calls_used
        FROM workflow_run_state_snapshots
        WHERE wait_id = $1
        """,
        wait_id,
    )
    assert persisted is not None
    serialized = persisted["state_json"]
    assert "must-not-be-serialized" not in serialized
    assert "tracing_api_key" not in serialized
    context = json.loads(serialized)["context"]["context"]
    assert context == {
        "workflow_instance_id": str(started.instance.instance_id),
        "workflow_step_id": str(started.step.step_id),
        "execution_task_id": str(started.task.task_id),
    }
    assert persisted["codec_version"] == 1
    assert persisted["agents_sdk_version"] == version("openai-agents")
    assert persisted["agent_definition_version"] == (
        "bounded-reasoning-approval-manager:v1"
    )
    assert persisted["attempt_count"] == 1
    assert persisted["lease_generation"] == 1
    assert persisted["model_requests_used"] == 4
    assert persisted["specialist_calls_used"] == 2

    await store.signal(
        PRINCIPAL,
        WorkflowSignalCommand(
            idempotency_key="sdk-approval-1",
            wait_id=wait_id,
            signal_key="approve",
            input={"approval_note": "Approved"},
        ),
    )
    restarted_worker = TaskWorker(
        ledger,
        ExecutorRegistry(
            {
                ExecutorKind.AGENT: BoundedReasoningExecutor(
                    model=ApprovalScriptedModel(resumed=True),
                    run_state_store=ledger,
                )
            }
        ),
        worker_id="worker-after-restart",
    )

    completed = await restarted_worker.run_once()

    assert completed.status is WorkerOutcomeStatus.COMPLETED
    assert completed.attempt_count == 2
    assert await postgres_pool.fetchval(
        "SELECT status FROM workflow_instances WHERE instance_id = $1",
        started.instance.instance_id,
    ) == "completed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("location", "secret"),
    [
        ("task_input", "Bearer test-secret-123"),
        ("tool_argument", "sk-proj-toolsecret123"),
        ("model_output", "ghp_modelsecret123456"),
    ],
)
async def test_secret_shaped_sdk_state_fails_closed_before_persistence(
    ledger,
    postgres_pool: asyncpg.Pool,
    caplog: pytest.LogCaptureFixture,
    location: str,
    secret: str,
) -> None:
    store = PostgresWorkflowStore(postgres_pool)
    question = secret if location == "task_input" else "Should we ship?"
    await _start_demo(store, f"secret-{location}", question=question)
    lease = await ledger.claim("secret-worker", timedelta(seconds=30))
    assert lease is not None
    model = ApprovalScriptedModel(
        approval_summary=(
            secret if location == "tool_argument" else "Ship the durable path."
        ),
        manager_preamble=secret if location == "model_output" else None,
    )
    executor = BoundedReasoningExecutor(
        model=model,
        run_state_store=ledger,
    )

    with pytest.raises(ExecutionFailure) as raised:
        await executor.execute(lease)

    assert raised.value.failure.code is FailureCode.AGENT_NON_RETRYABLE
    assert isinstance(raised.value.__cause__, RunStateIncompatible)
    assert secret not in str(raised.value.__cause__)
    assert secret not in caplog.text
    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM workflow_run_state_snapshots"
    ) == 0


@pytest.mark.asyncio
async def test_model_request_budget_is_not_reset_by_suspension(
    ledger,
    postgres_pool: asyncpg.Pool,
) -> None:
    store = PostgresWorkflowStore(postgres_pool)
    started = await _start_demo(store, "model-budget")
    limits = ReasoningLimits(max_model_requests=4)
    first_worker = TaskWorker(
        ledger,
        ExecutorRegistry(
            {
                ExecutorKind.AGENT: BoundedReasoningExecutor(
                    model=ApprovalScriptedModel(),
                    limits=limits,
                    run_state_store=ledger,
                )
            }
        ),
        worker_id="budget-worker-before",
    )
    assert (await first_worker.run_once()).status is WorkerOutcomeStatus.SUSPENDED
    await store.signal(
        PRINCIPAL,
        WorkflowSignalCommand(
            idempotency_key="model-budget-approval",
            wait_id=started.wait_targets[0].wait_id,
            signal_key="approve",
            input={"approval_note": "Approved"},
        ),
    )
    resumed_model = ApprovalScriptedModel(resumed=True)
    restarted_worker = TaskWorker(
        ledger,
        ExecutorRegistry(
            {
                ExecutorKind.AGENT: BoundedReasoningExecutor(
                    model=resumed_model,
                    limits=limits,
                    run_state_store=ledger,
                )
            }
        ),
        worker_id="budget-worker-after",
    )

    outcome = await restarted_worker.run_once()

    assert outcome.status is WorkerOutcomeStatus.DEAD_LETTERED
    assert outcome.failure is FailureCode.AGENT_NON_RETRYABLE
    assert resumed_model.manager_requests == 0


@pytest.mark.asyncio
async def test_specialist_call_budget_is_not_reset_by_suspension(
    ledger,
    postgres_pool: asyncpg.Pool,
) -> None:
    store = PostgresWorkflowStore(postgres_pool)
    started = await _start_demo(store, "specialist-budget")
    limits = ReasoningLimits(
        max_model_requests=6,
        max_specialist_calls=2,
    )
    first_worker = TaskWorker(
        ledger,
        ExecutorRegistry(
            {
                ExecutorKind.AGENT: BoundedReasoningExecutor(
                    model=ApprovalScriptedModel(),
                    limits=limits,
                    run_state_store=ledger,
                )
            }
        ),
        worker_id="specialist-worker-before",
    )
    assert (await first_worker.run_once()).status is WorkerOutcomeStatus.SUSPENDED
    await store.signal(
        PRINCIPAL,
        WorkflowSignalCommand(
            idempotency_key="specialist-budget-approval",
            wait_id=started.wait_targets[0].wait_id,
            signal_key="approve",
            input={"approval_note": "Approved"},
        ),
    )
    restarted_worker = TaskWorker(
        ledger,
        ExecutorRegistry(
            {
                ExecutorKind.AGENT: BoundedReasoningExecutor(
                    model=ApprovalScriptedModel(
                        resumed=True,
                        resumed_specialist_call=True,
                    ),
                    limits=limits,
                    run_state_store=ledger,
                )
            }
        ),
        worker_id="specialist-worker-after",
    )

    outcome = await restarted_worker.run_once()

    assert outcome.status is WorkerOutcomeStatus.DEAD_LETTERED
    assert outcome.failure is FailureCode.AGENT_NON_RETRYABLE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "incompatible_value"),
    [
        ("codec_version", 2),
        ("agents_sdk_version", "999.0.0"),
        ("agent_definition_version", "approval-manager:v2"),
    ],
)
async def test_resume_fails_closed_on_any_version_mismatch(
    ledger,
    postgres_pool: asyncpg.Pool,
    field: str,
    incompatible_value: int | str,
) -> None:
    store = PostgresWorkflowStore(postgres_pool)
    started = await store.start(
        PRINCIPAL,
        WorkflowStartCommand(
            idempotency_key=f"mismatch-{field}",
            origin_turn_id="turn-mismatch",
            definition_key="openpoke.reasoning_approval_demo",
            definition_version=1,
            input={
                "question": "Should we ship?",
                "evidence": "Recovery tests pass.",
                "constraints": "Preserve lease fencing.",
            },
        ),
    )
    first = await ledger.claim("worker-1", timedelta(seconds=30))
    assert first is not None
    suspended = await ledger.suspend(
        first,
        TaskSuspension(
            wait_key="approval",
            compatibility=COMPATIBILITY,
            model_requests_used=0,
            specialist_calls_used=0,
            state={"$schemaVersion": "test", "context": {}},
        ),
    )
    await store.signal(
        PRINCIPAL,
        WorkflowSignalCommand(
            idempotency_key=f"signal-{field}",
            wait_id=suspended.wait_id,
            signal_key="approve",
            input={"approval_note": "Approved"},
        ),
    )
    second = await ledger.claim("worker-2", timedelta(seconds=30))
    assert second is not None

    incompatible = COMPATIBILITY.model_copy(
        update={field: incompatible_value}
    )
    with pytest.raises(RunStateIncompatible):
        await ledger.load_suspension(second, incompatible)

    assert second.task_id == started.task.task_id


@pytest.mark.asyncio
async def test_persisted_run_state_is_write_once(
    ledger,
    postgres_pool: asyncpg.Pool,
) -> None:
    store = PostgresWorkflowStore(postgres_pool)
    await store.start(
        PRINCIPAL,
        WorkflowStartCommand(
            idempotency_key="immutable-state",
            origin_turn_id="turn-immutable",
            definition_key="openpoke.reasoning_approval_demo",
            definition_version=1,
            input={
                "question": "Should we ship?",
                "evidence": "Recovery tests pass.",
                "constraints": "Preserve lease fencing.",
            },
        ),
    )
    first = await ledger.claim("worker-1", timedelta(seconds=30))
    assert first is not None
    await ledger.suspend(
        first,
        TaskSuspension(
            wait_key="approval",
            compatibility=COMPATIBILITY,
            model_requests_used=0,
            specialist_calls_used=0,
            state={"$schemaVersion": "test", "context": {}},
        ),
    )

    with pytest.raises(asyncpg.RaiseError, match="RunState is immutable"):
        await postgres_pool.execute(
            """
            UPDATE workflow_run_state_snapshots
            SET codec_version = 2
            WHERE task_id = $1
            """,
            first.task_id,
        )
