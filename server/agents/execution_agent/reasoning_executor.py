"""Bounded Agents SDK coordination inside one durable Workflow Step."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Any, Literal

from agents import (
    Agent,
    FunctionTool,
    MaxTurnsExceeded,
    Model,
    ModelBehaviorError,
    ModelSettings,
    RunConfig,
    RunHooks,
    Runner,
    ToolGuardrailFunctionOutput,
    ToolExecutionConfig,
    ToolInputGuardrail,
    ToolInputGuardrailData,
    ToolInputGuardrailTripwireTriggered,
    function_tool,
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue, ValidationError

from ...services.task_queue import (
    ExecutionFailure,
    ExecutionSuspended,
    FailureCode,
    RunStateIncompatible,
    TaskFailure,
    TaskLease,
)
from .run_state_codec import AgentsRunStateCodec


BOUNDED_REASONING_APPROVAL_AGENT_NAME = (
    "bounded-reasoning-approval-manager"
)
APPROVAL_AGENT_DEFINITION_VERSION = (
    "bounded-reasoning-approval-manager:v1"
)
APPROVAL_WAIT_KEY = "approval"
APPROVAL_TOOL_NAME = "commit_recommendation"


@function_tool(
    name_override=APPROVAL_TOOL_NAME,
    needs_approval=True,
)
async def _commit_recommendation(summary: str) -> str:
    """Record the approved recommendation inside this bounded Step."""

    return f"Approved recommendation: {summary}"


class ReasoningStepInput(BaseModel):
    """Published business input for the bounded reasoning demo."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    question: str = Field(min_length=1, max_length=4_000)
    evidence: str = Field(min_length=1, max_length=8_000)
    constraints: str = Field(min_length=1, max_length=4_000)


class EvidenceSpecialistInput(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    question: str = Field(min_length=1, max_length=4_000)
    available_evidence: str = Field(min_length=1, max_length=8_000)


class RiskSpecialistInput(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    proposal: str = Field(min_length=1, max_length=4_000)
    constraints: str = Field(min_length=1, max_length=4_000)


class EvidenceSpecialistResult(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    findings: tuple[str, ...] = Field(min_length=1, max_length=8)
    confidence: Literal["low", "medium", "high"]


class RiskSpecialistResult(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    risks: tuple[str, ...] = Field(min_length=1, max_length=8)
    mitigation: str = Field(min_length=1, max_length=2_000)


class ReasoningStepResult(BaseModel):
    """The single typed result visible to the durable Workflow kernel."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    response: str = Field(min_length=1, max_length=4_000)
    evidence: tuple[str, ...] = Field(min_length=1, max_length=8)
    risks: tuple[str, ...] = Field(max_length=8)
    confidence: Literal["low", "medium", "high"]


@dataclass(frozen=True)
class ReasoningLimits:
    manager_max_turns: int = 4
    specialist_max_turns: int = 2
    max_model_requests: int = 6
    max_specialist_calls: int = 2
    max_output_tokens: int = 600
    max_local_tool_concurrency: int = 2

    def __post_init__(self) -> None:
        if (
            min(
                self.manager_max_turns,
                self.specialist_max_turns,
                self.max_model_requests,
                self.max_specialist_calls,
                self.max_output_tokens,
                self.max_local_tool_concurrency,
            )
            < 1
        ):
            raise ValueError("reasoning limits must be positive")


class _ModelRequestBudgetExceeded(RuntimeError):
    pass


class _ModelRequestBudget(RunHooks):
    """One aggregate request budget shared by manager and nested agents."""

    def __init__(self, maximum: int, *, initial_used: int = 0) -> None:
        if not 0 <= initial_used <= maximum:
            raise ValueError("initial model request usage exceeds its limit")
        self._maximum = maximum
        self._used = initial_used
        self._lock = asyncio.Lock()

    @property
    def used(self) -> int:
        return self._used

    async def on_llm_start(
        self,
        context,
        agent,
        system_prompt,
        input_items,
    ) -> None:
        del context, agent, system_prompt, input_items
        async with self._lock:
            if self._used >= self._maximum:
                raise _ModelRequestBudgetExceeded
            self._used += 1


class _SpecialistCallBudget:
    """Bound child starts and reject repeated SDK call identities."""

    def __init__(self, maximum: int, *, initial_used: int = 0) -> None:
        if not 0 <= initial_used <= maximum:
            raise ValueError("initial specialist usage exceeds its limit")
        self._maximum = maximum
        self._initial_used = initial_used
        self._charged_call_ids: set[str] = set()
        self._anonymous_calls = 0
        self._lock = asyncio.Lock()

    @property
    def used(self) -> int:
        return (
            self._initial_used
            + len(self._charged_call_ids)
            + self._anonymous_calls
        )

    async def reserve(self, call_id: str | None) -> bool:
        async with self._lock:
            if call_id and call_id in self._charged_call_ids:
                return False
            if self.used >= self._maximum:
                return False
            if call_id:
                self._charged_call_ids.add(call_id)
            else:
                self._anonymous_calls += 1
            return True


class BoundedReasoningExecutor:
    """Run one trusted reasoning Step as a bounded SDK manager run."""

    def __init__(
        self,
        *,
        model: Model,
        runner: Any = Runner,
        limits: ReasoningLimits = ReasoningLimits(),
        tracing_enabled: bool = False,
        run_state_store: Any | None = None,
    ) -> None:
        self._model = model
        self._runner = runner
        self._limits = limits
        self._tracing_enabled = tracing_enabled
        self._run_state_store = run_state_store

    async def execute(self, lease: TaskLease) -> dict[str, JsonValue]:
        if lease.workflow_instance_id is None or lease.workflow_step_id is None:
            raise ExecutionFailure(TaskFailure(code=FailureCode.AGENT_NON_RETRYABLE))
        task_input = ReasoningStepInput.model_validate(lease.input)
        approval_enabled = (
            lease.agent_name == BOUNDED_REASONING_APPROVAL_AGENT_NAME
        )
        if approval_enabled and self._run_state_store is None:
            raise ExecutionFailure(
                TaskFailure(code=FailureCode.AGENT_NON_RETRYABLE)
            )
        codec = AgentsRunStateCodec(
            agent_definition_version=APPROVAL_AGENT_DEFINITION_VERSION
        )
        snapshot = None
        if approval_enabled:
            try:
                snapshot = await self._run_state_store.load_suspension(
                    lease,
                    codec.compatibility,
                )
            except RunStateIncompatible as exc:
                raise ExecutionFailure(
                    TaskFailure(code=FailureCode.AGENT_NON_RETRYABLE)
                ) from exc
        try:
            budget = _ModelRequestBudget(
                self._limits.max_model_requests,
                initial_used=(
                    snapshot.model_requests_used if snapshot is not None else 0
                ),
            )
            specialist_budget = _SpecialistCallBudget(
                self._limits.max_specialist_calls,
                initial_used=(
                    snapshot.specialist_calls_used if snapshot is not None else 0
                ),
            )
        except ValueError as exc:
            raise ExecutionFailure(
                TaskFailure(code=FailureCode.AGENT_NON_RETRYABLE)
            ) from exc
        model_settings = ModelSettings(
            max_tokens=self._limits.max_output_tokens,
            parallel_tool_calls=True,
        )

        evidence_agent = Agent(
            name="Evidence specialist",
            instructions=(
                "ROLE: evidence specialist\n"
                "Analyze only the supplied evidence. Return the typed result."
            ),
            model=self._model,
            model_settings=model_settings,
            output_type=EvidenceSpecialistResult,
            handoffs=[],
        )
        risk_agent = Agent(
            name="Risk specialist",
            instructions=(
                "ROLE: risk specialist\n"
                "Identify concrete risks and one mitigation. "
                "Return the typed result."
            ),
            model=self._model,
            model_settings=model_settings,
            output_type=RiskSpecialistResult,
            handoffs=[],
        )
        manager_tools = [
            _bound_specialist_calls(
                evidence_agent.as_tool(
                    tool_name="analyze_evidence",
                    tool_description=(
                        "Analyze supplied evidence for the exact question."
                    ),
                    custom_output_extractor=_typed_output_json,
                    max_turns=self._limits.specialist_max_turns,
                    hooks=budget,
                    failure_error_function=None,
                    parameters=EvidenceSpecialistInput,
                ),
                specialist_budget,
            ),
            _bound_specialist_calls(
                risk_agent.as_tool(
                    tool_name="review_risks",
                    tool_description=(
                        "Review concrete risks for a proposal and constraints."
                    ),
                    custom_output_extractor=_typed_output_json,
                    max_turns=self._limits.specialist_max_turns,
                    hooks=budget,
                    failure_error_function=None,
                    parameters=RiskSpecialistInput,
                ),
                specialist_budget,
            ),
        ]
        if approval_enabled:
            manager_tools.append(_commit_recommendation)
        manager = Agent(
            name="Bounded reasoning manager",
            instructions=(
                "ROLE: manager\n"
                "Own the final answer. Use both specialists as tools, then "
                "return one typed result. Never invent or change Workflow "
                "Steps. When commit_recommendation is available, call it "
                "after specialist review and before returning the result."
            ),
            model=self._model,
            model_settings=model_settings,
            output_type=ReasoningStepResult,
            handoffs=[],
            tools=manager_tools,
        )
        run_config = RunConfig(
            tracing_disabled=not self._tracing_enabled,
            trace_include_sensitive_data=False,
            workflow_name="openpoke.reasoning_step",
            group_id=str(lease.workflow_instance_id),
            trace_metadata={
                "workflow_instance_id": str(lease.workflow_instance_id),
                "workflow_step_id": str(lease.workflow_step_id),
                "execution_task_id": str(lease.task_id),
                "task_attempt_id": (f"{lease.task_id}:{lease.attempt_count}"),
                "task_lease_id": (f"{lease.task_id}:{lease.lease_generation}"),
            },
            tool_execution=ToolExecutionConfig(
                max_function_tool_concurrency=(self._limits.max_local_tool_concurrency)
            ),
        )
        try:
            run_input: Any = task_input.model_dump_json()
            run_context: Any = None
            if approval_enabled:
                if snapshot is None:
                    run_context = codec.context_for(lease)
                else:
                    run_input = await codec.resume(
                        snapshot,
                        manager,
                        lease,
                        approval_tool_name=APPROVAL_TOOL_NAME,
                    )
            result = await self._runner.run(
                manager,
                run_input,
                context=run_context,
                max_turns=self._limits.manager_max_turns,
                hooks=budget,
                run_config=run_config,
            )
            if result.interruptions:
                if not approval_enabled or snapshot is not None:
                    raise RunStateIncompatible(
                        "unexpected Agents SDK interruption"
                    )
                raise ExecutionSuspended(
                    codec.suspend(
                        result,
                        lease,
                        wait_key=APPROVAL_WAIT_KEY,
                        approval_tool_name=APPROVAL_TOOL_NAME,
                        model_requests_used=budget.used,
                        specialist_calls_used=specialist_budget.used,
                    )
                )
            output = ReasoningStepResult.model_validate(result.final_output)
        except (
            MaxTurnsExceeded,
            ModelBehaviorError,
            ToolInputGuardrailTripwireTriggered,
            ValidationError,
            _ModelRequestBudgetExceeded,
            RunStateIncompatible,
        ) as exc:
            raise ExecutionFailure(
                TaskFailure(code=FailureCode.AGENT_NON_RETRYABLE)
            ) from exc
        except Exception as exc:
            if isinstance(exc, (ExecutionFailure, ExecutionSuspended)):
                raise
            raise ExecutionFailure(
                TaskFailure(code=FailureCode.AGENT_RETRYABLE)
            ) from exc
        return output.model_dump(mode="json")


async def _typed_output_json(result: Any) -> str:
    output = result.final_output
    if not isinstance(
        output,
        (EvidenceSpecialistResult, RiskSpecialistResult),
    ):
        raise TypeError("specialist returned an invalid result")
    return output.model_dump_json()


def _bound_specialist_calls(
    tool: FunctionTool,
    budget: _SpecialistCallBudget,
) -> FunctionTool:
    async def enforce_budget(
        data: ToolInputGuardrailData,
    ) -> ToolGuardrailFunctionOutput:
        call_id = getattr(data.context, "tool_call_id", None)
        allowed = await budget.reserve(call_id if isinstance(call_id, str) else None)
        if allowed:
            return ToolGuardrailFunctionOutput.allow()
        return ToolGuardrailFunctionOutput.raise_exception(
            {"reason": "specialist_call_budget_exhausted"}
        )

    return replace(
        tool,
        tool_input_guardrails=[
            ToolInputGuardrail(
                guardrail_function=enforce_budget,
                name="specialist_call_budget",
            )
        ],
    )
