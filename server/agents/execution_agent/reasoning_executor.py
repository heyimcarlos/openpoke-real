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
)
from pydantic import BaseModel, ConfigDict, Field, JsonValue, ValidationError

from ...services.task_queue import (
    ExecutionFailure,
    FailureCode,
    TaskFailure,
    TaskLease,
)


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

    def __init__(self, maximum: int) -> None:
        self._maximum = maximum
        self._used = 0
        self._lock = asyncio.Lock()

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

    def __init__(self, maximum: int) -> None:
        self._maximum = maximum
        self._charged_call_ids: set[str] = set()
        self._anonymous_calls = 0
        self._lock = asyncio.Lock()

    async def reserve(self, call_id: str | None) -> bool:
        async with self._lock:
            if call_id and call_id in self._charged_call_ids:
                return False
            used = len(self._charged_call_ids) + self._anonymous_calls
            if used >= self._maximum:
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
    ) -> None:
        self._model = model
        self._runner = runner
        self._limits = limits
        self._tracing_enabled = tracing_enabled

    async def execute(self, lease: TaskLease) -> dict[str, JsonValue]:
        if lease.workflow_instance_id is None or lease.workflow_step_id is None:
            raise ExecutionFailure(TaskFailure(code=FailureCode.AGENT_NON_RETRYABLE))
        task_input = ReasoningStepInput.model_validate(lease.input)
        budget = _ModelRequestBudget(self._limits.max_model_requests)
        specialist_budget = _SpecialistCallBudget(self._limits.max_specialist_calls)
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
        manager = Agent(
            name="Bounded reasoning manager",
            instructions=(
                "ROLE: manager\n"
                "Own the final answer. Use both specialists as tools, then "
                "return one typed result. Never invent or change Workflow "
                "Steps."
            ),
            model=self._model,
            model_settings=model_settings,
            output_type=ReasoningStepResult,
            handoffs=[],
            tools=[
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
            ],
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
            result = await self._runner.run(
                manager,
                task_input.model_dump_json(),
                max_turns=self._limits.manager_max_turns,
                hooks=budget,
                run_config=run_config,
            )
            output = ReasoningStepResult.model_validate(result.final_output)
        except (
            MaxTurnsExceeded,
            ModelBehaviorError,
            ToolInputGuardrailTripwireTriggered,
            ValidationError,
            _ModelRequestBudgetExceeded,
        ) as exc:
            raise ExecutionFailure(
                TaskFailure(code=FailureCode.AGENT_NON_RETRYABLE)
            ) from exc
        except Exception as exc:
            if isinstance(exc, ExecutionFailure):
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
