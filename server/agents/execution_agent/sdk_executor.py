"""OpenAI Agents SDK adapter for one durable reasoning task."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from agents import Agent, FunctionTool, RunConfig, Runner
from agents.models.openai_chatcompletions import OpenAIChatCompletionsModel
from openai import AsyncOpenAI
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from ...services.task_queue.execution import ExecutionFailure
from ...services.task_queue.models import FailureCode, TaskFailure, TaskLease
from .agent import ExecutionAgent, execution_storage_key
from .tools import get_tool_registry, get_tool_schemas


class AgentExecutionOutput(BaseModel):
    """Typed result returned to the durable worker."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    response: str = Field(min_length=1)


class _AgentTaskInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    instructions: str = Field(min_length=1)
    composio_user_id: str | None = Field(default=None, max_length=256)
    execution_storage_key: str | None = Field(
        default=None,
        pattern=r"^agent-[a-f0-9]{64}$",
    )


class ToolRegistryFactory(Protocol):
    def __call__(
        self,
        agent_name: str,
        composio_user_id: str | None,
        *,
        display_agent_name: str,
        tenant_id: str,
        actor_id: str,
    ) -> Mapping[str, Callable[..., Any]]: ...


class AgentsSdkExecutor:
    """Run the existing execution prompt and tools through the SDK Runner."""

    def __init__(
        self,
        *,
        api_key: str,
        model_name: str,
        runner: Any = Runner,
        tool_schemas: Sequence[dict[str, Any]] | None = None,
        tool_registry_factory: ToolRegistryFactory = get_tool_registry,
    ) -> None:
        if not api_key:
            raise ValueError("OpenRouter API key is required")
        self._model = OpenAIChatCompletionsModel(
            model=model_name,
            openai_client=AsyncOpenAI(
                api_key=api_key,
                base_url="https://openrouter.ai/api/v1",
            ),
        )
        self._runner = runner
        self._tool_schemas = (
            list(tool_schemas) if tool_schemas is not None else get_tool_schemas()
        )
        self._tool_registry_factory = tool_registry_factory

    async def execute(self, lease: TaskLease) -> dict[str, JsonValue]:
        task_input = _AgentTaskInput.model_validate(lease.input)
        execution_key = task_input.execution_storage_key or execution_storage_key(
            lease.tenant_id,
            lease.actor_id,
            lease.agent_name,
        )
        execution_agent = ExecutionAgent(
            lease.agent_name,
            storage_key=execution_key,
        )
        tool_failed: list[bool] = []
        tools = self._build_tools(
            execution_key,
            task_input.composio_user_id,
            lease.agent_name,
            lease.tenant_id,
            lease.actor_id,
            tool_failed,
        )
        instructions = execution_agent.build_system_prompt_with_history()
        agent = Agent(
            name=lease.agent_name,
            instructions=instructions,
            model=self._model,
            tools=tools,
            output_type=AgentExecutionOutput,
        )
        result = await self._runner.run(
            agent,
            task_input.instructions,
            max_turns=8,
            run_config=RunConfig(
                tracing_disabled=True,
                trace_include_sensitive_data=False,
            ),
        )
        if tool_failed:
            raise ExecutionFailure(
                TaskFailure(code=FailureCode.AGENT_NON_RETRYABLE)
            )
        output = AgentExecutionOutput.model_validate(result.final_output)
        return {
            "agent_name": lease.agent_name,
            "response": output.response,
        }

    def _build_tools(
        self,
        agent_name: str,
        composio_user_id: str | None,
        display_agent_name: str,
        tenant_id: str,
        actor_id: str,
        tool_failed: list[bool],
    ) -> list[FunctionTool]:
        registry = self._tool_registry_factory(
            agent_name,
            composio_user_id,
            display_agent_name=display_agent_name,
            tenant_id=tenant_id,
            actor_id=actor_id,
        )
        tools: list[FunctionTool] = []
        for schema in self._tool_schemas:
            function = schema.get("function", {})
            name = function.get("name")
            if not isinstance(name, str) or name not in registry:
                continue
            tools.append(
                FunctionTool(
                    name=name,
                    description=str(function.get("description", "")),
                    params_json_schema=dict(function.get("parameters", {})),
                    on_invoke_tool=self._tool_invoker(
                        registry[name],
                        tool_failed,
                    ),
                    strict_json_schema=False,
                )
            )
        return tools

    @staticmethod
    def _tool_invoker(
        tool: Callable[..., Any],
        tool_failed: list[bool],
    ) -> Callable[[Any, str], Any]:
        async def invoke(_context: Any, raw_arguments: str) -> Any:
            try:
                arguments = json.loads(raw_arguments)
                if not isinstance(arguments, dict):
                    tool_failed.append(True)
                    return {"error": "Tool arguments must be an object"}
                if inspect.iscoroutinefunction(tool):
                    result = await tool(**arguments)
                else:
                    result = await asyncio.to_thread(tool, **arguments)
                if inspect.isawaitable(result):
                    result = await result
                if isinstance(result, dict) and "error" in result:
                    tool_failed.append(True)
                return result
            except (json.JSONDecodeError, TypeError, ValueError):
                tool_failed.append(True)
                return {"error": "Tool invocation failed"}
            except Exception:
                tool_failed.append(True)
                return {"error": "Tool execution failed"}

        return invoke
