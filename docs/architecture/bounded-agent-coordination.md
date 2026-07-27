# Bounded agent coordination

One durable reasoning Workflow Step owns one execution task and lease. Inside
that Attempt, the OpenAI Agents SDK runs a manager that calls two specialists
with `Agent.as_tool()`. Specialists never own the user conversation and cannot
change the published Workflow.

The manager and specialists share these attempt-local limits:

- 4 manager turns
- 2 turns per specialist
- 6 model requests in total
- 2 specialist calls in total
- 2 concurrent local tool calls
- 600 output tokens per model request

The typed manager and specialist results require a model endpoint that supports
JSON Schema structured outputs. Bounded reasoning therefore has its own
`OPENPOKE_REASONING_AGENT_MODEL` setting, defaulting to
`openai/gpt-4.1-mini`. Independent execution keeps its existing model setting.

Specialist inputs and outputs are Pydantic contracts. The Workflow kernel sees
only the manager's validated `ReasoningStepResult`. Any child failure fails the
whole task Attempt. PostgreSQL then applies the existing fenced completion and
failure policy. Transient provider or child runtime failures may replay under
the global three-attempt limit. Contract, turn, budget, and guardrail failures
are non-retryable.

Tracing is disabled by default. A trusted worker setting can enable it. Enabled
traces exclude prompt and tool data and contain only Workflow Instance, Step,
execution task, Attempt, and lease correlation IDs. Tenant, actor, prompt, and
tool payloads are not trace metadata.

The current task ID plus attempt count identifies the Attempt. The task ID plus
lease generation identifies its fenced lease. No second child-attempt ledger is
introduced inside the SDK run.

## Promotion rule

Keep child coordination inside the SDK Attempt while it is temporary reasoning.
Promote a child into a durable Workflow Step when it needs independent recovery,
retry, cancellation, inspection, approval, or side-effect identity.

The runtime has no child DAG or Promise scheduler. Published Workflow
Definitions remain the only source of durable structure.
