# OpenPoke Durable Agent Architecture Resources

## Knowledge

- [PRD: Durable, tenant-aware execution](https://github.com/heyimcarlos/openpoke-real/issues/1)
  Product and architecture source of truth. Use for the authority, queue, worker,
  workflow, and delivery-stage boundaries.
- [Issue 8: Persist Threads and coalesced Agent Runs](https://github.com/heyimcarlos/openpoke-real/issues/8)
  Contract for durable conversation ingress, frozen cutoffs, run coalescing, and
  result continuations.
- [Issue 9: Start published Workflows through typed Commands](https://github.com/heyimcarlos/openpoke-real/issues/9)
  Contract for selecting immutable workflow structure without letting a model
  invent runtime steps.
- [OpenAI Agents SDK overview](https://developers.openai.com/api/docs/guides/agents)
  Primary source for the SDK's role as the temporary model and tool loop.
- [OpenAI Agents SDK orchestration](https://openai.github.io/openai-agents-python/multi_agent/)
  Primary source for agents-as-tools, handoffs, and code-driven orchestration.
- [OpenAI Agents SDK runner lifecycle](https://openai.github.io/openai-agents-python/running_agents/)
  Primary source for the Runner loop and explicit `max_turns` bound.
- [OpenAI Agents SDK sessions](https://openai.github.io/openai-agents-python/sessions/)
  Primary source for the custom Session protocol used by the PostgreSQL Thread
  adapter.
- [Durable Thread integration tests](https://github.com/heyimcarlos/openpoke-real/blob/260ac76530d16cec10f624e1335159f65a60bfe2/tests/test_durable_threads.py)
  Executable evidence for coalescing, frozen cutoffs, stale leases, and atomic
  result continuations.

## Wisdom (Communities)

- [OpenPoke GitHub issues](https://github.com/heyimcarlos/openpoke-real/issues)
  Best place to challenge architecture assumptions against current product
  decisions and implementation tickets.
