# OpenPoke

Principles belong here: contracts that should remain true while implementations
change. Current setup, commands, and the repository map live in `README.md`.
Product truth lives in `CONTEXT.md`, durable decisions in `docs/adr/`, and active
work in GitHub Issues.

## Collaboration Notes

- The user uses speech-to-text; infer likely intent from odd wording, ask only when needed.
- Code is cheap to write: no time estimates, implementation time isn't a blocker.
- Never use em-dashes anywhere. Use commas, colons, parentheses, or separate sentences.

## Reference Repositories

Repos in `.reference` (openpoke, composio, cloudflare/agents, executor.sh, …) are available for patterns. Clone a given Git URL into `.reference` and pull latest before using it.

## Engineering Priorities

- Prefer correctness and predictable behavior over short-term convenience.
- Preserve runtime behavior when changing lint, typing, or test structure.
- Keep package boundaries clear; use public package exports, not cross-package relative imports.
- Extract shared logic only when the shared behavior is real; avoid broad generic abstractions for one-off duplication.

## Agent skills

### Issue tracker

Issues and specs live in GitHub Issues. See `docs/agents/issue-tracker.md`.

### Triage labels

Use the five canonical labels unchanged. See `docs/agents/triage-labels.md`.

### Domain docs

This is a single-context repo: root `CONTEXT.md` and `docs/adr/`. See `docs/agents/domain.md`.

## Package Roles

- `server/`: FastAPI backend, agent runtimes, and durable services.
- `web/`: Next.js frontend and browser-facing API integration.

## Other

Note mistakes in MISTAKES.md, missing context or tools in DESIRES.md, and env learnings in LEARNINGS.md.
