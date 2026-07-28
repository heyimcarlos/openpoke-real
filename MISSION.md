# Mission: OpenPoke Durable Agent Architecture

## Why
Build a reliable code-level mental model of OpenPoke before extending its
workflow system, so architecture choices preserve durable authority, bounded
execution, and recoverability.

## Success looks like
- Trace one user message from HTTP acceptance to the final durable reply.
- Explain the difference between an Agent Run queue and an execution-task queue.
- Identify which component decides that work is runnable.
- Map every architecture-roadmap stage to current code or an open GitHub issue.

## Constraints
- Use the open PRD, its architecture-direction comments, tests, and current code.
- Prefer compact call graphs and concrete code references.
- Clearly separate implemented behavior from future direction.

## Out of scope
- Designing issues 10 to 13 in detail.
- Production deployment configuration beyond what explains worker boundaries.
