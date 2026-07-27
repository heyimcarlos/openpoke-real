# Task control-plane boundary

Issue #2 introduces an in-process control-plane seam. It does not expose a
public raw execution-task API.

```text
public message/business endpoint
  -> verify JWT
  -> Principal(actor, tenant, scopes)
  -> trusted code builds SubmitTask(cause, agent, input)
  -> TaskService authorizes create/read
  -> PostgresTaskLedger persists or queries tenant-owned work
```

The command cannot contain `tenant_id` or `actor_id`. Those values come only
from the verified `Principal` and are stored with the task for attribution.
Cross-tenant reads return no record.

The current JWT verifier fixes the accepted algorithm to HS256 and validates
signature, issuer, audience, expiry, subject, and tenant claims. The signing key
must come from runtime secret management; it must never be stored in source.
When authentication moves to an external identity provider, this boundary can
replace the shared-key verifier with asymmetric key discovery without changing
the task service or ledger.

The trusted task command is an ordinary module call while the API and ledger
live in one process. If the control plane becomes a separate deployment later,
service identity must authenticate that hop in addition to preserving the
original user `Principal`.

## Deliberate limits

- Task input is JSON and limited to 16 KiB after serialization.
- Idempotency is scoped to a tenant. Exact semantic replay returns the original
  task; conflicting reuse is rejected.
- This issue accepts and queries queued tasks only.
- Worker claims, leases, completion, retries, dead letters, admission limits,
  and cancellation belong to later issues.
