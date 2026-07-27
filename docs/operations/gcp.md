# GCP operating contract

This is the deployment target for the reviewed single-day durability slice. It
is not a claim that GCP resources, autoscaling, or the ideal workflow system
already exist.

## Architecture boundaries

### Current repository

```text
Browser -> Next.js proxy -> one FastAPI process
                              |
                              +-> PostgreSQL task ledger
                              |
                              +-> file-backed chat, scheduler, and watcher

PostgreSQL task ledger <- separate execution worker
```

PostgreSQL durably owns accepted tasks, attempts, leases, and fenced results.
Chat history, the interaction orchestrator, the scheduler, and the watcher still
have process-local or file-backed state. Run one API replica until those
components become tenant-scoped and durable.

### Single-day GCP target

```text
IAM-authenticated internal caller
  |
Private Cloud Run API service
  |
  +----------------------> Cloud SQL PostgreSQL
                               ^
                               |
Private Cloud Run worker pool -+
                               |
One-task Cloud Run migration Job
```

The API, worker, and migrator run the same immutable backend image digest with
different commands:

```text
API:      .venv/bin/python -m server.server --host 0.0.0.0 --port 8080
Worker:   .venv/bin/python -m server.worker --concurrency 2
Migrator: .venv/bin/python -m server.migrate
```

The initial API service accepts only IAM-authenticated internal traffic because
several existing Gmail routes do not yet enforce end-user authentication.
Worker pools have no public HTTP ingress. Cloud SQL has no public IP.

### Ideal growth path

```text
Verified public channel -> load balancer -> authenticated public API
                                            |
                                            v
                              durable message/turn
                                            |
                                            v
                              interaction orchestrator pool
                                      |
                                      v
                              workflow command
                                      |
                                      v
PostgreSQL workflow kernel -> runnable steps -> execution worker pools
              |
              +-> transactional outbox -> relay -> RabbitMQ wake-up
```

PostgreSQL remains authoritative. RabbitMQ wakes workers but does not decide
task state or workflow order. The durable workflow kernel owns definitions,
instances, steps, attempts, waits, signals, and dependency transitions.

The API authenticates and persists messages. A disposable interaction
orchestrator may answer directly, submit bounded independent work, or start a
published Workflow with typed inputs. The deterministic workflow kernel decides
which Steps are runnable. Execution workers run deterministic code or an Agents
SDK reasoning executor, while specialist-agent coordination remains inside one
leased reasoning Step.

## Capacity and connection budget

The initial one-API deployment has this maximum connection budget:

| Role | Initial instances | Pool per instance | Maximum connections |
| --- | ---: | ---: | ---: |
| API | 1 | 5 | 5 |
| Worker | 4 | 4 | 16 |
| Migrator Job | 1 | 1 | 1 |
| Initial maximum | | | 22 |

After issue #8 externalizes chat and orchestrator state, the approved capacity
envelope becomes:

| Role | Instance envelope | Pool per instance | Maximum connections |
| --- | ---: | ---: | ---: |
| API | 4 | 5 | 20 |
| Worker | 4 | 4 | 16 |
| Migrator Job | 1 | 1 | 1 |
| Post-#8 maximum | | | 37 |

The application operating budget is 50 connections. The initial maximum leaves
28 connections of headroom. The post-#8 envelope leaves 13. Cloud SQL system
connections and administrator access need a separate instance-level allowance.

The initial API deployment has one replica because chat and scheduler state are
not safe for multiple replicas. Four API replicas are the connection-budget
envelope after that state is externalized. The worker pool runs one to four
instances with two execution slots each. Its maximum capacity is therefore
eight, matching the PostgreSQL global active-task cap. The per-tenant active cap
remains two.

## Identities and secrets

Use four service accounts:

| Identity | Allowed | Not allowed |
| --- | --- | --- |
| API | connect as API database user, read API-scoped Secret Manager versions | DDL, worker secrets, deployment, unauthenticated public ingress |
| Worker | connect as worker database user, read execution provider and tool secrets | DDL, public ingress, deployment |
| Migrator | connect as migration database owner for the migration Job | provider secrets, serving traffic |
| Deployer | push/read release images, deploy approved revisions, act as the three runtime identities | runtime secret values, database login |

GitHub deployment authentication should use Workload Identity Federation, not a
stored service-account key. Runtime secret values are Secret Manager references,
not image layers, source files, CI variables, or command-line arguments. Cloud
Run encryption at rest, Secret Manager encryption, Cloud SQL encryption at rest,
and encrypted database connections cover the managed transport and storage
boundaries.

The current HS256 JWT verifier shares signing material. Production should verify
asymmetric tokens from an identity provider's JWKS so the API never holds the
token issuer's signing key.

## Release procedure

1. Require all four CI jobs to pass. Correctness invariants gate CI, measured
   latency does not.
2. Build the backend image once and record its immutable digest.
3. Confirm Cloud SQL backups and point-in-time recovery before schema changes.
4. Run `.venv/bin/python -m server.migrate` once as the dedicated migrator
   identity. Migrations must be backward compatible with the running revision.
5. Deploy the API command at the recorded digest, initially with one replica.
6. Deploy the private worker pool command at the same digest, with two slots per
   instance.
7. Submit an authenticated synthetic task and confirm acceptance, claim, fenced
   completion, projection, queue depth, oldest runnable age, and database
   connection use.

Do not automatically roll back database migrations. If application verification
fails, stop or roll back the API and worker revisions only when the previous
image remains schema compatible. Repair schema with a reviewed forward
migration. Restore from backup only through an explicit incident decision.

## Worker scaling policy

Cloud Run worker pools require manual scaling or an external autoscaler. Poll
the authoritative PostgreSQL runnable depth and oldest runnable age:

- Scale up promptly when depth exceeds available execution slots or oldest age
  crosses the response target.
- Choose `ceil(runnable depth / 2)`, clamped to one through four instances, then
  confirm the age trend after the change.
- Scale down one instance at a time only after runnable depth stays at zero and
  active attempts stay below the next capacity level for ten minutes.
- Never exceed four worker instances until both the eight-task global cap and
  the 50-connection operating budget are deliberately changed.

This is an operating policy, not a deployed autoscaler. Local synthetic evidence
does not establish a production Cloud Run latency SLO.

## Provisioning boundary

There is intentionally no Terraform or deployment workflow yet. Add them only
when a real GCP project, Artifact Registry repository, Cloud SQL instance,
Workload Identity Federation provider, and deployment environment can be
exercised and reviewed.
