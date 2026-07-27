# Execution durability and overload evidence

Synthetic work only. No model-provider executor is configured.

## Reproduce

```bash
docker compose --env-file compose.test.env -f compose.test.yaml up -d --wait
uv run --locked python -m scripts.run_execution_evidence --report docs/evidence/execution-load.md
```

- Producing code revision: `f7c6c0e`
- Report commit: `a4a55c7`
- Seed: `20260727`
- Duration: `4.311s`
- Exact command: `uv run --locked python -m scripts.run_execution_evidence --report docs/evidence/execution-load.md`
- Machine: `Linux-7.1.4-arch1-1-x86_64-with-glibc2.43`
- CPUs: `32`
- Python: `3.11.15`
- PostgreSQL: `16.14`

## Workload

The 1, 10, 50, and 100 client scenarios release their initial submissions from one barrier. Larger cases use one noisy tenant, one later quiet task, and four filler tenants. Workers start only after initial admission. The quiet task is admitted after PostgreSQL observes two active noisy Attempts and a queued noisy backlog. A synthetic gate holds those noisy claims until that observation, then releases them. Sixteen local claimant coroutines deliberately create contention against ledger caps of eight globally and two per tenant. Their 18-connection database pool is a test-harness setting, not the production worker capacity. The fixed seed places retry and dead-letter failures on guaranteed filler tasks.

## Scenario results

| Clients | Accepted | Rejected | Submit/s | Submit p50/p95/p99 ms | Queue p50/p95/p99 ms | End-to-end p50/p95/p99 ms | Backlog ms | Active | Retries | DLQ |
| ---: | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: |
| 1 | 1 | 0 | 163.681 | 4.82/4.82/4.82 | 22.328/22.328/22.328 | 45.49/45.49/45.49 | 21.058 | 1 | 0 | 0 |
| 10 | 10 | 0 | 87.502 | 12.2/20.43/20.43 | 55.219/133.48/133.48 | 100.407/161.878/161.878 | 131.47 | 4 | 3 | 1 |
| 50 | 50 | 0 | 270.476 | 28.651/46.15/47.893 | 229.764/511.246/539.843 | 256.217/539.217/567.638 | 540.593 | 8 | 3 | 1 |
| 100 | 90 | 10 | 593.373 | 31.863/58.975/61.169 | 229.881/759.988/810.965 | 251.363/786.153/837.411 | 810.715 | 8 | 3 | 1 |

## Tenant throughput and pool use

| Clients | Tenant | Accepted | Rejected | Terminal | Tasks/s |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | single | 1 | 0 | 1 | 5.495 |
| 10 | filler-1 | 2 | 0 | 2 | 9.142 |
| 10 | filler-2 | 1 | 0 | 1 | 4.571 |
| 10 | filler-3 | 1 | 0 | 1 | 4.571 |
| 10 | filler-4 | 1 | 0 | 1 | 4.571 |
| 10 | noisy | 4 | 0 | 4 | 18.285 |
| 10 | quiet | 1 | 0 | 1 | 4.571 |
| 50 | filler-1 | 5 | 0 | 5 | 7.735 |
| 50 | filler-2 | 5 | 0 | 5 | 7.735 |
| 50 | filler-3 | 5 | 0 | 5 | 7.735 |
| 50 | filler-4 | 4 | 0 | 4 | 6.188 |
| 50 | noisy | 30 | 0 | 30 | 46.409 |
| 50 | quiet | 1 | 0 | 1 | 1.547 |
| 100 | filler-1 | 10 | 0 | 10 | 10.87 |
| 100 | filler-2 | 10 | 0 | 10 | 10.87 |
| 100 | filler-3 | 10 | 0 | 10 | 10.87 |
| 100 | filler-4 | 9 | 0 | 9 | 9.783 |
| 100 | noisy | 50 | 10 | 50 | 54.35 |
| 100 | quiet | 1 | 0 | 1 | 1.087 |

| Clients | Acceptance DB pool peak/max | Worker DB pool peak/max |
| ---: | ---: | ---: |
| 1 | 1/5 | 17/18 |
| 10 | 5/5 | 16/18 |
| 50 | 5/5 | 17/18 |
| 100 | 5/5 | 17/18 |

## Recovery evidence

A separate acceptance process committed three commands and exited. A fresh process replayed them to the same task IDs. Separate worker processes then completed them.

A worker was SIGKILLed after reporting its committed Attempt-one lease. PostgreSQL time established expiry. A fresh worker claimed Attempt two. The stale completion was rejected while Attempt two was active, and the winner result remained unchanged.

- Recovery duration: `1068.315ms`
- Replacement attempt: `2`
- Stale results rejected: `1`

## Correctness gates

- [x] acceptance replay returns same task ids
- [x] acceptance survives process exit
- [x] killed worker reclaimed as attempt two
- [x] worker process was killed by sigkill
- [x] stale completion rejected
- [x] all accepted tasks terminal and queryable
- [x] sampled global active never exceeds eight
- [x] sampled tenant active never exceeds two
- [x] noisy tenant admits fifty and rejects ten
- [x] later quiet tenant progresses before noisy finishes
- [x] only synthetic executors used

The CLI exits nonzero on correctness failures. Timing values are observations, never gates. Existing concurrent PostgreSQL claim tests remain the transactional capacity proof. Active Attempts, backlog age, and pool use are sampled operational evidence.

## Limitations

- Local synthetic measurements do not establish a Cloud Run SLO.
- Very small scenario p99 values have little statistical value.
- Retry timing is not broken into separate per-Attempt spans.
- Public chat durability is outside this internal acceptance seam.
- PostgreSQL is both ledger and queue. RabbitMQ wake-ups, an outbox, and workflow dependencies remain later architecture.
