# RabbitMQ worker wakes

PostgreSQL remains the task and Workflow authority. RabbitMQ carries only a
versioned wake token containing an event ID and executor kind. It never carries
task inputs, user content, tenant identity, credentials, or execution authority.

```text
task transaction -> execution_tasks + task_wake_outbox
                                      |
                                      v
                              confirmed relay publish
                                      |
                         agent queue / synthetic queue
                                      |
                                      v
                         PostgreSQL compatible claim
                                      |
                                      v
                            execute + fenced result
```

Run `python -m server.outbox_relay` as a separate private process. Workers with
`OPENPOKE_RABBITMQ_URL` consume wakes and retain a 30 second PostgreSQL polling
fallback. Use `--executor-kind agent` and `--executor-kind synthetic` for
separate capacity pools.

The relay retries allowlisted transport failures with leased generation
fencing. A crash after RabbitMQ confirms but before PostgreSQL records the
publish can redeliver the same event ID. This is safe because the consumer
still asks PostgreSQL for a compatible task lease. RabbitMQ transport dead
letters are separate from `execution_tasks.status = 'dead_lettered'`.

## Deterministic claim-traffic measurement

Run:

```bash
uv run --locked python -m scripts.measure_dispatch
```

For four two-slot worker processes over 60 seconds, polling every 0.5 seconds
per slot performs 960 authoritative claim calls. With four accepted tasks and a
30 second process-level safety poll, broker-assisted dispatch performs 12 calls,
a 98.75 percent reduction. This simulation is deterministic and provider-free. It demonstrates idle
database traffic reduction, not end-to-end RabbitMQ latency or production
throughput.
