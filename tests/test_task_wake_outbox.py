from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from uuid import uuid4

import asyncpg
import pytest
from pamqp.commands import Basic

from server.services.task_queue import (
    ExecutorKind,
    FailureCode,
    OutboxFailureCode,
    OutboxPublishError,
    PostgresTaskLedger,
    PostgresWakeOutbox,
    Principal,
    BoundedTaskWorkerWakeHandler,
    RabbitMQWakePublisher,
    RabbitMQWakeBroker,
    SubmitTask,
    TaskFailure,
    TaskStatus,
    WakeEvent,
    WakeOutboxRelay,
)
from server.services.task_queue.execution import ExecutorRegistry, SyntheticExecutor
from server.services.task_queue.measurement import compare_idle_claim_traffic
from server.services.task_queue.worker import TaskWorker


PRINCIPAL = Principal(
    actor_id="actor-a",
    tenant_id="tenant-a",
    scopes=frozenset({"tasks:submit"}),
)


def _command(
    key: str,
    executor_kind: ExecutorKind = ExecutorKind.SYNTHETIC,
) -> SubmitTask:
    return SubmitTask(
        idempotency_key=key,
        origin_turn_id="turn-1",
        agent_name="work",
        executor_kind=executor_kind,
        input={"mode": "success", "duration_ms": 0},
    )


@pytest.mark.asyncio
async def test_task_acceptance_atomically_appends_one_payload_free_wake(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()

    accepted = await ledger.submit(PRINCIPAL, _command("accepted-once"))
    replay = await ledger.submit(PRINCIPAL, _command("accepted-once"))
    row = await postgres_pool.fetchrow("SELECT * FROM task_wake_outbox")

    assert replay == accepted
    assert row["task_id"] == accepted.task_id
    assert row["executor_kind"] == ExecutorKind.SYNTHETIC.value
    assert row["event_version"] == 1
    assert await postgres_pool.fetchval(
        "SELECT count(*) FROM task_wake_outbox"
    ) == 1
    assert set(
        WakeEvent(
            event_id=row["event_id"],
            event_version=row["event_version"],
            executor_kind=ExecutorKind(row["executor_kind"]),
        ).model_dump(mode="json")
    ) == {"event_id", "event_version", "executor_kind"}


class _RecordingPublisher:
    def __init__(self, failures: list[OutboxPublishError] | None = None) -> None:
        self.events: list[WakeEvent] = []
        self._failures = failures or []

    async def publish(self, event: WakeEvent) -> None:
        self.events.append(event)
        if self._failures:
            raise self._failures.pop(0)


@pytest.mark.asyncio
async def test_relay_retries_allowlisted_failure_and_fences_stale_completion(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    await ledger.submit(PRINCIPAL, _command("relay-retry"))
    outbox = PostgresWakeOutbox(postgres_pool, max_attempts=3)
    first = await outbox.claim("relay-old", timedelta(seconds=30))
    assert first is not None
    await outbox.fail(
        first,
        OutboxFailureCode.UNAVAILABLE,
        retry_delay=timedelta(seconds=1),
    )
    await postgres_pool.execute(
        """
        UPDATE task_wake_outbox
        SET available_at = clock_timestamp() - interval '1 second'
        """
    )
    publisher = _RecordingPublisher()
    relay = WakeOutboxRelay(
        outbox,
        publisher,
        relay_id="relay-current",
        lease_duration=timedelta(seconds=30),
        retry_delay=timedelta(seconds=1),
    )

    outcome = await relay.run_once()

    assert outcome.value == "published"
    assert len(publisher.events) == 1
    assert await postgres_pool.fetchval(
        "SELECT failure_code FROM task_wake_outbox"
    ) is None
    assert await postgres_pool.fetchval(
        "SELECT published_at IS NOT NULL FROM task_wake_outbox"
    )
    assert not await outbox.complete(first)


@pytest.mark.asyncio
async def test_publish_then_crash_can_redeliver_same_event_id(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    await ledger.submit(PRINCIPAL, _command("redelivery"))
    outbox = PostgresWakeOutbox(postgres_pool)
    publisher = _RecordingPublisher()
    abandoned = await outbox.claim("relay-crashed", timedelta(seconds=30))
    assert abandoned is not None
    await publisher.publish(abandoned.event)
    await postgres_pool.execute(
        """
        UPDATE task_wake_outbox
        SET lease_expires_at = clock_timestamp() - interval '1 second'
        """
    )
    relay = WakeOutboxRelay(
        outbox,
        publisher,
        relay_id="relay-restarted",
        lease_duration=timedelta(seconds=30),
    )

    await relay.run_once()

    assert [item.event_id for item in publisher.events] == [
        abandoned.event.event_id,
        abandoned.event.event_id,
    ]


@pytest.mark.asyncio
async def test_crash_on_final_publish_attempt_terminalizes_transport_only(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    task = await ledger.submit(PRINCIPAL, _command("final-crash"))
    outbox = PostgresWakeOutbox(postgres_pool, max_attempts=1)
    abandoned = await outbox.claim("relay-crashed", timedelta(seconds=30))
    assert abandoned is not None
    await postgres_pool.execute(
        """
        UPDATE task_wake_outbox
        SET lease_expires_at = clock_timestamp() - interval '1 second'
        """
    )

    assert await outbox.claim("relay-next", timedelta(seconds=30)) is None

    assert await postgres_pool.fetchval(
        "SELECT status FROM task_wake_outbox"
    ) == "transport_dead_lettered"
    assert (await ledger.get(PRINCIPAL.tenant_id, task.task_id)).status is (
        TaskStatus.QUEUED
    )


@pytest.mark.asyncio
async def test_broker_wake_executes_only_compatible_work_and_duplicate_is_harmless(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    agent = await ledger.submit(
        PRINCIPAL,
        _command("agent-first", ExecutorKind.AGENT),
    )
    synthetic = await ledger.submit(PRINCIPAL, _command("synthetic-second"))
    event_row = await postgres_pool.fetchrow(
        """
        SELECT *
        FROM task_wake_outbox
        WHERE task_id = $1
        """,
        synthetic.task_id,
    )
    event = WakeEvent(
        event_id=event_row["event_id"],
        event_version=event_row["event_version"],
        executor_kind=ExecutorKind(event_row["executor_kind"]),
    )
    worker = TaskWorker(
        ledger,
        ExecutorRegistry({ExecutorKind.SYNTHETIC: SyntheticExecutor()}),
        worker_id="broker-worker",
    )
    handler = BoundedTaskWorkerWakeHandler([worker])

    await handler(event)
    await handler(event)

    assert (await ledger.get(PRINCIPAL.tenant_id, synthetic.task_id)).status is (
        TaskStatus.COMPLETED
    )
    assert (await ledger.get(PRINCIPAL.tenant_id, agent.task_id)).status is (
        TaskStatus.QUEUED
    )


@pytest.mark.asyncio
async def test_recoverable_lease_expiry_requeues_and_emits_one_new_wake(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    task = await ledger.submit(PRINCIPAL, _command("expired"))
    lease = await ledger.claim("worker-old", timedelta(milliseconds=1))
    assert lease is not None
    await asyncio.sleep(0.01)

    assert await ledger.recover_expired(limit=10) == 1
    assert await ledger.recover_expired(limit=10) == 0

    recovered = await ledger.get(PRINCIPAL.tenant_id, task.task_id)
    assert recovered is not None
    assert recovered.status is TaskStatus.QUEUED
    assert await postgres_pool.fetchval(
        """
        SELECT count(*)
        FROM task_wake_outbox
        WHERE task_id = $1 AND source_transition = 'lease_expired'
        """,
        task.task_id,
    ) == 1


@pytest.mark.asyncio
async def test_retry_appends_a_new_wake_without_touching_task_dlq(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    task = await ledger.submit(PRINCIPAL, _command("retry-wake"))
    lease = await ledger.claim("worker-1", timedelta(seconds=30))
    assert lease is not None

    retried = await ledger.fail(
        lease,
        TaskFailure(code=FailureCode.SYNTHETIC_RETRYABLE),
    )

    assert retried.status is TaskStatus.QUEUED
    assert await postgres_pool.fetchval(
        """
        SELECT count(*)
        FROM task_wake_outbox
        WHERE task_id = $1 AND source_transition = 'retry'
        """,
        task.task_id,
    ) == 1
    assert await postgres_pool.fetchval(
        """
        SELECT count(*)
        FROM task_wake_outbox
        WHERE status = 'transport_dead_lettered'
        """
    ) == 0


class _FakeExchange:
    def __init__(self, confirmation=None) -> None:
        self.confirmation = confirmation
        self.calls = []

    async def publish(self, message, *, routing_key, mandatory, timeout):
        self.calls.append((message, routing_key, mandatory, timeout))
        return self.confirmation or Basic.Ack(delivery_tag=1)


@pytest.mark.asyncio
async def test_rabbitmq_adapter_publishes_persistent_minimal_versioned_message() -> None:
    exchange = _FakeExchange()
    publisher = RabbitMQWakePublisher(exchange)
    event = WakeEvent(
        event_id=uuid4(),
        event_version=1,
        executor_kind=ExecutorKind.AGENT,
    )

    await publisher.publish(event)

    message, routing_key, mandatory, timeout = exchange.calls[0]
    assert json.loads(message.body) == event.model_dump(mode="json")
    assert routing_key == "agent"
    assert mandatory is True
    assert timeout == 10
    assert message.message_id == str(event.event_id)
    assert message.delivery_mode.value == 2


@pytest.mark.asyncio
async def test_rabbitmq_adapter_maps_negative_confirmation_to_allowlisted_failure() -> None:
    publisher = RabbitMQWakePublisher(
        _FakeExchange(confirmation=Basic.Nack(delivery_tag=1))
    )
    event = WakeEvent(
        event_id=uuid4(),
        event_version=1,
        executor_kind=ExecutorKind.AGENT,
    )

    with pytest.raises(OutboxPublishError) as raised:
        await publisher.publish(event)

    assert raised.value.code is OutboxFailureCode.REJECTED


class _FakeQueue:
    async def bind(self, exchange, *, routing_key) -> None:
        return None


class _FakeChannel:
    def __init__(self) -> None:
        self.prefetch_count = None
        self.publisher_confirms = None
        self.on_return_raises = None
        self.queues = []

    async def set_qos(self, *, prefetch_count: int) -> None:
        self.prefetch_count = prefetch_count

    async def declare_exchange(self, *args, **kwargs):
        return _FakeExchange()

    async def declare_queue(self, *args, **kwargs):
        self.queues.append((args, kwargs))
        return _FakeQueue()


class _FakeConnection:
    def __init__(self, channel: _FakeChannel) -> None:
        self.fake_channel = channel

    async def channel(
        self,
        *,
        publisher_confirms: bool,
        on_return_raises: bool,
    ):
        self.fake_channel.publisher_confirms = publisher_confirms
        self.fake_channel.on_return_raises = on_return_raises
        return self.fake_channel


@pytest.mark.asyncio
async def test_rabbitmq_consumer_prefetch_matches_bounded_worker_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = _FakeChannel()

    async def connect(url: str):
        return _FakeConnection(channel)

    monkeypatch.setattr(
        "server.services.task_queue.broker.aio_pika.connect_robust",
        connect,
    )

    await RabbitMQWakeBroker.connect(
        "amqp://broker",
        prefetch_count=2,
    )

    assert channel.prefetch_count == 2
    assert channel.publisher_confirms is True
    assert channel.on_return_raises is True
    assert channel.queues[1][1]["arguments"] == {
        "x-dead-letter-exchange": RabbitMQWakeBroker.DEAD_LETTER_EXCHANGE,
        "x-dead-letter-routing-key": "agent",
    }


class _FakeIncomingMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.action = None

    async def ack(self) -> None:
        self.action = "ack"

    async def nack(self, *, requeue: bool) -> None:
        self.action = ("nack", requeue)

    async def reject(self, *, requeue: bool) -> None:
        self.action = ("reject", requeue)


@pytest.mark.asyncio
async def test_rabbitmq_consumer_rejects_poison_and_requeues_transient_failure() -> None:
    broker = RabbitMQWakeBroker(None, None, None)
    malformed = _FakeIncomingMessage(b'{"task_payload":"secret"}')
    wrong_version = _FakeIncomingMessage(
        WakeEvent(
            event_id=uuid4(),
            event_version=2,
            executor_kind=ExecutorKind.AGENT,
        ).model_dump_json().encode()
    )
    transient = _FakeIncomingMessage(
        WakeEvent(
            event_id=uuid4(),
            event_version=1,
            executor_kind=ExecutorKind.AGENT,
        ).model_dump_json().encode()
    )

    async def should_not_run(event) -> None:
        raise AssertionError("poison message reached handler")

    await broker.handle_delivery(
        malformed,
        executor_kind=ExecutorKind.AGENT,
        handler=should_not_run,
    )
    await broker.handle_delivery(
        wrong_version,
        executor_kind=ExecutorKind.AGENT,
        handler=should_not_run,
    )

    async def unavailable(event) -> None:
        raise RuntimeError("database unavailable")

    await broker.handle_delivery(
        transient,
        executor_kind=ExecutorKind.AGENT,
        handler=unavailable,
    )

    assert malformed.action == ("reject", False)
    assert wrong_version.action == ("reject", False)
    assert transient.action == ("reject", False)


def test_measurement_shows_wakes_remove_idle_claim_traffic() -> None:
    result = compare_idle_claim_traffic(
        worker_slots=8,
        worker_processes=4,
        observation_seconds=60,
        poll_interval_seconds=0.5,
        fallback_poll_seconds=30,
        accepted_tasks=4,
    )

    assert result.polling_only_claims == 960
    assert result.broker_assisted_claims == 12
    assert result.claim_reduction_percent == 98.75
