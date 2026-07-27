from __future__ import annotations

import asyncio
from datetime import timedelta

import asyncpg
import pytest
from pydantic import ValidationError

from server.services.task_queue import (
    AdmissionRejected,
    FailureCode,
    IdempotencyConflict,
    MissingScope,
    PostgresTaskLedger,
    Principal,
    StaleLease,
    SubmitTask,
    TaskFailure,
    TaskService,
)


def _principal(
    tenant_id: str,
    *scopes: str,
) -> Principal:
    return Principal(
        actor_id=f"{tenant_id}-user",
        tenant_id=tenant_id,
        scopes=frozenset(scopes),
    )


def _command(index: int) -> SubmitTask:
    return SubmitTask(
        idempotency_key=f"task-{index}",
        origin_turn_id=f"turn-{index}",
        agent_name="synthetic",
        input={"index": index},
    )


@pytest.mark.asyncio
async def test_concurrent_admission_accepts_only_the_tenant_limit(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool, tenant_outstanding_limit=50)
    await ledger.migrate()
    principal = _principal("tenant-a")

    outcomes = await asyncio.gather(
        *(ledger.submit(principal, _command(index)) for index in range(51)),
        return_exceptions=True,
    )

    accepted = [outcome for outcome in outcomes if not isinstance(outcome, Exception)]
    rejected = [outcome for outcome in outcomes if isinstance(outcome, Exception)]

    assert len(accepted) == 50
    assert len(rejected) == 1
    assert isinstance(rejected[0], AdmissionRejected)
    assert rejected[0].retry_after_seconds == 5
    persisted = await asyncio.gather(
        *(ledger.get(principal.tenant_id, task.task_id) for task in accepted)
    )
    assert persisted == accepted


@pytest.mark.asyncio
async def test_exact_replay_succeeds_when_tenant_is_at_admission_limit(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool, tenant_outstanding_limit=2)
    await ledger.migrate()
    principal = _principal("tenant-a")
    first = await ledger.submit(principal, _command(1))
    await ledger.submit(principal, _command(2))

    replayed = await ledger.submit(principal, _command(1))

    assert replayed == first
    with pytest.raises(AdmissionRejected):
        await ledger.submit(principal, _command(3))


@pytest.mark.asyncio
async def test_full_tenant_does_not_block_another_tenant(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool, tenant_outstanding_limit=1)
    await ledger.migrate()
    full_tenant = _principal("tenant-a")
    quiet_tenant = _principal("tenant-b")
    await ledger.submit(full_tenant, _command(1))

    accepted = await ledger.submit(quiet_tenant, _command(2))

    assert accepted.tenant_id == quiet_tenant.tenant_id


@pytest.mark.asyncio
async def test_retryable_failure_uses_three_attempts_then_dead_letters(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    principal = _principal("tenant-a")
    accepted = await ledger.submit(principal, _command(1))
    failure = TaskFailure(code=FailureCode.SYNTHETIC_RETRYABLE)

    for attempt in (1, 2):
        lease = await ledger.claim(f"worker-{attempt}", timedelta(minutes=1))
        assert lease is not None
        assert lease.attempt_count == attempt

        requeued = await ledger.fail(lease, failure)

        assert requeued.status == "queued"
        assert requeued.attempt_count == attempt
        assert requeued.failure == FailureCode.SYNTHETIC_RETRYABLE

    final_lease = await ledger.claim("worker-3", timedelta(minutes=1))
    assert final_lease is not None
    assert final_lease.attempt_count == 3

    dead_lettered = await ledger.fail(final_lease, failure)

    assert dead_lettered.status == "dead_lettered"
    assert dead_lettered.attempt_count == 3
    assert dead_lettered.failure == FailureCode.SYNTHETIC_RETRYABLE
    assert await ledger.get(principal.tenant_id, accepted.task_id) == dead_lettered
    assert await ledger.claim("worker-4", timedelta(minutes=1)) is None


@pytest.mark.asyncio
async def test_non_retryable_failure_dead_letters_on_first_attempt(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    principal = _principal("tenant-a")
    await ledger.submit(principal, _command(1))
    lease = await ledger.claim("worker-1", timedelta(minutes=1))
    assert lease is not None

    dead_lettered = await ledger.fail(
        lease,
        TaskFailure(code=FailureCode.SYNTHETIC_NON_RETRYABLE),
    )

    assert dead_lettered.status == "dead_lettered"
    assert dead_lettered.attempt_count == 1
    assert dead_lettered.failure == FailureCode.SYNTHETIC_NON_RETRYABLE


@pytest.mark.asyncio
async def test_expired_final_attempt_is_dead_lettered_without_attempt_four(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool, max_attempts=3)
    await ledger.migrate()
    principal = _principal("tenant-a")
    accepted = await ledger.submit(principal, _command(1))

    for attempt in (1, 2):
        lease = await ledger.claim(f"worker-{attempt}", timedelta(minutes=1))
        assert lease is not None
        await ledger.fail(
            lease,
            TaskFailure(code=FailureCode.SYNTHETIC_RETRYABLE),
        )

    expired = await ledger.claim("worker-3", timedelta(milliseconds=1))
    assert expired is not None
    assert expired.attempt_count == 3
    await asyncio.sleep(0.02)

    assert await ledger.claim("worker-4", timedelta(minutes=1)) is None
    recovered = await ledger.get(principal.tenant_id, accepted.task_id)
    assert recovered is not None
    assert recovered.status == "dead_lettered"
    assert recovered.attempt_count == 3
    assert recovered.failure == FailureCode.LEASE_EXPIRED


@pytest.mark.asyncio
async def test_lowered_attempt_budget_dead_letters_already_exhausted_queue(
    postgres_pool: asyncpg.Pool,
) -> None:
    original_policy = PostgresTaskLedger(postgres_pool, max_attempts=4)
    await original_policy.migrate()
    principal = _principal("tenant-a")
    accepted = await original_policy.submit(principal, _command(1))
    failure = TaskFailure(code=FailureCode.SYNTHETIC_RETRYABLE)
    for attempt in (1, 2, 3):
        lease = await original_policy.claim(
            f"worker-{attempt}",
            timedelta(minutes=1),
        )
        assert lease is not None
        requeued = await original_policy.fail(lease, failure)
        assert requeued.status == "queued"

    lowered_policy = PostgresTaskLedger(postgres_pool, max_attempts=3)

    assert await lowered_policy.claim("worker-4", timedelta(minutes=1)) is None
    reconciled = await lowered_policy.get(
        principal.tenant_id,
        accepted.task_id,
    )
    assert reconciled is not None
    assert reconciled.status == "dead_lettered"
    assert reconciled.failure == FailureCode.ATTEMPTS_EXHAUSTED


@pytest.mark.asyncio
async def test_expired_worker_cannot_report_failure_after_reclaim(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    principal = _principal("tenant-a")
    await ledger.submit(principal, _command(1))
    expired = await ledger.claim("worker-1", timedelta(milliseconds=1))
    assert expired is not None
    await asyncio.sleep(0.02)
    replacement = await ledger.claim("worker-2", timedelta(minutes=1))
    assert replacement is not None

    with pytest.raises(StaleLease):
        await ledger.fail(
            expired,
            TaskFailure(code=FailureCode.SYNTHETIC_RETRYABLE),
        )

    current = await ledger.get(principal.tenant_id, replacement.task_id)
    assert current is not None
    assert current.status == "running"
    assert current.attempt_count == 2
    assert current.failure is None


@pytest.mark.asyncio
async def test_queued_cancellation_is_authorized_and_tenant_owned(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    service = TaskService(ledger)
    owner = _principal("tenant-a", "tasks:create", "tasks:read", "tasks:cancel")
    other = _principal("tenant-b", "tasks:cancel")
    owner_task = await service.submit(owner, _command(1))
    await ledger.submit(other, _command(2))

    with pytest.raises(MissingScope):
        await service.cancel(
            _principal("tenant-a", "tasks:read"),
            owner_task.task_id,
        )
    assert await service.cancel(other, owner_task.task_id) is None

    cancelled = await service.cancel(owner, owner_task.task_id)

    assert cancelled is not None
    assert cancelled.status == "cancelled"
    assert await service.get(owner, owner_task.task_id) == cancelled
    claim = await ledger.claim("worker-1", timedelta(minutes=1))
    assert claim is not None
    assert claim.tenant_id == "tenant-b"
    assert await ledger.claim("worker-2", timedelta(minutes=1)) is None


@pytest.mark.asyncio
async def test_cancellation_releases_tenant_admission_capacity(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool, tenant_outstanding_limit=1)
    await ledger.migrate()
    principal = _principal("tenant-a")
    accepted = await ledger.submit(principal, _command(1))

    cancelled = await ledger.cancel(principal.tenant_id, accepted.task_id)
    replacement = await ledger.submit(principal, _command(2))

    assert cancelled is not None
    assert cancelled.status == "cancelled"
    assert replacement.status == "queued"


@pytest.mark.asyncio
async def test_submit_handles_conflict_from_legacy_writer_during_rollout(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(postgres_pool)
    await ledger.migrate()
    principal = _principal("tenant-a")
    command = SubmitTask(
        idempotency_key="rollout-conflict",
        origin_turn_id="new-turn",
        agent_name="slow-new",
        input={"writer": "new"},
    )
    lock_namespace = 1_011_334_821
    lock_key = 77
    await postgres_pool.execute(
        f"""
        CREATE FUNCTION pause_new_writer() RETURNS trigger AS $$
        BEGIN
            IF NEW.agent_name = 'slow-new' THEN
                PERFORM pg_advisory_xact_lock(
                    {lock_namespace},
                    {lock_key}
                );
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER pause_new_writer
        BEFORE INSERT ON execution_tasks
        FOR EACH ROW EXECUTE FUNCTION pause_new_writer();
        """
    )

    async with postgres_pool.acquire() as blocker:
        await blocker.execute(
            "SELECT pg_advisory_lock($1, $2)",
            lock_namespace,
            lock_key,
        )
        submission = asyncio.create_task(ledger.submit(principal, command))
        try:
            for _ in range(100):
                waiting = await postgres_pool.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_locks
                        WHERE locktype = 'advisory'
                          AND classid = $1::oid
                          AND objid = $2::oid
                          AND NOT granted
                    )
                    """,
                    lock_namespace,
                    lock_key,
                )
                if waiting:
                    break
                await asyncio.sleep(0.01)
            assert waiting

            await postgres_pool.execute(
                """
                INSERT INTO execution_tasks (
                    tenant_id,
                    actor_id,
                    idempotency_key,
                    request_fingerprint,
                    origin_turn_id,
                    agent_name,
                    input
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                """,
                principal.tenant_id,
                "legacy-user",
                command.idempotency_key,
                "legacy-fingerprint",
                "legacy-turn",
                "legacy-agent",
                "{}",
            )
        finally:
            await blocker.execute(
                "SELECT pg_advisory_unlock($1, $2)",
                lock_namespace,
                lock_key,
            )

    with pytest.raises(IdempotencyConflict):
        await submission


@pytest.mark.asyncio
async def test_attempt_budget_and_retry_guidance_are_configurable(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(
        postgres_pool,
        tenant_outstanding_limit=1,
        max_attempts=1,
        admission_retry_after_seconds=17,
    )
    await ledger.migrate()
    principal = _principal("tenant-a")
    await ledger.submit(principal, _command(1))

    with pytest.raises(AdmissionRejected) as rejection:
        await ledger.submit(principal, _command(2))
    assert rejection.value.retry_after_seconds == 17

    lease = await ledger.claim("worker-1", timedelta(minutes=1))
    assert lease is not None
    failed = await ledger.fail(
        lease,
        TaskFailure(code=FailureCode.SYNTHETIC_RETRYABLE),
    )
    assert failed.status == "dead_lettered"
    assert failed.attempt_count == 1


@pytest.mark.asyncio
async def test_active_capacity_limits_are_configurable(
    postgres_pool: asyncpg.Pool,
) -> None:
    ledger = PostgresTaskLedger(
        postgres_pool,
        global_active_limit=2,
        tenant_active_limit=1,
    )
    await ledger.migrate()
    for tenant_id in ("tenant-a", "tenant-b"):
        principal = _principal(tenant_id)
        await ledger.submit(principal, _command(1))
        await ledger.submit(principal, _command(2))

    claims = await asyncio.gather(
        *(
            ledger.claim(
                worker_id=f"worker-{index}",
                lease_duration=timedelta(minutes=1),
            )
            for index in range(4)
        )
    )
    accepted = [claim for claim in claims if claim is not None]

    assert len(accepted) == 2
    assert {claim.tenant_id for claim in accepted} == {"tenant-a", "tenant-b"}


@pytest.mark.parametrize(
    "failure_data",
    [
        {"code": "provider_error"},
        {
            "code": "synthetic_retryable",
            "provider_payload": {"authorization": "secret"},
        },
        {
            "code": "synthetic_non_retryable",
            "message": "raw exception text",
        },
    ],
    ids=["unknown-code", "provider-payload", "raw-message"],
)
def test_failure_data_allows_only_bounded_sanitized_codes(
    failure_data: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        TaskFailure.model_validate(failure_data)
