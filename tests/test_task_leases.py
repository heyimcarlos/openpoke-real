from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import asyncpg
import pytest

from server.services.task_queue import (
    PostgresTaskLedger,
    Principal,
    StaleLease,
    SubmitTask,
)


async def _submit_tasks(
    ledger: PostgresTaskLedger,
    tenant_id: str,
    count: int,
) -> None:
    principal = Principal(
        actor_id=f"{tenant_id}-user",
        tenant_id=tenant_id,
        scopes=frozenset(),
    )
    for index in range(count):
        await ledger.submit(
            principal,
            SubmitTask(
                idempotency_key=f"{tenant_id}-{index}",
                origin_turn_id=f"{tenant_id}-turn-{index}",
                agent_name="synthetic",
                input={"index": index},
            ),
        )


@pytest.mark.asyncio
async def test_concurrent_claims_are_unique_and_respect_default_capacity(
    ledger: PostgresTaskLedger,
) -> None:
    for tenant_number in range(5):
        await _submit_tasks(ledger, f"tenant-{tenant_number}", 5)

    claims = await asyncio.gather(
        *(
            ledger.claim(
                worker_id=f"worker-{worker_number}",
                lease_duration=timedelta(minutes=1),
            )
            for worker_number in range(20)
        )
    )
    accepted_claims = [claim for claim in claims if claim is not None]

    assert len(accepted_claims) == 8
    assert len({claim.task_id for claim in accepted_claims}) == 8
    tenant_counts = Counter(claim.tenant_id for claim in accepted_claims)
    assert all(count <= 2 for count in tenant_counts.values())


@pytest.mark.asyncio
async def test_quiet_tenant_progresses_behind_noisy_tenant_backlog(
    ledger: PostgresTaskLedger,
) -> None:
    await _submit_tasks(ledger, "noisy", 10)
    await _submit_tasks(ledger, "quiet", 1)

    claims = [
        await ledger.claim(
            worker_id=f"worker-{worker_number}",
            lease_duration=timedelta(minutes=1),
        )
        for worker_number in range(3)
    ]

    assert [claim.tenant_id for claim in claims if claim is not None] == [
        "noisy",
        "quiet",
        "noisy",
    ]


@pytest.mark.asyncio
async def test_replenished_noisy_backlog_does_not_starve_later_quiet_tenant(
    ledger: PostgresTaskLedger,
) -> None:
    await _submit_tasks(ledger, "noisy", 10)
    first = await ledger.claim("worker-1", timedelta(minutes=1))
    second = await ledger.claim("worker-2", timedelta(minutes=1))
    assert first is not None
    assert second is not None
    await _submit_tasks(ledger, "quiet", 1)
    await ledger.complete(first, {"done": True})

    next_claim = await ledger.claim("worker-3", timedelta(minutes=1))

    assert next_claim is not None
    assert next_claim.tenant_id == "quiet"


@pytest.mark.asyncio
async def test_completion_requires_current_worker_generation_and_live_lease(
    ledger: PostgresTaskLedger,
) -> None:
    await _submit_tasks(ledger, "tenant-a", 1)
    lease = await ledger.claim("worker-current", timedelta(minutes=1))
    assert lease is not None

    wrong_worker = replace(lease, worker_id="worker-stale")
    wrong_generation = replace(
        lease,
        lease_generation=lease.lease_generation - 1,
    )
    for stale_lease in (wrong_worker, wrong_generation):
        with pytest.raises(StaleLease):
            await ledger.complete(stale_lease, {"winner": "stale"})

    still_running = await ledger.get(lease.tenant_id, lease.task_id)
    assert still_running is not None
    assert still_running.status == "running"
    assert still_running.result is None

    completed = await ledger.complete(lease, {"winner": "current"})

    assert completed.status == "completed"
    assert completed.result == {"winner": "current"}


@pytest.mark.asyncio
async def test_expired_lease_is_reclaimed_and_old_completion_cannot_win(
    ledger: PostgresTaskLedger,
) -> None:
    await _submit_tasks(ledger, "tenant-a", 1)
    expired = await ledger.claim("worker-expired", timedelta(milliseconds=1))
    assert expired is not None
    assert expired.attempt_count == 1
    assert expired.lease_generation == 1
    await asyncio.sleep(0.02)

    with pytest.raises(StaleLease):
        await ledger.complete(expired, {"winner": "expired"})

    replacement = await ledger.claim("worker-replacement", timedelta(minutes=1))
    assert replacement is not None
    assert replacement.task_id == expired.task_id
    assert replacement.attempt_count == 2
    assert replacement.lease_generation == 2

    with pytest.raises(StaleLease):
        await ledger.complete(expired, {"winner": "expired"})

    still_running = await ledger.get(replacement.tenant_id, replacement.task_id)
    assert still_running is not None
    assert still_running.status == "running"
    assert still_running.result is None

    completed = await ledger.complete(replacement, {"winner": "replacement"})

    assert completed.status == "completed"
    assert completed.result == {"winner": "replacement"}


@pytest.mark.asyncio
async def test_migration_preserves_tasks_accepted_under_issue_2_schema(
    postgres_pool: asyncpg.Pool,
) -> None:
    migration_001 = (
        Path(__file__).resolve().parents[1]
        / "server"
        / "migrations"
        / "001_execution_tasks.sql"
    )
    await postgres_pool.execute(migration_001.read_text(encoding="utf-8"))
    ledger = PostgresTaskLedger(postgres_pool)
    principal = Principal(
        actor_id="existing-user",
        tenant_id="existing-tenant",
        scopes=frozenset(),
    )
    accepted = await ledger.submit(
        principal,
        SubmitTask(
            idempotency_key="accepted-before-upgrade",
            origin_turn_id="existing-turn",
            agent_name="synthetic",
            input={"preserve": True},
        ),
    )

    await asyncio.gather(*(ledger.migrate() for _ in range(4)))

    preserved = await ledger.get(principal.tenant_id, accepted.task_id)
    claim = await ledger.claim("upgrade-worker", timedelta(minutes=1))
    assert preserved == accepted
    assert claim is not None
    assert claim.task_id == accepted.task_id
