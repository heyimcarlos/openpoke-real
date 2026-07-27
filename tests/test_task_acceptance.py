from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote, urlparse
from uuid import uuid4

import asyncpg
import jwt
import pytest
import pytest_asyncio
from pydantic import ValidationError

from server.services.task_queue import (
    IdempotencyConflict,
    JwtPrincipalVerifier,
    MissingScope,
    PostgresTaskLedger,
    Principal,
    SubmitTask,
    TaskService,
)


DATABASE_URL = os.getenv(
    "OPENPOKE_TEST_DATABASE_URL",
    "postgresql://postgres@127.0.0.1:55432/openpoke_test",
)
SIGNING_KEY = "test-only-signing-key-with-at-least-32-bytes"


def _require_disposable_test_database(database_url: str) -> None:
    database_name = unquote(urlparse(database_url).path).lstrip("/")
    if not database_name.startswith("test_") and not database_name.endswith("_test"):
        raise RuntimeError(
            "task-ledger tests require a database named test_* or *_test"
        )


_require_disposable_test_database(DATABASE_URL)


@pytest_asyncio.fixture
async def ledger() -> PostgresTaskLedger:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    task_ledger = PostgresTaskLedger(pool)
    await pool.execute("DROP TABLE IF EXISTS execution_tasks")
    await task_ledger.migrate()
    try:
        yield task_ledger
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_acceptance_is_durable_and_exact_replay_returns_original_task(
) -> None:
    accepting_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=2,
    )
    principal = Principal(
        actor_id="user-7",
        tenant_id="tenant-a",
        scopes=frozenset({"tasks:create", "tasks:read"}),
    )
    command = SubmitTask(
        idempotency_key="turn-42-email-search",
        origin_turn_id="turn-42",
        agent_name="email-search",
        input={"instructions": "find the latest invoice"},
    )
    try:
        accepting_ledger = PostgresTaskLedger(accepting_pool)
        await accepting_pool.execute("DROP TABLE IF EXISTS execution_tasks")
        await accepting_ledger.migrate()
        service = TaskService(accepting_ledger)
        accepted = await service.submit(principal, command)
        replayed = await service.submit(principal, command)
    finally:
        await accepting_pool.close()

    restarted_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    try:
        recovered = await TaskService(PostgresTaskLedger(restarted_pool)).get(
            principal,
            accepted.task_id,
        )
    finally:
        await restarted_pool.close()

    assert replayed.task_id == accepted.task_id
    assert recovered is not None
    assert recovered.task_id == accepted.task_id
    assert recovered.tenant_id == "tenant-a"
    assert recovered.actor_id == "user-7"
    assert recovered.origin_turn_id == "turn-42"
    assert recovered.status == "queued"
    assert recovered.input == {"instructions": "find the latest invoice"}


@pytest.mark.asyncio
async def test_concurrent_exact_replays_converge_on_one_task(
    ledger: PostgresTaskLedger,
) -> None:
    principal = Principal(
        actor_id="user-7",
        tenant_id="tenant-a",
        scopes=frozenset({"tasks:create"}),
    )
    command = SubmitTask(
        idempotency_key="concurrent-replay",
        origin_turn_id="turn-concurrent",
        agent_name="synthetic",
        input={"work": "once"},
    )
    service = TaskService(ledger)

    accepted = await asyncio.gather(
        *(service.submit(principal, command) for _ in range(8))
    )

    assert len({task.task_id for task in accepted}) == 1


@pytest.mark.asyncio
async def test_conflicting_replay_is_rejected_and_cross_tenant_read_is_hidden(
    ledger: PostgresTaskLedger,
) -> None:
    owner = Principal(
        actor_id="owner",
        tenant_id="tenant-a",
        scopes=frozenset({"tasks:create", "tasks:read"}),
    )
    other_tenant = Principal(
        actor_id="other-user",
        tenant_id="tenant-b",
        scopes=frozenset({"tasks:read"}),
    )
    service = TaskService(ledger)
    accepted = await service.submit(
        owner,
        SubmitTask(
            idempotency_key="shared-key",
            origin_turn_id="turn-1",
            agent_name="email-search",
            input={"query": "invoice"},
        ),
    )

    with pytest.raises(IdempotencyConflict):
        await service.submit(
            owner,
            SubmitTask(
                idempotency_key="shared-key",
                origin_turn_id="turn-1",
                agent_name="email-search",
                input={"query": "different request"},
            ),
        )

    assert await service.get(other_tenant, accepted.task_id) is None
    assert (await service.get(owner, accepted.task_id)) == accepted


@pytest.mark.parametrize(
    "command_data",
    [
        {
            "tenant_id": "attacker-selected-tenant",
            "idempotency_key": "key-1",
            "origin_turn_id": "turn-1",
            "agent_name": "synthetic",
            "input": {},
        },
        {
            "idempotency_key": "x" * 129,
            "origin_turn_id": "turn-1",
            "agent_name": "synthetic",
            "input": {},
        },
        {
            "idempotency_key": "key-1",
            "origin_turn_id": "turn-1",
            "agent_name": "synthetic",
            "input": {"content": "x" * 16_385},
        },
        {
            "idempotency_key": "key-1",
            "origin_turn_id": "turn-1",
            "agent_name": "synthetic",
            "input": {"score": float("nan")},
        },
        {
            "idempotency_key": "key-1",
            "origin_turn_id": "turn-1",
            "agent_name": "synthetic",
            "input": {"content": "cannot persist \u0000 in jsonb"},
        },
    ],
    ids=[
        "tenant-in-command",
        "identifier-too-long",
        "payload-too-large",
        "non-finite-number",
        "postgres-null-character",
    ],
)
def test_task_command_rejects_untrusted_tenant_and_unbounded_input(
    command_data: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        SubmitTask.model_validate(command_data)


@pytest.mark.asyncio
async def test_verified_jwt_drives_tenant_owned_acceptance(
    ledger: PostgresTaskLedger,
) -> None:
    token = jwt.encode(
        {
            "sub": "user-from-token",
            "tenant_id": "tenant-from-token",
            "scope": "tasks:create tasks:read",
            "iss": "https://auth.openpoke.test",
            "aud": "openpoke-api",
            "exp": datetime.now(timezone.utc) + timedelta(minutes=5),
        },
        SIGNING_KEY,
        algorithm="HS256",
    )
    principal = JwtPrincipalVerifier(
        signing_key=SIGNING_KEY,
        issuer="https://auth.openpoke.test",
        audience="openpoke-api",
    ).verify(token)
    service = TaskService(ledger)

    accepted = await service.submit(
        principal,
        SubmitTask(
            idempotency_key="verified-boundary",
            origin_turn_id="turn-verified",
            agent_name="synthetic",
            input={"work": "bounded"},
        ),
    )

    assert accepted.tenant_id == "tenant-from-token"
    assert accepted.actor_id == "user-from-token"
    assert await service.get(principal, accepted.task_id) == accepted


@pytest.mark.asyncio
async def test_task_service_requires_operation_scope(
    ledger: PostgresTaskLedger,
) -> None:
    service = TaskService(ledger)
    command = SubmitTask(
        idempotency_key="scope-check",
        origin_turn_id="turn-1",
        agent_name="synthetic",
        input={},
    )

    with pytest.raises(MissingScope):
        await service.submit(
            Principal(
                actor_id="user-7",
                tenant_id="tenant-a",
                scopes=frozenset({"tasks:read"}),
            ),
            command,
        )

    with pytest.raises(MissingScope):
        await service.get(
            Principal(
                actor_id="user-7",
                tenant_id="tenant-a",
                scopes=frozenset({"tasks:create"}),
            ),
            task_id=uuid4(),
        )
