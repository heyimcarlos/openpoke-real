from __future__ import annotations

from datetime import timedelta
import asyncio

import asyncpg
import pytest
import pytest_asyncio
from agents import Session

from server.services.task_queue import (
    AdmissionRejected,
    ExecutorKind,
    PostgresTaskLedger,
    Principal,
    SubmitTask,
)
from server.services.threads import (
    AgentRunStatus,
    DelegationLimitReached,
    MessageConflict,
    PostgresThreadLedger,
    PostgresThreadSession,
    StaleAgentRunLease,
)


PRINCIPAL = Principal(
    actor_id="user-7",
    tenant_id="tenant-a",
    scopes=frozenset({"chat:send", "tasks:create"}),
)


@pytest_asyncio.fixture
async def thread_ledger(
    postgres_pool: asyncpg.Pool,
) -> PostgresThreadLedger:
    task_ledger = PostgresTaskLedger(postgres_pool)
    await task_ledger.migrate()
    return PostgresThreadLedger(postgres_pool)


@pytest.mark.asyncio
async def test_inbound_messages_are_idempotent_and_coalesce_before_claim(
    thread_ledger: PostgresThreadLedger,
) -> None:
    first = await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-1",
        content="first",
    )
    replay = await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-1",
        content="first",
    )
    second = await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-2",
        content="second",
    )

    assert replay == first
    assert first.ingress_sequence == 1
    assert second.ingress_sequence == 2
    with pytest.raises(MessageConflict):
        await thread_ledger.append_message(
            PRINCIPAL,
            message_id="sms-1",
            content="different",
        )

    lease = await thread_ledger.claim_run(
        "orchestrator-1",
        timedelta(seconds=30),
    )
    assert lease is not None
    assert lease.thread_id == first.thread_id
    assert lease.ingress_cutoff == 2
    assert [item["content"] for item in await thread_ledger.get_context(lease)] == [
        "first",
        "second",
    ]
    assert await thread_ledger.claim_run(
        "orchestrator-2",
        timedelta(seconds=30),
    ) is None


@pytest.mark.asyncio
async def test_concurrent_append_allocates_one_sequence_per_source(
    thread_ledger: PostgresThreadLedger,
) -> None:
    duplicate = await asyncio.gather(
        *(
            thread_ledger.append_message(
                PRINCIPAL,
                message_id="same-sms",
                content="same",
            )
            for _ in range(10)
        )
    )
    distinct = await asyncio.gather(
        *(
            thread_ledger.append_message(
                PRINCIPAL,
                message_id=f"sms-{index}",
                content=f"message {index}",
            )
            for index in range(20)
        )
    )

    assert len({message.message_id for message in duplicate}) == 1
    assert sorted(
        message.ingress_sequence for message in [duplicate[0], *distinct]
    ) == list(range(1, 22))


@pytest.mark.asyncio
async def test_history_is_tenant_and_actor_scoped(
    thread_ledger: PostgresThreadLedger,
) -> None:
    other = Principal(
        actor_id="user-8",
        tenant_id="tenant-b",
        scopes=PRINCIPAL.scopes,
    )
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="tenant-a-message",
        content="private A",
    )
    await thread_ledger.append_message(
        other,
        message_id="tenant-b-message",
        content="private B",
    )

    assert [message.content for message in await thread_ledger.list_messages(PRINCIPAL)] == [
        "private A"
    ]
    assert [message.content for message in await thread_ledger.list_messages(other)] == [
        "private B"
    ]


@pytest.mark.asyncio
async def test_message_arriving_during_run_is_released_after_completion(
    thread_ledger: PostgresThreadLedger,
) -> None:
    first = await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-1",
        content="first",
    )
    claimed = await thread_ledger.claim_run(
        "orchestrator-1",
        timedelta(seconds=30),
    )
    assert claimed is not None

    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-2",
        content="second",
    )
    completed = await thread_ledger.complete_run(claimed, response="reply one")
    assert completed.status is AgentRunStatus.COMPLETED

    next_run = await thread_ledger.claim_run(
        "orchestrator-2",
        timedelta(seconds=30),
    )
    assert next_run is not None
    assert next_run.thread_id == first.thread_id
    assert next_run.ingress_cutoff == 2
    context = await thread_ledger.get_context(next_run)
    assert [(item["role"], item["content"]) for item in context] == [
        ("user", "first"),
        ("assistant", "reply one"),
        ("user", "second"),
    ]


@pytest.mark.asyncio
async def test_expired_run_is_reclaimed_and_stale_result_is_rejected(
    thread_ledger: PostgresThreadLedger,
) -> None:
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-1",
        content="hello",
    )
    expired = await thread_ledger.claim_run(
        "orchestrator-old",
        timedelta(milliseconds=1),
    )
    assert expired is not None

    replacement = None
    for _ in range(20):
        await asyncio.sleep(0.005)
        replacement = await thread_ledger.claim_run(
            "orchestrator-new",
            timedelta(seconds=30),
        )
        if replacement is not None:
            break
    assert replacement is not None
    assert replacement.run_id == expired.run_id
    assert replacement.lease_generation == 2

    with pytest.raises(StaleAgentRunLease):
        await thread_ledger.complete_run(expired, response="stale")
    await thread_ledger.complete_run(replacement, response="current")
    history = await thread_ledger.list_messages(PRINCIPAL)
    assert [message.content for message in history] == ["hello", "current"]


@pytest.mark.asyncio
async def test_staged_session_output_from_expired_generation_never_enters_context(
    thread_ledger: PostgresThreadLedger,
) -> None:
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-1",
        content="hello",
    )
    expired = await thread_ledger.claim_run(
        "orchestrator-old",
        timedelta(milliseconds=20),
    )
    assert expired is not None
    old_session = PostgresThreadSession(thread_ledger, expired)
    await old_session.add_items(
        [{"role": "assistant", "content": "uncommitted stale output"}]
    )
    await asyncio.sleep(0.03)

    replacement = await thread_ledger.claim_run(
        "orchestrator-new",
        timedelta(seconds=30),
    )
    assert replacement is not None
    assert await PostgresThreadSession(
        thread_ledger,
        replacement,
    ).get_items() == [{"role": "user", "content": "hello"}]
    await thread_ledger.complete_run(replacement, response="current reply")
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-2",
        content="next",
    )
    next_run = await thread_ledger.claim_run(
        "orchestrator-next",
        timedelta(seconds=30),
    )
    assert next_run is not None
    assert [
        item["content"] for item in await thread_ledger.get_context(next_run)
    ] == ["hello", "current reply", "next"]


@pytest.mark.asyncio
async def test_delegation_budget_is_durable_and_replay_does_not_consume_it(
    thread_ledger: PostgresThreadLedger,
) -> None:
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-1",
        content="delegate",
    )
    lease = await thread_ledger.claim_run(
        "orchestrator-1",
        timedelta(seconds=30),
    )
    assert lease is not None

    first_command = SubmitTask(
        idempotency_key="delegation:first",
        origin_turn_id=str(lease.run_id),
        agent_name="invoices",
        executor_kind=ExecutorKind.AGENT,
        input={"instructions": "find invoice"},
    )
    first = await thread_ledger.submit_delegation(
        lease,
        PRINCIPAL,
        first_command,
    )
    replay = await thread_ledger.submit_delegation(
        lease,
        PRINCIPAL,
        first_command,
    )
    second = await thread_ledger.submit_delegation(
        lease,
        PRINCIPAL,
        SubmitTask(
            idempotency_key="delegation:second",
            origin_turn_id=str(lease.run_id),
            agent_name="calendar",
            input={"instructions": "book followup"},
        ),
    )

    assert replay.task_id == first.task_id
    assert second.task_id != first.task_id
    with pytest.raises(DelegationLimitReached):
        await thread_ledger.submit_delegation(
            lease,
            PRINCIPAL,
            SubmitTask(
                idempotency_key="delegation:third",
                origin_turn_id=str(lease.run_id),
                agent_name="contacts",
                input={"instructions": "find phone"},
            ),
        )


@pytest.mark.asyncio
async def test_delegation_uses_same_tenant_backlog_admission(
    postgres_pool: asyncpg.Pool,
) -> None:
    task_ledger = PostgresTaskLedger(postgres_pool)
    await task_ledger.migrate()
    thread_ledger = PostgresThreadLedger(
        postgres_pool,
        tenant_outstanding_limit=1,
    )
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-1",
        content="delegate",
    )
    run = await thread_ledger.claim_run(
        "orchestrator-1",
        timedelta(seconds=30),
    )
    assert run is not None
    await thread_ledger.submit_delegation(
        run,
        PRINCIPAL,
        SubmitTask(
            idempotency_key="delegation:first",
            origin_turn_id=str(run.run_id),
            agent_name="first",
            input={"instructions": "first"},
        ),
    )

    with pytest.raises(AdmissionRejected):
        await thread_ledger.submit_delegation(
            run,
            PRINCIPAL,
            SubmitTask(
                idempotency_key="delegation:second",
                origin_turn_id=str(run.run_id),
                agent_name="second",
                input={"instructions": "second"},
            ),
        )


@pytest.mark.asyncio
async def test_task_completion_atomically_appends_one_result_continuation(
    postgres_pool: asyncpg.Pool,
    thread_ledger: PostgresThreadLedger,
) -> None:
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-1",
        content="delegate",
    )
    run = await thread_ledger.claim_run(
        "orchestrator-1",
        timedelta(seconds=30),
    )
    assert run is not None
    task = await thread_ledger.submit_delegation(
        run,
        PRINCIPAL,
        SubmitTask(
            idempotency_key="delegation:invoice",
            origin_turn_id=str(run.run_id),
            agent_name="invoices",
            input={"instructions": "find invoice"},
        ),
    )
    await thread_ledger.complete_run(run, response="I started that.")

    task_ledger = PostgresTaskLedger(postgres_pool)
    task_lease = await task_ledger.claim("execution-1", timedelta(seconds=30))
    assert task_lease is not None
    assert task_lease.task_id == task.task_id
    await task_ledger.complete(task_lease, {"response": "invoice found"})

    history = await thread_ledger.list_messages(PRINCIPAL)
    assert [(message.role, message.content) for message in history] == [
        ("user", "delegate"),
        ("assistant", "I started that."),
        ("agent", "[SUCCESS] invoices: invoice found"),
    ]
    continuation = await thread_ledger.claim_run(
        "orchestrator-2",
        timedelta(seconds=30),
    )
    assert continuation is not None
    assert continuation.ingress_cutoff == 2


@pytest.mark.asyncio
async def test_session_reads_bounded_history_at_claimed_cutoff(
    thread_ledger: PostgresThreadLedger,
) -> None:
    first = await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-1",
        content="first",
    )
    run = await thread_ledger.claim_run(
        "orchestrator-1",
        timedelta(seconds=30),
    )
    assert run is not None
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-2",
        content="after cutoff",
    )

    session = PostgresThreadSession(thread_ledger, run, default_limit=1)

    assert session.session_id == str(first.thread_id)
    assert isinstance(session, Session)
    assert session.session_settings.limit == 1
    assert await session.get_items() == [{"role": "user", "content": "first"}]
    with pytest.raises(RuntimeError):
        await session.pop_item()
    with pytest.raises(RuntimeError):
        await session.clear_session()


@pytest.mark.asyncio
async def test_session_keeps_multiple_output_batches(
    thread_ledger: PostgresThreadLedger,
) -> None:
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-batches",
        content="hello",
    )
    run = await thread_ledger.claim_run(
        "orchestrator-batches",
        timedelta(seconds=30),
    )
    assert run is not None
    session = PostgresThreadSession(thread_ledger, run)

    await session.add_items([{"role": "assistant", "content": "first batch"}])
    await session.add_items([{"role": "assistant", "content": "second batch"}])
    await thread_ledger.complete_run(run, response=None)
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="sms-after-batches",
        content="next",
    )
    next_run = await thread_ledger.claim_run(
        "orchestrator-next",
        timedelta(seconds=30),
    )
    assert next_run is not None
    assert [item["content"] for item in await thread_ledger.get_context(next_run)] == [
        "hello",
        "first batch",
        "second batch",
        "next",
    ]


@pytest.mark.asyncio
async def test_message_size_is_bounded(
    thread_ledger: PostgresThreadLedger,
) -> None:
    with pytest.raises(ValueError, match="message exceeds"):
        await thread_ledger.append_message(
            PRINCIPAL,
            message_id="oversized",
            content="x" * 16_385,
        )


@pytest.mark.asyncio
async def test_terminal_failure_releases_message_arriving_during_last_attempt(
    thread_ledger: PostgresThreadLedger,
) -> None:
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="first-attempt-input",
        content="first",
    )
    lease = None
    for attempt in range(3):
        lease = await thread_ledger.claim_run(
            f"orchestrator-{attempt}",
            timedelta(seconds=30),
        )
        assert lease is not None
        if attempt < 2:
            await thread_ledger.fail_run(lease)
    assert lease is not None
    await thread_ledger.append_message(
        PRINCIPAL,
        message_id="arrived-during-final-attempt",
        content="second",
    )

    failed = await thread_ledger.fail_run(lease)
    replacement = await thread_ledger.claim_run(
        "orchestrator-replacement",
        timedelta(seconds=30),
    )

    assert failed.status is AgentRunStatus.FAILED
    assert replacement is not None
    assert replacement.run_id != failed.run_id
    assert replacement.ingress_cutoff == 2
