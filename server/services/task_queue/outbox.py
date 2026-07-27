"""Durable worker wake events and a fenced publication relay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Protocol
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict

from .models import ExecutorKind


class WakeEvent(BaseModel):
    """The complete broker message, deliberately excluding task data."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    event_version: int
    executor_kind: ExecutorKind


@dataclass(frozen=True)
class WakeEventLease:
    event: WakeEvent
    relay_id: str
    lease_generation: int
    expires_at: datetime
    publish_attempt_count: int


class OutboxFailureCode(str, Enum):
    UNAVAILABLE = "unavailable"
    REJECTED = "rejected"
    TIMEOUT = "timeout"


class OutboxPublishError(RuntimeError):
    """An allowlisted transport failure safe to persist."""

    def __init__(self, code: OutboxFailureCode) -> None:
        super().__init__(f"wake publication failed: {code.value}")
        self.code = code


class WakePublisher(Protocol):
    async def publish(self, event: WakeEvent) -> None:
        """Return only after the broker confirms the persistent publish."""


class RelayOutcome(str, Enum):
    IDLE = "idle"
    PUBLISHED = "published"
    RETRY = "retry"
    TRANSPORT_DEAD_LETTERED = "transport_dead_lettered"


async def append_task_wake(
    connection: asyncpg.Connection,
    *,
    task_id: UUID,
    executor_kind: ExecutorKind | str,
    source_transition: str,
    source_generation: int = 0,
) -> UUID | None:
    """Append a deduplicated wake inside an existing task transaction."""

    if await connection.fetchval(
        "SELECT to_regclass('task_wake_outbox')"
    ) is None:
        return None
    kind = (
        executor_kind.value
        if isinstance(executor_kind, ExecutorKind)
        else executor_kind
    )
    return await connection.fetchval(
        """
        INSERT INTO task_wake_outbox (
            task_id,
            executor_kind,
            source_transition,
            source_generation
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (task_id, source_transition, source_generation)
        DO UPDATE SET task_id = EXCLUDED.task_id
        RETURNING event_id
        """,
        task_id,
        kind,
        source_transition,
        source_generation,
    )


class PostgresWakeOutbox:
    """Lease publication work while PostgreSQL remains authoritative."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        max_attempts: int = 10,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("outbox max_attempts must be positive")
        self._pool = pool
        self._max_attempts = max_attempts

    async def claim(
        self,
        relay_id: str,
        lease_duration: timedelta,
    ) -> WakeEventLease | None:
        if not relay_id or len(relay_id) > 128:
            raise ValueError("relay_id must contain 1 to 128 characters")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        row = await self._pool.fetchrow(
            """
            WITH terminalized AS (
                UPDATE task_wake_outbox
                SET status = 'transport_dead_lettered',
                    lease_owner = NULL,
                    lease_expires_at = NULL
                WHERE status = 'publishing'
                  AND lease_expires_at <= clock_timestamp()
                  AND publish_attempt_count >= $3
                RETURNING event_id
            ),
            candidate AS (
                SELECT event_id
                FROM task_wake_outbox
                WHERE available_at <= clock_timestamp()
                  AND publish_attempt_count < $3
                  AND (
                      status = 'pending'
                      OR (
                          status = 'publishing'
                          AND lease_expires_at <= clock_timestamp()
                      )
                  )
                ORDER BY available_at, created_at, event_id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE task_wake_outbox AS event
            SET status = 'publishing',
                publish_attempt_count = publish_attempt_count + 1,
                lease_generation = lease_generation + 1,
                lease_owner = $1,
                lease_expires_at = clock_timestamp() + $2::interval
            FROM candidate
            WHERE event.event_id = candidate.event_id
            RETURNING event.*
            """,
            relay_id,
            lease_duration,
            self._max_attempts,
        )
        return _lease_from_row(row) if row else None

    async def complete(self, lease: WakeEventLease) -> bool:
        result = await self._pool.execute(
            """
            UPDATE task_wake_outbox
            SET status = 'published',
                published_at = clock_timestamp(),
                failure_code = NULL,
                lease_owner = NULL,
                lease_expires_at = NULL
            WHERE event_id = $1
              AND status = 'publishing'
              AND lease_owner = $2
              AND lease_generation = $3
            """,
            lease.event.event_id,
            lease.relay_id,
            lease.lease_generation,
        )
        return result == "UPDATE 1"

    async def fail(
        self,
        lease: WakeEventLease,
        failure: OutboxFailureCode,
        *,
        retry_delay: timedelta,
    ) -> RelayOutcome:
        if retry_delay < timedelta(0):
            raise ValueError("retry_delay cannot be negative")
        row = await self._pool.fetchrow(
            """
            UPDATE task_wake_outbox
            SET status = CASE
                    WHEN publish_attempt_count >= $5
                    THEN 'transport_dead_lettered'
                    ELSE 'pending'
                END,
                failure_code = $4,
                available_at = clock_timestamp() + $6::interval,
                lease_owner = NULL,
                lease_expires_at = NULL
            WHERE event_id = $1
              AND status = 'publishing'
              AND lease_owner = $2
              AND lease_generation = $3
            RETURNING status
            """,
            lease.event.event_id,
            lease.relay_id,
            lease.lease_generation,
            failure.value,
            self._max_attempts,
            retry_delay,
        )
        if row is None:
            return RelayOutcome.IDLE
        if row["status"] == "transport_dead_lettered":
            return RelayOutcome.TRANSPORT_DEAD_LETTERED
        return RelayOutcome.RETRY


class WakeOutboxRelay:
    """Publish one leased wake and fence its resulting state transition."""

    def __init__(
        self,
        outbox: PostgresWakeOutbox,
        publisher: WakePublisher,
        *,
        relay_id: str,
        lease_duration: timedelta,
        retry_delay: timedelta = timedelta(seconds=1),
    ) -> None:
        self._outbox = outbox
        self._publisher = publisher
        self._relay_id = relay_id
        self._lease_duration = lease_duration
        self._retry_delay = retry_delay

    async def run_once(self) -> RelayOutcome:
        lease = await self._outbox.claim(
            self._relay_id,
            self._lease_duration,
        )
        if lease is None:
            return RelayOutcome.IDLE
        try:
            await self._publisher.publish(lease.event)
        except OutboxPublishError as exc:
            return await self._outbox.fail(
                lease,
                exc.code,
                retry_delay=self._retry_delay,
            )
        if not await self._outbox.complete(lease):
            return RelayOutcome.IDLE
        return RelayOutcome.PUBLISHED


def _lease_from_row(row: asyncpg.Record) -> WakeEventLease:
    return WakeEventLease(
        event=WakeEvent(
            event_id=row["event_id"],
            event_version=row["event_version"],
            executor_kind=ExecutorKind(row["executor_kind"]),
        ),
        relay_id=row["lease_owner"],
        lease_generation=row["lease_generation"],
        expires_at=row["lease_expires_at"],
        publish_attempt_count=row["publish_attempt_count"],
    )
