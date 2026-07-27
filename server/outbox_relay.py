"""Relay PostgreSQL task wake events to RabbitMQ with publisher confirms."""

from __future__ import annotations

import argparse
import asyncio
import os
import socket
from datetime import timedelta

from .config import get_settings
from .database import DatabaseRole, create_role_pool
from .services.task_queue import (
    PostgresTaskLedger,
    PostgresWakeOutbox,
    RabbitMQWakeBroker,
    RelayOutcome,
    WakeOutboxRelay,
)


async def _run(*, once: bool, poll_interval_seconds: float) -> None:
    settings = get_settings()
    if not settings.database_url:
        raise RuntimeError("OPENPOKE_DATABASE_URL is not configured")
    if not settings.rabbitmq_url:
        raise RuntimeError("OPENPOKE_RABBITMQ_URL is not configured")
    pool = await create_role_pool(settings.database_url, DatabaseRole.RELAY)
    broker = await RabbitMQWakeBroker.connect(settings.rabbitmq_url)
    try:
        relay = WakeOutboxRelay(
            PostgresWakeOutbox(pool),
            broker.publisher,
            relay_id=f"{socket.gethostname()}:{os.getpid()}",
            lease_duration=timedelta(seconds=30),
        )
        ledger = PostgresTaskLedger(pool)
        while True:
            await ledger.recover_expired(limit=100)
            outcome = await relay.run_once()
            if once:
                print(outcome.value)
                return
            if outcome is RelayOutcome.IDLE:
                await asyncio.sleep(poll_interval_seconds)
    finally:
        await broker.close()
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Relay PostgreSQL task wakes to RabbitMQ"
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=0.5)
    args = parser.parse_args()
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")
    asyncio.run(
        _run(
            once=args.once,
            poll_interval_seconds=args.poll_interval,
        )
    )


if __name__ == "__main__":
    main()
