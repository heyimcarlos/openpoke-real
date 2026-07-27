"""Separately runnable durable interaction Agent Run worker."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket

from .config import get_settings
from .database import DatabaseRole, create_role_pool
from .services.task_queue import PostgresTaskLedger, TaskService
from .services.threads import PostgresThreadLedger
from .services.threads.worker import AgentRunWorker


async def _loop(worker: AgentRunWorker, poll_interval: float) -> None:
    while True:
        outcome = await worker.run_once()
        if outcome.run_id is None:
            await asyncio.sleep(poll_interval)


async def _run(*, once: bool, poll_interval: float) -> None:
    settings = get_settings()
    if not settings.database_url:
        raise RuntimeError("OPENPOKE_DATABASE_URL is not configured")
    if not settings.openrouter_api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not configured")
    pool = await create_role_pool(
        settings.database_url,
        DatabaseRole.ORCHESTRATOR,
    )
    try:
        worker = AgentRunWorker(
            PostgresThreadLedger(pool),
            TaskService(PostgresTaskLedger(pool)),
            worker_id=f"{socket.gethostname()}:{os.getpid()}",
        )
        if once:
            outcome = await worker.run_once()
            print(
                json.dumps(
                    {
                        "status": outcome.status.value,
                        "run_id": str(outcome.run_id) if outcome.run_id else None,
                    }
                )
            )
            return
        await _loop(worker, poll_interval)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the OpenPoke orchestrator worker"
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=0.5)
    args = parser.parse_args()
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")
    asyncio.run(_run(once=args.once, poll_interval=args.poll_interval))


if __name__ == "__main__":
    main()
