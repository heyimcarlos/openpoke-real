"""Separately runnable durable execution worker."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
from datetime import timedelta

import asyncpg

from .agents.execution_agent import AgentsSdkExecutor
from .config import get_settings
from .services.task_queue import ExecutorKind, PostgresTaskLedger, TaskService
from .services.task_queue.execution import ExecutorRegistry, SyntheticExecutor
from .services.task_queue.projection import InteractionResultSink
from .services.task_queue.worker import TaskWorker


async def _run(*, once: bool, poll_interval_seconds: float) -> None:
    settings = get_settings()
    if not settings.database_url:
        raise RuntimeError("OPENPOKE_DATABASE_URL is not configured")
    if not settings.openrouter_api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not configured")

    pool = await asyncpg.create_pool(
        settings.database_url,
        min_size=1,
        max_size=4,
    )
    try:
        ledger = PostgresTaskLedger(pool)
        await ledger.migrate()
        task_service = TaskService(ledger)
        executors = ExecutorRegistry(
            {
                ExecutorKind.AGENT: AgentsSdkExecutor(
                    api_key=settings.openrouter_api_key,
                    model_name=settings.execution_agent_model,
                ),
                ExecutorKind.SYNTHETIC: SyntheticExecutor(),
            }
        )
        worker = TaskWorker(
            ledger,
            executors,
            worker_id=f"{socket.gethostname()}:{os.getpid()}",
            lease_duration=timedelta(seconds=120),
            execution_timeout_seconds=90,
            result_sink=InteractionResultSink(task_service),
        )

        while True:
            outcome = await worker.run_once()
            if once:
                print(
                    json.dumps(
                        {
                            "status": outcome.status.value,
                            "task_id": (
                                str(outcome.task_id) if outcome.task_id else None
                            ),
                            "attempt_count": outcome.attempt_count,
                            "failure": (
                                outcome.failure.value if outcome.failure else None
                            ),
                        }
                    )
                )
                return
            if outcome.task_id is None:
                await asyncio.sleep(poll_interval_seconds)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the OpenPoke task worker")
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
