"""Separately runnable durable execution worker."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
from datetime import timedelta

from .agents.execution_agent import AgentsSdkExecutor
from .config import get_settings
from .database import DatabaseRole, create_role_pool
from .services.task_queue import ExecutorKind, PostgresTaskLedger, TaskService
from .services.task_queue.execution import ExecutorRegistry, SyntheticExecutor
from .services.task_queue.projection import InteractionResultSink
from .services.task_queue.worker import TaskWorker, WorkerOutcome


def _outcome_json(outcome: WorkerOutcome) -> str:
    return json.dumps(
        {
            "status": outcome.status.value,
            "task_id": str(outcome.task_id) if outcome.task_id else None,
            "attempt_count": outcome.attempt_count,
            "failure": outcome.failure.value if outcome.failure else None,
        }
    )


async def _worker_loop(
    worker: TaskWorker,
    *,
    poll_interval_seconds: float,
) -> None:
    while True:
        outcome = await worker.run_once()
        if outcome.task_id is None:
            await asyncio.sleep(poll_interval_seconds)


async def _run(
    *,
    once: bool,
    poll_interval_seconds: float,
    concurrency: int,
    project_results_locally: bool,
) -> None:
    settings = get_settings()
    if not settings.database_url:
        raise RuntimeError("OPENPOKE_DATABASE_URL is not configured")
    if not settings.openrouter_api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not configured")

    pool = await create_role_pool(settings.database_url, DatabaseRole.WORKER)
    try:
        ledger = PostgresTaskLedger(pool)
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
        result_sink = (
            InteractionResultSink(task_service)
            if project_results_locally
            else None
        )
        workers = [
            TaskWorker(
                ledger,
                executors,
                worker_id=f"{socket.gethostname()}:{os.getpid()}:{slot}",
                lease_duration=timedelta(seconds=120),
                execution_timeout_seconds=90,
                result_sink=result_sink,
            )
            for slot in range(concurrency)
        ]
        if once:
            print(_outcome_json(await workers[0].run_once()))
            return
        await asyncio.gather(
            *(
                _worker_loop(
                    worker,
                    poll_interval_seconds=poll_interval_seconds,
                )
                for worker in workers
            )
        )
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the OpenPoke task worker")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument(
        "--concurrency",
        type=int,
        default=2,
        help="Concurrent claim and execution slots, 1 or 2 (default: 2)",
    )
    parser.add_argument(
        "--no-local-result-projection",
        action="store_true",
        help=(
            "Keep completed results in PostgreSQL without writing to the "
            "worker's local conversation files"
        ),
    )
    args = parser.parse_args()
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")
    if args.concurrency not in {1, 2}:
        parser.error("--concurrency must be 1 or 2")
    asyncio.run(
        _run(
            once=args.once,
            poll_interval_seconds=args.poll_interval,
            concurrency=args.concurrency,
            project_results_locally=not args.no_local_result_projection,
        )
    )


if __name__ == "__main__":
    main()
