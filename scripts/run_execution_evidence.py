"""Measure deterministic overload, idempotency, and process recovery evidence."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
import json
import math
import os
import platform
import random
import re
import shlex
import signal
import socket
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from server.services.task_queue import (
    AdmissionRejected,
    ExecutorKind,
    PostgresTaskLedger,
    Principal,
    StaleLease,
    SubmitTask,
    TaskLease,
    TaskService,
)
from server.services.task_queue.execution import ExecutorRegistry, SyntheticExecutor
from server.services.task_queue.worker import TaskWorker


SEED = 20260727
GLOBAL_ACTIVE_LIMIT = 8
TENANT_ACTIVE_LIMIT = 2
TENANT_OUTSTANDING_LIMIT = 50
WORKER_CONCURRENCY = 16
TERMINAL_STATUSES = {"completed", "dead_lettered", "cancelled"}
DEFAULT_DATABASE_URL = (
    "postgresql://postgres@127.0.0.1:55432/openpoke_test"
)
_SCHEMA_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic OpenPoke execution evidence"
    )
    parser.add_argument("--task-duration-ms", type=int, default=20)
    parser.add_argument("--lease-ms", type=int, default=200)
    parser.add_argument("--recovery-duration-ms", type=int, default=700)
    parser.add_argument("--report", type=Path)
    parser.add_argument(
        "--child",
        choices=("accept", "worker"),
        help=argparse.SUPPRESS,
    )
    return parser


def _database_url() -> str:
    return os.getenv("OPENPOKE_EVIDENCE_DATABASE_URL", DEFAULT_DATABASE_URL)


def _schema() -> str | None:
    value = os.getenv("OPENPOKE_EVIDENCE_SCHEMA")
    if value and not _SCHEMA_PATTERN.fullmatch(value):
        raise ValueError("OPENPOKE_EVIDENCE_SCHEMA is not a valid identifier")
    return value


async def _pool(
    *,
    schema: str,
    application_name: str,
    max_size: int,
) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        _database_url(),
        min_size=1,
        max_size=max_size,
        server_settings={
            "application_name": application_name,
            "search_path": schema,
        },
    )


def _principal(spec: dict[str, Any]) -> Principal:
    return Principal(
        actor_id=spec["actor_id"],
        tenant_id=spec["tenant_id"],
        scopes=frozenset({"tasks:create"}),
    )


def _command(spec: dict[str, Any]) -> SubmitTask:
    return SubmitTask(
        idempotency_key=spec["idempotency_key"],
        origin_turn_id=spec["origin_turn_id"],
        agent_name="synthetic evidence",
        executor_kind=ExecutorKind.SYNTHETIC,
        input=spec["input"],
    )


async def _watch_pool_usage(
    pool: asyncpg.Pool,
    stop: asyncio.Event,
) -> int:
    peak = 0
    while not stop.is_set():
        peak = max(peak, pool.get_size() - pool.get_idle_size())
        await asyncio.sleep(0.0005)
    return max(peak, pool.get_size() - pool.get_idle_size())


def _ledger(pool: asyncpg.Pool) -> PostgresTaskLedger:
    return PostgresTaskLedger(
        pool,
        global_active_limit=GLOBAL_ACTIVE_LIMIT,
        tenant_active_limit=TENANT_ACTIVE_LIMIT,
        tenant_outstanding_limit=TENANT_OUTSTANDING_LIMIT,
    )


async def _submit_batch(
    pool: asyncpg.Pool,
    service: TaskService,
    specs: list[dict[str, Any]],
) -> dict[str, Any]:
    gate = asyncio.Event()
    stop_monitor = asyncio.Event()
    pool_monitor = asyncio.create_task(_watch_pool_usage(pool, stop_monitor))

    async def submit_one(spec: dict[str, Any]) -> dict[str, Any]:
        await gate.wait()
        started = time.perf_counter()
        try:
            record = await service.submit(_principal(spec), _command(spec))
        except AdmissionRejected:
            return {
                "accepted": False,
                "latency_ms": (time.perf_counter() - started) * 1_000,
                "tenant_id": spec["tenant_id"],
            }
        return {
            "accepted": True,
            "latency_ms": (time.perf_counter() - started) * 1_000,
            "task_id": str(record.task_id),
            "tenant_id": spec["tenant_id"],
        }

    submissions = [asyncio.create_task(submit_one(spec)) for spec in specs]
    await asyncio.sleep(0)
    gate.set()
    try:
        results = await asyncio.gather(*submissions)
    finally:
        stop_monitor.set()
        peak_pool_usage = await pool_monitor
    return {
        "pool_peak_in_use": peak_pool_usage,
        "results": results,
    }


async def _accept_child(schema: str) -> dict[str, Any]:
    specs = json.loads(sys.stdin.read())
    pool = await _pool(
        schema=schema,
        application_name="openpoke-evidence-accept",
        max_size=20,
    )
    try:
        return await _submit_batch(pool, TaskService(_ledger(pool)), specs)
    finally:
        await pool.close()


async def _outstanding_count(
    pool: asyncpg.Pool,
    prefix: str,
) -> tuple[int, int]:
    row = await pool.fetchrow(
        """
        SELECT count(*) AS total,
               count(*) FILTER (
                   WHERE status IN ('queued', 'running')
               ) AS outstanding
        FROM execution_tasks
        WHERE left(idempotency_key, length($1)) = $1
        """,
        prefix,
    )
    return row["total"], row["outstanding"]


async def _drain(
    pool: asyncpg.Pool,
    *,
    prefix: str,
    expected: int,
    workers: int,
    lease_ms: int,
    noisy_gate: asyncio.Event | None = None,
) -> dict[str, Any]:
    ledger = _ledger(pool)
    executor = (
        _GatedSyntheticExecutor(noisy_gate)
        if noisy_gate is not None
        else SyntheticExecutor()
    )
    executors = ExecutorRegistry(
        {ExecutorKind.SYNTHETIC: executor}
    )
    stop = asyncio.Event()
    outcomes: Counter[str] = Counter()
    stop_monitor = asyncio.Event()
    pool_monitor = asyncio.create_task(_watch_pool_usage(pool, stop_monitor))

    async def work(index: int) -> None:
        worker = TaskWorker(
            ledger,
            executors,
            worker_id=f"{socket.gethostname()}:{os.getpid()}:{index}",
            lease_duration=timedelta(milliseconds=lease_ms),
            execution_timeout_seconds=10,
        )
        while not stop.is_set():
            outcome = await worker.run_once()
            outcomes[outcome.status.value] += 1
            if outcome.task_id is None:
                await asyncio.sleep(0.002)

    worker_tasks = [
        asyncio.create_task(work(index)) for index in range(workers)
    ]
    try:
        deadline = time.monotonic() + 60
        while True:
            total, outstanding = await _outstanding_count(pool, prefix)
            if total == expected and outstanding == 0:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"worker drain timed out: total={total}, "
                    f"expected={expected}, outstanding={outstanding}"
                )
            await asyncio.sleep(0.003)
    finally:
        stop.set()
        await asyncio.gather(*worker_tasks)
        stop_monitor.set()
        peak_pool_usage = await pool_monitor
    return {
        "outcomes": dict(outcomes),
        "pool_peak_in_use": peak_pool_usage,
    }


def _lease_json(lease: TaskLease) -> dict[str, Any]:
    return {
        "task_id": str(lease.task_id),
        "tenant_id": lease.tenant_id,
        "actor_id": lease.actor_id,
        "origin_turn_id": lease.origin_turn_id,
        "agent_name": lease.agent_name,
        "executor_kind": lease.executor_kind.value,
        "input": lease.input,
        "attempt_count": lease.attempt_count,
        "lease_generation": lease.lease_generation,
        "worker_id": lease.worker_id,
        "expires_at": lease.expires_at.isoformat(),
    }


def _lease_from_json(value: dict[str, Any]) -> TaskLease:
    return TaskLease(
        task_id=UUID(value["task_id"]),
        tenant_id=value["tenant_id"],
        actor_id=value["actor_id"],
        origin_turn_id=value["origin_turn_id"],
        agent_name=value["agent_name"],
        executor_kind=ExecutorKind(value["executor_kind"]),
        input=value["input"],
        attempt_count=value["attempt_count"],
        lease_generation=value["lease_generation"],
        worker_id=value["worker_id"],
        expires_at=datetime.fromisoformat(value["expires_at"]),
    )


class _ReportingSyntheticExecutor(SyntheticExecutor):
    async def execute(self, lease: TaskLease) -> dict[str, Any]:
        print(json.dumps({"lease": _lease_json(lease)}), flush=True)
        return await super().execute(lease)


class _GatedSyntheticExecutor(SyntheticExecutor):
    def __init__(self, gate: asyncio.Event) -> None:
        super().__init__()
        self._gate = gate

    async def execute(self, lease: TaskLease) -> dict[str, Any]:
        if lease.tenant_id == "noisy":
            await self._gate.wait()
        return await super().execute(lease)


async def _worker_child(
    schema: str,
    *,
    lease_ms: int,
) -> dict[str, Any]:
    pool = await _pool(
        schema=schema,
        application_name="openpoke-evidence-killed-worker",
        max_size=2,
    )
    try:
        ledger = _ledger(pool)
        worker = TaskWorker(
            ledger,
            ExecutorRegistry(
                {ExecutorKind.SYNTHETIC: _ReportingSyntheticExecutor()}
            ),
            worker_id=f"killed:{socket.gethostname()}:{os.getpid()}",
            lease_duration=timedelta(milliseconds=lease_ms),
            execution_timeout_seconds=10,
        )
        outcome = await worker.run_once()
        return {"outcome": outcome.status.value}
    finally:
        await pool.close()


def _child_environment(schema: str) -> dict[str, str]:
    env = os.environ.copy()
    env["OPENPOKE_EVIDENCE_DATABASE_URL"] = _database_url()
    env["OPENPOKE_EVIDENCE_SCHEMA"] = schema
    for name in (
        "OPENROUTER_API_KEY",
        "COMPOSIO_API_KEY",
        "COMPOSIO_GMAIL_AUTH_CONFIG_ID",
    ):
        env.pop(name, None)
    return env


def _child_command(
    child: str,
    *,
    lease_ms: int,
) -> list[str]:
    return [
        sys.executable,
        "-m",
        "scripts.run_execution_evidence",
        "--child",
        child,
        "--lease-ms",
        str(lease_ms),
    ]


async def _invoke_accept(
    schema: str,
    specs: list[dict[str, Any]],
    *,
    lease_ms: int,
) -> dict[str, Any]:
    process = await asyncio.create_subprocess_exec(
        *_child_command("accept", lease_ms=lease_ms),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_child_environment(schema),
    )
    try:
        stdout, stderr = await process.communicate(
            json.dumps(specs).encode("utf-8")
        )
    except BaseException:
        await _terminate(process)
        raise
    if process.returncode != 0:
        raise RuntimeError(
            f"acceptance process failed: {stderr.decode().strip()}"
        )
    return json.loads(stdout)


async def _start_worker(
    schema: str,
    *,
    lease_ms: int,
) -> tuple[asyncio.subprocess.Process, TaskLease]:
    process = await asyncio.create_subprocess_exec(
        *_child_command("worker", lease_ms=lease_ms),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_child_environment(schema),
    )
    try:
        if process.stdout is None:
            raise RuntimeError("worker stdout pipe was not created")
        lease_line = await asyncio.wait_for(
            process.stdout.readline(),
            timeout=10,
        )
        if not lease_line:
            raise RuntimeError("worker exited before reporting its lease")
        lease = _lease_from_json(json.loads(lease_line)["lease"])
        return process, lease
    except BaseException:
        await _terminate(process)
        raise


async def _finish_worker(
    process: asyncio.subprocess.Process,
) -> dict[str, Any]:
    try:
        stdout, stderr = await process.communicate()
    except BaseException:
        await _terminate(process)
        raise
    if process.returncode != 0:
        raise RuntimeError(f"worker failed: {stderr.decode().strip()}")
    return json.loads(stdout)


async def _terminate(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        process.kill()
    await process.wait()


def _task_spec(
    *,
    prefix: str,
    tenant_id: str,
    index: int,
    duration_ms: int,
    mode: str = "success",
) -> dict[str, Any]:
    return {
        "actor_id": f"actor-{tenant_id}",
        "tenant_id": tenant_id,
        "idempotency_key": f"{prefix}{tenant_id}-{index}",
        "origin_turn_id": f"{prefix}turn-{tenant_id}-{index}",
        "input": {"duration_ms": duration_ms, "mode": mode},
    }


def _scenario_specs(
    clients: int,
    *,
    prefix: str,
    task_duration_ms: int,
) -> list[dict[str, Any]]:
    if clients == 1:
        return [
            _task_spec(
                prefix=prefix,
                tenant_id="single",
                index=0,
                duration_ms=task_duration_ms,
            )
        ]

    distributions = {
        10: (4, (2, 1, 1, 1)),
        50: (30, (5, 5, 5, 4)),
        100: (60, (10, 10, 10, 9)),
    }
    noisy_count, filler_counts = distributions[clients]
    specs: list[dict[str, Any]] = []
    for index in range(noisy_count):
        specs.append(
            _task_spec(
                prefix=prefix,
                tenant_id="noisy",
                index=index,
                duration_ms=max(task_duration_ms, 25),
            )
        )
    for filler, filler_count in enumerate(filler_counts):
        for index in range(filler_count):
            mode = "success"
            if filler == 0 and index == 0:
                mode = "fail_always"
            elif filler == 1 and index == 0:
                mode = "fail_once"
            specs.append(
                _task_spec(
                    prefix=prefix,
                    tenant_id=f"filler-{filler + 1}",
                    index=index,
                    duration_ms=task_duration_ms,
                    mode=mode,
                )
            )
    specs.append(
        _task_spec(
            prefix=prefix,
            tenant_id="quiet",
            index=0,
            duration_ms=task_duration_ms,
        )
    )
    random.Random(SEED + clients).shuffle(specs)
    return specs


def _percentiles(values: list[float]) -> dict[str, float]:
    if not values:
        return {"p50_ms": 0.0, "p95_ms": 0.0, "p99_ms": 0.0}
    ordered = sorted(values)

    def nearest_rank(percent: float) -> float:
        index = max(0, math.ceil(percent * len(ordered)) - 1)
        return round(ordered[index], 3)

    return {
        "p50_ms": nearest_rank(0.50),
        "p95_ms": nearest_rank(0.95),
        "p99_ms": nearest_rank(0.99),
    }


async def _sample_execution(
    pool: asyncpg.Pool,
    prefix: str,
    operation: asyncio.Task[dict[str, Any]],
) -> dict[str, Any]:
    peak_global = 0
    peak_by_tenant: defaultdict[str, int] = defaultdict(int)
    oldest_backlog_ms = 0.0
    while not operation.done():
        rows = await pool.fetch(
            """
            SELECT tenant_id,
                   count(*) FILTER (
                       WHERE status = 'running'
                         AND lease_expires_at > clock_timestamp()
                   ) AS active,
                   COALESCE(
                       extract(
                           epoch FROM (
                               clock_timestamp()
                               - min(created_at) FILTER (
                                   WHERE status = 'queued'
                               )
                           )
                       ) * 1000,
                       0
                   ) AS oldest_backlog_ms
            FROM execution_tasks
            WHERE left(idempotency_key, length($1)) = $1
            GROUP BY tenant_id
            """,
            prefix,
        )
        active = sum(row["active"] for row in rows)
        peak_global = max(peak_global, active)
        for row in rows:
            peak_by_tenant[row["tenant_id"]] = max(
                peak_by_tenant[row["tenant_id"]],
                row["active"],
            )
            oldest_backlog_ms = max(
                oldest_backlog_ms,
                float(row["oldest_backlog_ms"]),
            )
        await asyncio.sleep(0.002)
    worker = await operation
    return {
        "peak_active_attempts": peak_global,
        "peak_active_attempts_by_tenant": dict(peak_by_tenant),
        "oldest_backlog_ms": round(oldest_backlog_ms, 3),
        "worker": worker,
    }


async def _wait_for_noisy_backlog(
    pool: asyncpg.Pool,
    prefix: str,
) -> dict[str, int]:
    deadline = time.monotonic() + 10
    while True:
        row = await pool.fetchrow(
            """
            SELECT count(*) FILTER (
                       WHERE tenant_id = 'noisy'
                         AND status = 'running'
                         AND lease_expires_at > clock_timestamp()
                   ) AS active,
                   count(*) FILTER (
                       WHERE tenant_id = 'noisy'
                         AND status = 'queued'
                   ) AS queued
            FROM execution_tasks
            WHERE left(idempotency_key, length($1)) = $1
            """,
            prefix,
        )
        if row["active"] == TENANT_ACTIVE_LIMIT and row["queued"] > 0:
            return {
                "noisy_active": row["active"],
                "noisy_queued": row["queued"],
            }
        if time.monotonic() >= deadline:
            raise TimeoutError("noisy tenant backlog was not observed")
        await asyncio.sleep(0.002)


async def _records(
    pool: asyncpg.Pool,
    prefix: str,
) -> list[asyncpg.Record]:
    return await pool.fetch(
        """
        SELECT task_id, tenant_id, status, attempt_count,
               created_at, started_at, finished_at
        FROM execution_tasks
        WHERE left(idempotency_key, length($1)) = $1
        ORDER BY created_at, task_id
        """,
        prefix,
    )


async def _run_scenario(
    pool: asyncpg.Pool,
    schema: str,
    *,
    run_id: str,
    clients: int,
    task_duration_ms: int,
    lease_ms: int,
) -> dict[str, Any]:
    scenario_started = time.perf_counter()
    prefix = f"evidence-{run_id}-n{clients}-"
    specs = _scenario_specs(
        clients,
        prefix=prefix,
        task_duration_ms=task_duration_ms,
    )
    initial_specs = [
        spec for spec in specs if spec["tenant_id"] != "quiet"
    ]
    quiet_specs = [
        spec for spec in specs if spec["tenant_id"] == "quiet"
    ]
    service = TaskService(_ledger(pool))
    initial_acceptance = await _submit_batch(pool, service, initial_specs)
    submission_finished = time.perf_counter()
    initial_accepted = sum(
        result["accepted"] for result in initial_acceptance["results"]
    )
    worker_pool = await _pool(
        schema=schema,
        application_name=f"openpoke-evidence-load-{clients}",
        max_size=WORKER_CONCURRENCY + 2,
    )
    noisy_gate = asyncio.Event() if quiet_specs else None
    drain = asyncio.create_task(
        _drain(
            worker_pool,
            prefix=prefix,
            expected=initial_accepted + len(quiet_specs),
            workers=WORKER_CONCURRENCY,
            lease_ms=max(lease_ms, task_duration_ms * 5 + 100),
            noisy_gate=noisy_gate,
        )
    )
    sampling = asyncio.create_task(_sample_execution(pool, prefix, drain))
    try:
        release_condition = None
        quiet_acceptance = {"pool_peak_in_use": 0, "results": []}
        if quiet_specs:
            release_condition = await _wait_for_noisy_backlog(pool, prefix)
            quiet_acceptance = await _submit_batch(
                pool,
                service,
                quiet_specs,
            )
            submission_finished = time.perf_counter()
            noisy_gate.set()
        observed = await sampling
    except BaseException:
        if noisy_gate is not None:
            noisy_gate.set()
        sampling.cancel()
        drain.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(sampling, drain)
        raise
    finally:
        await worker_pool.close()
    rows = await _records(pool, prefix)
    scenario_seconds = time.perf_counter() - scenario_started
    acceptance_results = (
        initial_acceptance["results"] + quiet_acceptance["results"]
    )
    accepted = sum(result["accepted"] for result in acceptance_results)
    rejected = len(acceptance_results) - accepted
    queue_delays = [
        (row["started_at"] - row["created_at"]).total_seconds() * 1_000
        for row in rows
        if row["started_at"] is not None
    ]
    end_to_end = [
        (row["finished_at"] - row["created_at"]).total_seconds() * 1_000
        for row in rows
        if row["finished_at"] is not None
    ]
    by_tenant_acceptance: defaultdict[str, dict[str, int]] = defaultdict(
        lambda: {"accepted": 0, "rejected": 0}
    )
    for result in acceptance_results:
        key = "accepted" if result["accepted"] else "rejected"
        by_tenant_acceptance[result["tenant_id"]][key] += 1
    terminal_by_tenant = Counter(row["tenant_id"] for row in rows)
    tenants: dict[str, Any] = {}
    for tenant_id, counts in sorted(by_tenant_acceptance.items()):
        tenants[tenant_id] = {
            **counts,
            "terminal": terminal_by_tenant[tenant_id],
            "throughput_per_second": round(
                terminal_by_tenant[tenant_id] / max(scenario_seconds, 0.000001),
                3,
            ),
        }

    quiet_before_noisy_finished = clients == 1 or (
        min(
            row["started_at"]
            for row in rows
            if row["tenant_id"] == "quiet"
        )
        < max(
            row["finished_at"]
            for row in rows
            if row["tenant_id"] == "noisy"
        )
    )
    quiet_progress = clients == 1 or bool(
        release_condition
        and release_condition["noisy_active"] == TENANT_ACTIVE_LIMIT
        and release_condition["noisy_queued"] > 0
        and terminal_by_tenant["quiet"] == 1
        and quiet_before_noisy_finished
    )

    all_terminal = (
        len(rows) == accepted
        and all(row["status"] in TERMINAL_STATUSES for row in rows)
        and all(row["finished_at"] is not None for row in rows)
    )
    return {
        "clients": clients,
        "accepted": accepted,
        "rejected": rejected,
        "submission_throughput_per_second": round(
            clients
            / max(submission_finished - scenario_started, 0.000001),
            3,
        ),
        "scenario_duration_seconds": round(scenario_seconds, 6),
        "submission_latency": _percentiles(
            [result["latency_ms"] for result in acceptance_results]
        ),
        "queue_delay": _percentiles(queue_delays),
        "end_to_end": _percentiles(end_to_end),
        "oldest_backlog_ms": observed["oldest_backlog_ms"],
        "peak_active_attempts": observed["peak_active_attempts"],
        "peak_active_attempts_by_tenant": (
            observed["peak_active_attempts_by_tenant"]
        ),
        "retries": sum(max(row["attempt_count"] - 1, 0) for row in rows),
        "dead_letters": sum(
            row["status"] == "dead_lettered" for row in rows
        ),
        "tenants": tenants,
        "quiet_tenant_progressed_while_noisy_backlogged": quiet_progress,
        "quiet_started_before_noisy_finished": quiet_before_noisy_finished,
        "quiet_release_observation": release_condition,
        "all_accepted_terminal": all_terminal,
        "pool_usage": {
            "acceptance_configured_max": 5,
            "acceptance_peak_in_use": max(
                initial_acceptance["pool_peak_in_use"],
                quiet_acceptance["pool_peak_in_use"],
            ),
            "worker_configured_max": WORKER_CONCURRENCY + 2,
            "worker_peak_in_use": observed["worker"]["pool_peak_in_use"],
        },
    }


async def _run_acceptance_replay(
    pool: asyncpg.Pool,
    schema: str,
    *,
    run_id: str,
    task_duration_ms: int,
    lease_ms: int,
) -> dict[str, Any]:
    prefix = f"evidence-{run_id}-replay-"
    specs = [
        _task_spec(
            prefix=prefix,
            tenant_id="replay",
            index=index,
            duration_ms=task_duration_ms,
        )
        for index in range(3)
    ]
    first = await _invoke_accept(schema, specs, lease_ms=lease_ms)
    second = await _invoke_accept(schema, specs, lease_ms=lease_ms)
    first_ids = [result["task_id"] for result in first["results"]]
    second_ids = [result["task_id"] for result in second["results"]]
    for _ in specs:
        worker, _lease = await _start_worker(
            schema,
            lease_ms=max(lease_ms, task_duration_ms * 5 + 100),
        )
        await _finish_worker(worker)
    rows = await _records(pool, prefix)
    return {
        "accepted": len(rows),
        "same_task_ids": first_ids == second_ids,
        "fresh_process_completed": (
            len(rows) == 3
            and all(row["status"] == "completed" for row in rows)
        ),
    }


async def _wait_for_expiry(
    pool: asyncpg.Pool,
    task_id: UUID,
) -> None:
    deadline = time.monotonic() + 10
    while True:
        expired = await pool.fetchval(
            """
            SELECT clock_timestamp() >= lease_expires_at
            FROM execution_tasks
            WHERE task_id = $1
            """,
            task_id,
        )
        if expired:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("captured worker lease did not expire")
        await asyncio.sleep(0.005)


async def _run_lease_recovery(
    pool: asyncpg.Pool,
    ledger: PostgresTaskLedger,
    schema: str,
    *,
    run_id: str,
    lease_ms: int,
    recovery_duration_ms: int,
) -> dict[str, Any]:
    prefix = f"evidence-{run_id}-recovery-"
    spec = _task_spec(
        prefix=prefix,
        tenant_id="recovery",
        index=0,
        duration_ms=recovery_duration_ms,
    )
    accepted = await _invoke_accept(schema, [spec], lease_ms=lease_ms)
    task_id = UUID(accepted["results"][0]["task_id"])
    killed, captured_lease = await _start_worker(
        schema,
        lease_ms=lease_ms,
    )
    await _terminate(killed)
    killed_by_sigkill = killed.returncode == -signal.SIGKILL
    recovery_started = time.perf_counter()
    await _wait_for_expiry(pool, task_id)

    replacement_lease_ms = max(
        lease_ms * 5,
        recovery_duration_ms + 500,
    )
    replacement, replacement_lease = await _start_worker(
        schema,
        lease_ms=replacement_lease_ms,
    )
    try:
        stale_results_rejected = 0
        try:
            await ledger.complete(captured_lease, {"response": "stale"})
        except StaleLease:
            stale_results_rejected = 1
        replacement_summary = await _finish_worker(replacement)
    finally:
        await _terminate(replacement)
    recovery_seconds = time.perf_counter() - recovery_started
    record = await ledger.get("recovery", task_id)
    return {
        "attempt_count": record.attempt_count if record else None,
        "fresh_process_completed": bool(
            record
            and record.status.value == "completed"
            and replacement_summary["outcome"] == "completed"
        ),
        "killed_by_sigkill": killed_by_sigkill,
        "replacement_attempt_count": replacement_lease.attempt_count,
        "stale_results_rejected": stale_results_rejected,
        "winner_result_preserved": bool(
            record
            and record.result == {
                "response": "synthetic task completed"
            }
        ),
        "recovery_duration_ms": round(recovery_seconds * 1_000, 3),
    }


def _safe_command() -> str:
    return shlex.join(
        [
            "uv",
            "run",
            "--locked",
            "python",
            "-m",
            "scripts.run_execution_evidence",
            *sys.argv[1:],
        ]
    )


def _revision() -> str:
    try:
        revision = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        dirty = subprocess.check_output(
            ["git", "status", "--porcelain"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        return f"{revision}-dirty" if dirty else revision
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _write_report(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Execution durability and overload evidence",
        "",
        "Synthetic work only. No model-provider executor is configured.",
        "",
        "## Reproduce",
        "",
        "```bash",
        "docker compose --env-file compose.test.env "
        "-f compose.test.yaml up -d --wait",
        "uv run --locked python -m scripts.run_execution_evidence "
        "--report docs/evidence/execution-load.md",
        "```",
        "",
        f"- Revision: `{summary['revision']}`",
        f"- Seed: `{summary['seed']}`",
        f"- Duration: `{summary['duration_seconds']:.3f}s`",
        f"- Exact command: `{summary['command']}`",
        f"- Machine: `{summary['machine']['platform']}`",
        f"- CPUs: `{summary['machine']['cpu_count']}`",
        f"- Python: `{summary['machine']['python']}`",
        f"- PostgreSQL: `{summary['machine']['postgresql']}`",
        "",
        "## Workload",
        "",
        "The 1, 10, 50, and 100 client scenarios release their initial "
        "submissions from one barrier. Larger cases use one noisy tenant, one "
        "later quiet task, and "
        "four filler tenants. Workers start only after initial admission. The "
        "quiet task is admitted after PostgreSQL observes two active noisy "
        "Attempts and a queued noisy backlog. A synthetic gate holds those "
        "noisy claims until that observation, then releases them. Sixteen "
        "worker coroutines use "
        "ledger caps of eight globally and two per tenant. The fixed seed "
        "places retry and dead-letter failures on guaranteed filler tasks.",
        "",
        "## Scenario results",
        "",
        "| Clients | Accepted | Rejected | Submit/s | Submit p50/p95/p99 ms | "
        "Queue p50/p95/p99 ms | End-to-end p50/p95/p99 ms | Backlog ms | "
        "Active | Retries | DLQ |",
        "| ---: | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for scenario in summary["scenarios"]:
        submit = scenario["submission_latency"]
        queue = scenario["queue_delay"]
        end_to_end = scenario["end_to_end"]
        lines.append(
            f"| {scenario['clients']} | {scenario['accepted']} | "
            f"{scenario['rejected']} | "
            f"{scenario['submission_throughput_per_second']} | "
            f"{submit['p50_ms']}/{submit['p95_ms']}/{submit['p99_ms']} | "
            f"{queue['p50_ms']}/{queue['p95_ms']}/{queue['p99_ms']} | "
            f"{end_to_end['p50_ms']}/{end_to_end['p95_ms']}/"
            f"{end_to_end['p99_ms']} | {scenario['oldest_backlog_ms']} | "
            f"{scenario['peak_active_attempts']} | {scenario['retries']} | "
            f"{scenario['dead_letters']} |"
        )
    lines.extend(
        [
            "",
            "## Tenant throughput and pool use",
            "",
            "| Clients | Tenant | Accepted | Rejected | Terminal | Tasks/s |",
            "| ---: | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for scenario in summary["scenarios"]:
        for tenant, metrics in scenario["tenants"].items():
            lines.append(
                f"| {scenario['clients']} | {tenant} | "
                f"{metrics['accepted']} | {metrics['rejected']} | "
                f"{metrics['terminal']} | "
                f"{metrics['throughput_per_second']} |"
            )
    lines.extend(
        [
            "",
            "| Clients | Accept pool peak/max | Worker pool peak/max |",
            "| ---: | ---: | ---: |",
        ]
    )
    for scenario in summary["scenarios"]:
        usage = scenario["pool_usage"]
        lines.append(
            f"| {scenario['clients']} | "
            f"{usage['acceptance_peak_in_use']}/"
            f"{usage['acceptance_configured_max']} | "
            f"{usage['worker_peak_in_use']}/"
            f"{usage['worker_configured_max']} |"
        )
    recovery = summary["lease_recovery"]
    lines.extend(
        [
            "",
            "## Recovery evidence",
            "",
            "A separate acceptance process committed three commands and "
            "exited. A fresh process replayed them to the same task IDs. "
            "Separate worker processes then completed them.",
            "",
            "A worker was SIGKILLed after reporting its committed Attempt-one "
            "lease. PostgreSQL time established expiry. A fresh worker claimed "
            "Attempt two. The stale completion was rejected while Attempt two "
            "was active, and the winner result remained unchanged.",
            "",
            f"- Recovery duration: `{recovery['recovery_duration_ms']}ms`",
            f"- Replacement attempt: `{recovery['replacement_attempt_count']}`",
            f"- Stale results rejected: "
            f"`{recovery['stale_results_rejected']}`",
            "",
            "## Correctness gates",
            "",
        ]
    )
    for name, passed in summary["invariants"].items():
        if name != "all_passed":
            lines.append(f"- [{'x' if passed else ' '}] {name.replace('_', ' ')}")
    lines.extend(
        [
            "",
            "The CLI exits nonzero on correctness failures. Timing values are "
            "observations, never gates. Existing concurrent PostgreSQL claim "
            "tests remain the transactional capacity proof. Active Attempts, "
            "backlog age, and pool use are sampled operational evidence.",
            "",
            "## Limitations",
            "",
            "- Local synthetic measurements do not establish a Cloud Run SLO.",
            "- Very small scenario p99 values have little statistical value.",
            "- Retry timing is not broken into separate per-Attempt spans.",
            "- Public chat durability is outside this internal acceptance seam.",
            "- PostgreSQL is both ledger and queue. RabbitMQ wake-ups, an "
            "outbox, and workflow dependencies remain later architecture.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.task_duration_ms < 0 or args.task_duration_ms > 5_000:
        raise ValueError("--task-duration-ms must be between 0 and 5000")
    if args.lease_ms < 20:
        raise ValueError("--lease-ms must be at least 20")
    if (
        args.recovery_duration_ms <= args.lease_ms
        or args.recovery_duration_ms > 5_000
    ):
        raise ValueError(
            "--recovery-duration-ms must exceed --lease-ms and be at most 5000"
        )

    supplied_schema = _schema()
    schema = supplied_schema or f"openpoke_evidence_{uuid4().hex}"
    controller_connection: asyncpg.Connection | None = None
    pool: asyncpg.Pool | None = None
    started = time.perf_counter()
    if supplied_schema is None:
        controller_connection = await asyncpg.connect(_database_url())
        await controller_connection.execute(f'CREATE SCHEMA "{schema}"')
    try:
        pool = await _pool(
            schema=schema,
            application_name="openpoke-evidence-controller",
            max_size=5,
        )
        ledger = PostgresTaskLedger(
            pool,
            global_active_limit=GLOBAL_ACTIVE_LIMIT,
            tenant_active_limit=TENANT_ACTIVE_LIMIT,
            tenant_outstanding_limit=TENANT_OUTSTANDING_LIMIT,
        )
        await ledger.migrate()
        run_id = uuid4().hex
        replay = await _run_acceptance_replay(
            pool,
            schema,
            run_id=run_id,
            task_duration_ms=args.task_duration_ms,
            lease_ms=args.lease_ms,
        )
        recovery = await _run_lease_recovery(
            pool,
            ledger,
            schema,
            run_id=run_id,
            lease_ms=args.lease_ms,
            recovery_duration_ms=args.recovery_duration_ms,
        )
        scenarios = [
            await _run_scenario(
                pool,
                schema,
                run_id=run_id,
                clients=clients,
                task_duration_ms=args.task_duration_ms,
                lease_ms=args.lease_ms,
            )
            for clients in (1, 10, 50, 100)
        ]
        accepted = replay["accepted"] + 1 + sum(
            scenario["accepted"] for scenario in scenarios
        )
        rejected = sum(scenario["rejected"] for scenario in scenarios)
        all_terminal = all(
            scenario["all_accepted_terminal"] for scenario in scenarios
        )
        invariants = {
            "acceptance_replay_returns_same_task_ids": replay["same_task_ids"],
            "acceptance_survives_process_exit": replay[
                "fresh_process_completed"
            ],
            "killed_worker_reclaimed_as_attempt_two": (
                recovery["attempt_count"] == 2
                and recovery["replacement_attempt_count"] == 2
                and recovery["fresh_process_completed"]
            ),
            "worker_process_was_killed_by_sigkill": recovery[
                "killed_by_sigkill"
            ],
            "stale_completion_rejected": (
                recovery["stale_results_rejected"] == 1
                and recovery["winner_result_preserved"]
            ),
            "all_accepted_tasks_terminal_and_queryable": all_terminal,
            "sampled_global_active_never_exceeds_eight": all(
                scenario["peak_active_attempts"] <= GLOBAL_ACTIVE_LIMIT
                for scenario in scenarios
            ),
            "sampled_tenant_active_never_exceeds_two": all(
                max(
                    scenario["peak_active_attempts_by_tenant"].values(),
                    default=0,
                )
                <= TENANT_ACTIVE_LIMIT
                for scenario in scenarios
            ),
            "noisy_tenant_admits_fifty_and_rejects_ten": (
                scenarios[-1]["tenants"]["noisy"]["accepted"] == 50
                and scenarios[-1]["tenants"]["noisy"]["rejected"] == 10
            ),
            "later_quiet_tenant_progresses_before_noisy_finishes": all(
                scenario[
                    "quiet_tenant_progressed_while_noisy_backlogged"
                ]
                for scenario in scenarios
            ),
            "only_synthetic_executors_used": (
                await pool.fetchval(
                    """
                    SELECT bool_and(executor_kind = 'synthetic')
                    FROM execution_tasks
                    WHERE left(idempotency_key, length($1)) = $1
                    """,
                    f"evidence-{run_id}-",
                )
                is True
            ),
        }
        invariants["all_passed"] = all(invariants.values())
        postgresql = await pool.fetchval("SHOW server_version")
        summary = {
            "ok": invariants["all_passed"],
            "seed": SEED,
            "revision": _revision(),
            "command": _safe_command(),
            "duration_seconds": time.perf_counter() - started,
            "machine": {
                "platform": platform.platform(),
                "cpu_count": os.cpu_count(),
                "python": platform.python_version(),
                "postgresql": postgresql,
            },
            "config": {
                "global_active_limit": GLOBAL_ACTIVE_LIMIT,
                "tenant_active_limit": TENANT_ACTIVE_LIMIT,
                "tenant_outstanding_limit": TENANT_OUTSTANDING_LIMIT,
                "worker_concurrency": WORKER_CONCURRENCY,
            },
            "acceptance_replay": replay,
            "lease_recovery": recovery,
            "scenarios": scenarios,
            "totals": {
                "accepted": accepted,
                "rejected": rejected,
                "retries": sum(
                    scenario["retries"] for scenario in scenarios
                ),
                "dead_letters": sum(
                    scenario["dead_letters"] for scenario in scenarios
                ),
                "stale_results_rejected": recovery[
                    "stale_results_rejected"
                ],
            },
            "invariants": invariants,
            "provider_execution_enabled": False,
        }
        if args.report is not None:
            _write_report(args.report, summary)
        return summary
    finally:
        if pool is not None:
            await pool.close()
        if supplied_schema is None and controller_connection is not None:
            await controller_connection.execute(
                f'DROP SCHEMA "{schema}" CASCADE'
            )
            await controller_connection.close()


async def _dispatch(args: argparse.Namespace) -> dict[str, Any]:
    schema = _schema()
    if args.child:
        if schema is None:
            raise ValueError("child process requires an evidence schema")
        if args.child == "accept":
            return await _accept_child(schema)
        return await _worker_child(schema, lease_ms=args.lease_ms)
    return await _run(args)


def main() -> None:
    args = _parser().parse_args()
    try:
        summary = asyncio.run(_dispatch(args))
    except Exception as exc:
        print(
            f"execution evidence failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc
    print(json.dumps(summary, sort_keys=True))
    if not summary.get("ok", True):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
