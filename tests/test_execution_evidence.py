from __future__ import annotations

import json
import os
import subprocess
import sys

import asyncpg
import pytest

from conftest import DATABASE_URL


@pytest.mark.asyncio
async def test_evidence_cli_proves_overload_and_process_recovery(
    database_schema: str,
    postgres_pool: asyncpg.Pool,
) -> None:
    env = os.environ.copy()
    env["OPENPOKE_EVIDENCE_DATABASE_URL"] = DATABASE_URL
    env["OPENPOKE_EVIDENCE_SCHEMA"] = database_schema

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.run_execution_evidence",
            "--task-duration-ms",
            "5",
            "--lease-ms",
            "80",
            "--recovery-duration-ms",
            "300",
        ],
        cwd=os.getcwd(),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    summary = json.loads(completed.stdout)
    assert summary["ok"] is True
    assert summary["seed"] == 20260727
    assert summary["config"] == {
        "global_active_limit": 8,
        "tenant_active_limit": 2,
        "tenant_outstanding_limit": 50,
        "worker_concurrency": 16,
    }

    replay = summary["acceptance_replay"]
    assert replay["accepted"] == 3
    assert replay["same_task_ids"] is True
    assert replay["fresh_process_completed"] is True

    recovery = summary["lease_recovery"]
    assert recovery["attempt_count"] == 2
    assert recovery["replacement_attempt_count"] == 2
    assert recovery["killed_by_sigkill"] is True
    assert recovery["fresh_process_completed"] is True
    assert recovery["stale_results_rejected"] == 1
    assert recovery["winner_result_preserved"] is True

    scenarios = {
        scenario["clients"]: scenario for scenario in summary["scenarios"]
    }
    assert set(scenarios) == {1, 10, 50, 100}
    assert scenarios[100]["accepted"] == 90
    assert scenarios[100]["rejected"] == 10
    assert scenarios[100]["tenants"]["noisy"]["accepted"] == 50
    assert scenarios[100]["tenants"]["noisy"]["rejected"] == 10
    assert scenarios[100]["quiet_tenant_progressed_while_noisy_backlogged"]
    for clients in (10, 50, 100):
        assert scenarios[clients]["quiet_started_before_noisy_finished"]
        assert scenarios[clients]["tenants"]["quiet"]["accepted"] == 1
        assert scenarios[clients]["quiet_release_observation"][
            "noisy_active"
        ] == 2
        assert scenarios[clients]["quiet_release_observation"][
            "noisy_queued"
        ] > 0

    for scenario in scenarios.values():
        assert scenario["all_accepted_terminal"]
        assert scenario["peak_active_attempts"] <= 8
        assert max(scenario["peak_active_attempts_by_tenant"].values()) <= 2

    assert summary["totals"]["accepted"] == 155
    assert summary["totals"]["rejected"] == 10
    assert summary["totals"]["stale_results_rejected"] == 1
    assert summary["invariants"]["all_passed"] is True
    assert summary["provider_execution_enabled"] is False

    rows = await postgres_pool.fetch(
        """
        SELECT status, executor_kind, attempt_count, started_at, finished_at
        FROM execution_tasks
        """
    )
    assert len(rows) == 155
    assert {row["executor_kind"] for row in rows} == {"synthetic"}
    assert {row["status"] for row in rows} <= {
        "completed",
        "dead_lettered",
    }
    assert all(row["started_at"] is not None for row in rows)
    assert all(row["finished_at"] is not None for row in rows)
    assert max(row["attempt_count"] for row in rows) <= 3
