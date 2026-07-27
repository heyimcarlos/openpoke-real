from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from server.database import DatabaseRole, create_role_pool


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("role", "expected_max_size"),
    [
        (DatabaseRole.API, 5),
        (DatabaseRole.ORCHESTRATOR, 4),
        (DatabaseRole.WORKER, 4),
        (DatabaseRole.MIGRATOR, 1),
    ],
)
async def test_runtime_role_uses_its_connection_budget(
    monkeypatch: pytest.MonkeyPatch,
    role: DatabaseRole,
    expected_max_size: int,
) -> None:
    captured: dict[str, object] = {}
    sentinel = object()

    async def fake_create_pool(database_url: str, **kwargs: object) -> object:
        captured.update(database_url=database_url, **kwargs)
        return sentinel

    monkeypatch.setattr("server.database.asyncpg.create_pool", fake_create_pool)

    pool = await create_role_pool("postgresql://database/openpoke", role)

    assert pool is sentinel
    assert captured == {
        "database_url": "postgresql://database/openpoke",
        "min_size": 1,
        "max_size": expected_max_size,
    }


@pytest.mark.parametrize(
    ("module", "expected_help"),
    [
        ("server.server", "OpenPoke FastAPI server"),
        ("server.worker", "--no-local-result-projection"),
        ("server.orchestrator_worker", "OpenPoke orchestrator worker"),
        ("server.migrate", "Apply OpenPoke database migrations"),
    ],
)
def test_container_role_has_a_cli(module: str, expected_help: str) -> None:
    repository_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "-m", module, "--help"],
        cwd=repository_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert expected_help in result.stdout


def test_worker_rejects_capacity_above_two_slots() -> None:
    repository_root = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "server.worker",
            "--once",
            "--concurrency",
            "3",
        ],
        cwd=repository_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "--concurrency must be 1 or 2" in result.stderr
