"""Dedicated database migration command."""

from __future__ import annotations

import argparse
import asyncio

from .config import get_settings
from .database import DatabaseRole, create_role_pool
from .services.task_queue import PostgresTaskLedger


async def _run() -> None:
    database_url = get_settings().database_url
    if not database_url:
        raise RuntimeError("OPENPOKE_DATABASE_URL is not configured")

    pool = await create_role_pool(database_url, DatabaseRole.MIGRATOR)
    try:
        await PostgresTaskLedger(pool).migrate()
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply OpenPoke database migrations"
    )
    parser.parse_args()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
