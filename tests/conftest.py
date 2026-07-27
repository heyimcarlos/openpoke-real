from __future__ import annotations

import os
from collections.abc import AsyncIterator
from uuid import uuid4

import asyncpg
import pytest_asyncio

from server.services.task_queue import PostgresTaskLedger


DATABASE_URL = os.getenv(
    "OPENPOKE_TEST_DATABASE_URL",
    "postgresql://postgres@127.0.0.1:55432/openpoke_test",
)


@pytest_asyncio.fixture
async def database_schema() -> AsyncIterator[str]:
    schema = f"openpoke_test_{uuid4().hex}"
    connection = await asyncpg.connect(DATABASE_URL)
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    try:
        yield schema
    finally:
        await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await connection.close()


@pytest_asyncio.fixture
async def postgres_pool(database_schema: str) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=20,
        server_settings={"search_path": database_schema},
    )
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def ledger(postgres_pool: asyncpg.Pool) -> PostgresTaskLedger:
    task_ledger = PostgresTaskLedger(postgres_pool)
    await task_ledger.migrate()
    return task_ledger
