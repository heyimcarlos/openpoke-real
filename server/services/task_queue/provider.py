"""Lazy application-owned PostgreSQL task service."""

from __future__ import annotations

import asyncio

import asyncpg

from ...config import get_settings
from .ledger import PostgresTaskLedger
from .service import TaskService


_pool: asyncpg.Pool | None = None
_service: TaskService | None = None
_lock = asyncio.Lock()


async def get_shared_task_service() -> TaskService:
    """Create one bounded database pool on first authenticated chat request."""

    global _pool, _service
    if _service is not None:
        return _service

    async with _lock:
        if _service is not None:
            return _service
        database_url = get_settings().database_url
        if not database_url:
            raise RuntimeError("OPENPOKE_DATABASE_URL is not configured")
        pool = await asyncpg.create_pool(
            database_url,
            min_size=1,
            max_size=10,
        )
        ledger = PostgresTaskLedger(pool)
        try:
            await ledger.migrate()
        except Exception:
            await pool.close()
            raise
        _pool = pool
        _service = TaskService(ledger)
        return _service


async def close_shared_task_service() -> None:
    global _pool, _service
    if _pool is not None:
        await _pool.close()
    _pool = None
    _service = None
