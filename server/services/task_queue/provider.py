"""Lazy application-owned PostgreSQL task service."""

from __future__ import annotations

import asyncio

import asyncpg

from server.config import get_settings
from server.database import DatabaseRole, create_role_pool
from .ledger import PostgresTaskLedger
from .service import TaskService
from ..threads import PostgresThreadLedger


_pool: asyncpg.Pool | None = None
_service: TaskService | None = None
_thread_ledger: PostgresThreadLedger | None = None
_lock = asyncio.Lock()


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool
    async with _lock:
        if _pool is not None:
            return _pool
        database_url = get_settings().database_url
        if not database_url:
            raise RuntimeError("OPENPOKE_DATABASE_URL is not configured")
        _pool = await create_role_pool(database_url, DatabaseRole.API)
        return _pool


async def get_shared_task_service() -> TaskService:
    """Create one application-owned task service."""

    global _service
    if _service is None:
        _service = TaskService(PostgresTaskLedger(await _get_pool()))
    return _service


async def get_shared_thread_ledger() -> PostgresThreadLedger:
    """Create one application-owned durable Thread ledger."""

    global _thread_ledger
    if _thread_ledger is None:
        _thread_ledger = PostgresThreadLedger(await _get_pool())
    return _thread_ledger


async def close_shared_task_service() -> None:
    global _pool, _service, _thread_ledger
    if _pool is not None:
        await _pool.close()
    _pool = None
    _service = None
    _thread_ledger = None
