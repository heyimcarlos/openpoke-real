"""Database connection budgets for independently deployed runtime roles."""

from __future__ import annotations

from enum import Enum

import asyncpg


class DatabaseRole(str, Enum):
    API = "api"
    ORCHESTRATOR = "orchestrator"
    WORKER = "worker"
    RELAY = "relay"
    MIGRATOR = "migrator"


_MAX_POOL_SIZE = {
    DatabaseRole.API: 5,
    DatabaseRole.ORCHESTRATOR: 4,
    DatabaseRole.WORKER: 4,
    DatabaseRole.RELAY: 2,
    DatabaseRole.MIGRATOR: 1,
}


async def create_role_pool(
    database_url: str,
    role: DatabaseRole,
) -> asyncpg.Pool:
    """Create the role's bounded pool without changing database schema."""

    return await asyncpg.create_pool(
        database_url,
        min_size=1,
        max_size=_MAX_POOL_SIZE[role],
    )


__all__ = ["DatabaseRole", "create_role_pool"]
