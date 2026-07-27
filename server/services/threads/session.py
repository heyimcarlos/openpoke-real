"""Thin OpenAI Agents SDK Session adapter over authoritative Thread records."""

from __future__ import annotations

from agents import SessionSettings

from .ledger import PostgresThreadLedger
from .models import AgentRunLease


class PostgresThreadSession:
    """Expose one claimed, frozen Thread context through the SDK protocol."""

    def __init__(
        self,
        ledger: PostgresThreadLedger,
        lease: AgentRunLease,
        *,
        default_limit: int = 50,
    ) -> None:
        if default_limit < 1 or default_limit > 50:
            raise ValueError("default_limit must be between 1 and 50")
        self._ledger = ledger
        self._lease = lease
        self._default_limit = default_limit
        self.session_id = str(lease.thread_id)
        self.session_settings = SessionSettings(limit=default_limit)

    async def get_items(self, limit: int | None = None) -> list[dict]:
        requested = self._default_limit if limit is None else limit
        return await self._ledger.get_context(
            self._lease,
            limit=min(requested, self._default_limit),
        )

    async def add_items(self, items: list[dict]) -> None:
        await self._ledger.append_run_items(self._lease, items)

    async def pop_item(self) -> dict | None:
        return await self._ledger.pop_run_item(self._lease)

    async def clear_session(self) -> None:
        raise RuntimeError(
            "durable Thread history cannot be cleared from an Agent Run session"
        )
