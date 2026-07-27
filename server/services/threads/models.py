"""Durable conversation and disposable Agent Run records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from uuid import UUID


MAX_MESSAGE_BYTES = 16_384


class AgentRunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True)
class ThreadMessage:
    message_id: UUID
    thread_id: UUID
    ingress_sequence: int | None
    context_sequence: int | None
    role: str
    content: str
    created_at: datetime


@dataclass(frozen=True)
class AgentRunRecord:
    run_id: UUID
    thread_id: UUID
    status: AgentRunStatus
    ingress_cutoff: int
    context_cutoff: int
    attempt_count: int
    delegation_count: int


@dataclass(frozen=True)
class AgentRunLease:
    run_id: UUID
    thread_id: UUID
    tenant_id: str
    actor_id: str
    composio_user_id: str | None
    ingress_cutoff: int
    context_cutoff: int
    attempt_count: int
    lease_generation: int
    worker_id: str
    expires_at: datetime
    input_source_kind: str
