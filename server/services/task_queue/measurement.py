"""Deterministic dispatch traffic comparison for capacity planning."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ClaimTrafficComparison:
    polling_only_claims: int
    broker_assisted_claims: int
    claim_reduction_percent: float


def compare_idle_claim_traffic(
    *,
    worker_slots: int,
    worker_processes: int,
    observation_seconds: float,
    poll_interval_seconds: float,
    fallback_poll_seconds: float,
    accepted_tasks: int,
) -> ClaimTrafficComparison:
    """Compare DB claim calls under an idle polling loop and bounded wakes."""

    values = (
        worker_slots,
        worker_processes,
        observation_seconds,
        poll_interval_seconds,
        fallback_poll_seconds,
    )
    if any(value <= 0 for value in values) or accepted_tasks < 0:
        raise ValueError("measurement inputs must be positive")
    polling = int(
        worker_slots * observation_seconds / poll_interval_seconds
    )
    broker = int(
        worker_processes * observation_seconds / fallback_poll_seconds
    ) + accepted_tasks
    reduction = round((1 - broker / polling) * 100, 2)
    return ClaimTrafficComparison(
        polling_only_claims=polling,
        broker_assisted_claims=broker,
        claim_reduction_percent=reduction,
    )
