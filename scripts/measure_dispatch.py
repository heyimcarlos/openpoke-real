"""Print deterministic DB claim traffic for polling and broker-assisted modes."""

from __future__ import annotations

import json

from server.services.task_queue.measurement import compare_idle_claim_traffic


def main() -> None:
    result = compare_idle_claim_traffic(
        worker_slots=8,
        worker_processes=4,
        observation_seconds=60,
        poll_interval_seconds=0.5,
        fallback_poll_seconds=30,
        accepted_tasks=4,
    )
    print(
        json.dumps(
            {
                "scenario": "8 idle slots, 60 seconds, 4 accepted tasks",
                "polling_only_claims": result.polling_only_claims,
                "broker_assisted_claims": result.broker_assisted_claims,
                "claim_reduction_percent": result.claim_reduction_percent,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
