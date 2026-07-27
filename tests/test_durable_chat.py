from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg
import httpx
import jwt
import pytest
from fastapi import FastAPI

from server.agents.interaction_agent.runtime import InteractionResult
from server.app import register_exception_handlers
from server.routes import chat as chat_route
from server.services.task_queue import (
    JwtPrincipalVerifier,
    PostgresTaskLedger,
    TaskService,
)
from server.services.threads import PostgresThreadLedger
from server.services.threads.worker import (
    AgentRunOutcomeStatus,
    AgentRunWorker,
)


SIGNING_KEY = "test-only-signing-key-with-at-least-32-bytes"
ISSUER = "https://auth.openpoke.test"
AUDIENCE = "openpoke-api"


def _token() -> str:
    return jwt.encode(
        {
            "sub": "user-7",
            "tenant_id": "tenant-a",
            "composio_user_id": "composio-user-7",
            "scope": "chat:send",
            "iss": ISSUER,
            "aud": AUDIENCE,
            "exp": datetime.now(timezone.utc) + timedelta(minutes=5),
        },
        SIGNING_KEY,
        algorithm="HS256",
    )


@pytest.mark.asyncio
async def test_web_message_is_durable_before_disposable_orchestrator_runs(
    postgres_pool: asyncpg.Pool,
) -> None:
    task_ledger = PostgresTaskLedger(postgres_pool)
    await task_ledger.migrate()
    thread_ledger = PostgresThreadLedger(postgres_pool)
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(chat_route.router, prefix="/api/v1")
    app.dependency_overrides[chat_route.get_chat_jwt_verifier] = lambda: (
        JwtPrincipalVerifier(
            signing_key=SIGNING_KEY,
            issuer=ISSUER,
            audience=AUDIENCE,
        )
    )
    app.dependency_overrides[chat_route.get_allowed_chat_principal] = lambda: (
        "tenant-a",
        "user-7",
    )
    app.dependency_overrides[chat_route.get_chat_thread_ledger] = lambda: (
        thread_ledger
    )
    authorization = {"Authorization": f"Bearer {_token()}"}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        accepted = await client.post(
            "/api/v1/chat/send",
            headers=authorization,
            json={
                "turn_id": "browser-turn-1",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        before_worker = await client.get(
            "/api/v1/chat/history",
            headers=authorization,
        )

        captured: dict[str, object] = {}

        class FakeRuntime:
            def __init__(self, **kwargs: object) -> None:
                captured.update(kwargs)

            async def execute(self, user_message: str) -> InteractionResult:
                captured["input"] = user_message
                return InteractionResult(success=True, response="hello back")

            async def handle_agent_message(
                self,
                agent_message: str,
            ) -> InteractionResult:
                raise AssertionError(agent_message)

        outcome = await AgentRunWorker(
            thread_ledger,
            TaskService(task_ledger),
            worker_id="orchestrator-1",
            runtime_factory=FakeRuntime,
        ).run_once()
        after_worker = await client.get(
            "/api/v1/chat/history",
            headers=authorization,
        )
        cleared = await client.delete(
            "/api/v1/chat/history",
            headers=authorization,
        )
        after_clear = await client.get(
            "/api/v1/chat/history",
            headers=authorization,
        )

    assert accepted.status_code == 202
    assert before_worker.json()["messages"][0]["content"] == "hello"
    assert outcome.status is AgentRunOutcomeStatus.COMPLETED
    assert captured["input"] == "hello"
    assert captured["persist_locally"] is False
    tool_context = captured["tool_context"]
    assert tool_context.principal.tenant_id == "tenant-a"
    assert tool_context.principal.composio_user_id == "composio-user-7"
    assert tool_context.persist_locally is False
    assert [
        (message["role"], message["content"])
        for message in after_worker.json()["messages"]
    ] == [
        ("user", "hello"),
        ("assistant", "hello back"),
    ]
    assert cleared.status_code == 200
    assert after_clear.json() == {"messages": []}


@pytest.mark.asyncio
async def test_orchestrator_runtime_failure_has_three_attempt_limit(
    postgres_pool: asyncpg.Pool,
) -> None:
    task_ledger = PostgresTaskLedger(postgres_pool)
    await task_ledger.migrate()
    thread_ledger = PostgresThreadLedger(postgres_pool)
    principal = chat_route.Principal(
        actor_id="user-7",
        tenant_id="tenant-a",
        scopes=frozenset({"chat:send", "tasks:create"}),
    )
    await thread_ledger.append_message(
        principal,
        message_id="turn-fails",
        content="fail",
    )

    class FailingRuntime:
        def __init__(self, **_kwargs: object) -> None:
            pass

        async def execute(self, _message: str) -> InteractionResult:
            return InteractionResult(
                success=False,
                response="",
                error="provider unavailable",
            )

        async def handle_agent_message(self, _message: str) -> InteractionResult:
            raise AssertionError

    worker = AgentRunWorker(
        thread_ledger,
        TaskService(task_ledger),
        worker_id="orchestrator-1",
        runtime_factory=FailingRuntime,
    )

    outcomes = [await worker.run_once() for _ in range(4)]

    assert [outcome.status for outcome in outcomes] == [
        AgentRunOutcomeStatus.RETRIED,
        AgentRunOutcomeStatus.RETRIED,
        AgentRunOutcomeStatus.FAILED,
        AgentRunOutcomeStatus.IDLE,
    ]
