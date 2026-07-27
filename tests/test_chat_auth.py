from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import jwt
import pytest
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from server.app import register_exception_handlers
from server.routes import chat as chat_route
from server.services.task_queue import JwtPrincipalVerifier


SIGNING_KEY = "test-only-signing-key-with-at-least-32-bytes"
ISSUER = "https://auth.openpoke.test"
AUDIENCE = "openpoke-api"


def _token(**overrides: object) -> str:
    claims: dict[str, object] = {
        "sub": "user-7",
        "tenant_id": "tenant-a",
        "scope": "chat:send",
        "composio_user_id": "local-openpoke-user",
        "iss": ISSUER,
        "aud": AUDIENCE,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=5),
    }
    claims.update(overrides)
    return jwt.encode(claims, SIGNING_KEY, algorithm="HS256")


@pytest.fixture
def chat_app(monkeypatch: pytest.MonkeyPatch) -> tuple[FastAPI, list[object]]:
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
    task_service = object()
    app.dependency_overrides[chat_route.get_chat_task_service] = lambda: task_service
    captured: list[object] = []

    async def fake_handle(payload, *, principal, task_service):
        captured.extend([payload, principal, task_service])
        return PlainTextResponse("", status_code=202)

    monkeypatch.setattr(chat_route, "handle_chat_request", fake_handle)
    return app, captured


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("authorization", "expected_status"),
    [
        (None, 401),
        ("Basic opaque", 401),
        ("Bearer invalid", 401),
        (f"Bearer {_token(scope='tasks:create')}", 403),
    ],
)
async def test_chat_rejects_untrusted_or_unauthorized_bearer(
    chat_app: tuple[FastAPI, list[object]],
    authorization: str | None,
    expected_status: int,
) -> None:
    app, captured = chat_app
    headers = {"Authorization": authorization} if authorization else {}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/api/v1/chat/send",
            headers=headers,
            json={
                "turn_id": "turn-client-stable-7",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )

    assert response.status_code == expected_status
    assert captured == []
    if expected_status == 401:
        assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.asyncio
async def test_chat_derives_principal_from_bearer_and_forwards_stable_turn(
    chat_app: tuple[FastAPI, list[object]],
) -> None:
    app, captured = chat_app

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/api/v1/chat/send",
            headers={"Authorization": f"Bearer {_token()}"},
            json={
                "turn_id": "turn-client-stable-7",
                "tenant_id": "attacker-selected",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )

    assert response.status_code == 202
    payload, principal, task_service = captured
    assert payload.turn_id == "turn-client-stable-7"
    assert principal.actor_id == "user-7"
    assert principal.tenant_id == "tenant-a"
    assert principal.scopes == frozenset({"chat:send", "tasks:create"})
    assert principal.composio_user_id == "local-openpoke-user"
    assert task_service is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "claims",
    [
        {"tenant_id": "tenant-b"},
        {"sub": "user-8"},
    ],
)
async def test_chat_rejects_valid_token_for_other_principal(
    chat_app: tuple[FastAPI, list[object]],
    claims: dict[str, str],
) -> None:
    app, captured = chat_app

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/api/v1/chat/send",
            headers={"Authorization": f"Bearer {_token(**claims)}"},
            json={
                "turn_id": "turn-client-stable-7",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )

    assert response.status_code == 403
    assert response.json()["error"] == (
        "Token principal is not authorized for this chat"
    )
    assert captured == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tenant_id", "actor_id"),
    [
        (None, "user-7"),
        ("tenant-a", None),
    ],
)
async def test_chat_is_unavailable_without_complete_principal_binding(
    monkeypatch: pytest.MonkeyPatch,
    tenant_id: str | None,
    actor_id: str | None,
) -> None:
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
    monkeypatch.setattr(
        chat_route,
        "get_settings",
        lambda: type(
            "Settings",
            (),
            {
                "allowed_chat_tenant_id": tenant_id,
                "allowed_chat_actor_id": actor_id,
            },
        )(),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            "/api/v1/chat/history",
            headers={"Authorization": f"Bearer {_token()}"},
        )

    assert response.status_code == 503
    assert response.json()["error"] == (
        "Chat principal binding is not configured"
    )


@pytest.mark.asyncio
async def test_chat_history_requires_same_bearer_boundary(
    chat_app: tuple[FastAPI, list[object]],
) -> None:
    app, _ = chat_app
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        unauthenticated_get = await client.get("/api/v1/chat/history")
        unauthenticated_delete = await client.delete("/api/v1/chat/history")
        authenticated_get = await client.get(
            "/api/v1/chat/history",
            headers={"Authorization": f"Bearer {_token()}"},
        )

    assert unauthenticated_get.status_code == 401
    assert unauthenticated_delete.status_code == 401
    assert authenticated_get.status_code == 200
