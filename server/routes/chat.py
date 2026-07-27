from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.responses import JSONResponse

from ..config import get_settings
from ..models import ChatHistoryClearResponse, ChatHistoryResponse, ChatRequest
from ..services import handle_chat_request
from ..services.task_queue import (
    InvalidToken,
    JwtPrincipalVerifier,
    Principal,
)
from ..services.task_queue.provider import get_shared_thread_ledger
from ..services.threads import PostgresThreadLedger

router = APIRouter(prefix="/chat", tags=["chat"])


def get_chat_jwt_verifier() -> JwtPrincipalVerifier:
    settings = get_settings()
    try:
        return JwtPrincipalVerifier(
            signing_key=settings.jwt_signing_key or "",
            issuer=settings.jwt_issuer or "",
            audience=settings.jwt_audience or "",
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Chat authentication is not configured",
        ) from None


def get_allowed_chat_principal() -> tuple[str, str]:
    settings = get_settings()
    tenant_id = (settings.allowed_chat_tenant_id or "").strip()
    actor_id = (settings.allowed_chat_actor_id or "").strip()
    if not tenant_id or not actor_id:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Chat principal binding is not configured",
        )
    return tenant_id, actor_id


async def get_chat_thread_ledger() -> PostgresThreadLedger:
    try:
        return await get_shared_thread_ledger()
    except RuntimeError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Thread service is not configured",
        ) from None


def require_chat_principal(
    authorization: Annotated[str | None, Header()] = None,
    verifier: JwtPrincipalVerifier = Depends(get_chat_jwt_verifier),
    allowed_principal: tuple[str, str] = Depends(get_allowed_chat_principal),
) -> Principal:
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        principal = verifier.verify(token)
    except InvalidToken:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from None
    if "chat:send" not in principal.scopes:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Missing required scope: chat:send",
        )
    if (principal.tenant_id, principal.actor_id) != allowed_principal:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Token principal is not authorized for this chat",
        )
    return principal


@router.post("/send", response_class=JSONResponse, summary="Submit a chat message and receive a completion")
# Handle incoming chat messages and route them to the interaction agent
async def chat_send(
    payload: ChatRequest,
    principal: Principal = Depends(require_chat_principal),
    thread_ledger: PostgresThreadLedger = Depends(get_chat_thread_ledger),
) -> JSONResponse:
    task_principal = Principal(
        actor_id=principal.actor_id,
        tenant_id=principal.tenant_id,
        scopes=principal.scopes | {"tasks:create"},
        composio_user_id=principal.composio_user_id,
    )
    return await handle_chat_request(
        payload,
        principal=task_principal,
        thread_ledger=thread_ledger,
    )


@router.get(
    "/history",
    response_model=ChatHistoryResponse,
)
async def chat_history(
    principal: Principal = Depends(require_chat_principal),
    thread_ledger: PostgresThreadLedger = Depends(get_chat_thread_ledger),
) -> ChatHistoryResponse:
    messages = await thread_ledger.list_messages(principal)
    return ChatHistoryResponse(
        messages=[
            {
                "role": message.role if message.role != "agent" else "assistant",
                "content": message.content,
                "timestamp": message.created_at.isoformat(),
            }
            for message in messages
        ]
    )


@router.delete(
    "/history",
    response_model=ChatHistoryClearResponse,
)
async def clear_history(
    principal: Principal = Depends(require_chat_principal),
    thread_ledger: PostgresThreadLedger = Depends(get_chat_thread_ledger),
) -> ChatHistoryClearResponse:
    await thread_ledger.clear(principal)
    return ChatHistoryClearResponse()


__all__ = ["router"]
