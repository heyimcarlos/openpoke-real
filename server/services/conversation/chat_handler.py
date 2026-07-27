from typing import Optional, Union

from fastapi import status
from fastapi.responses import JSONResponse, PlainTextResponse

from ...logging_config import logger
from ...models import ChatMessage, ChatRequest
from ..task_queue import Principal
from ..threads import PostgresThreadLedger
from ...utils import error_response


# Extract the most recent user message from the chat request payload
def _extract_latest_user_message(payload: ChatRequest) -> Optional[ChatMessage]:
    for message in reversed(payload.messages):
        if message.role.lower().strip() == "user" and message.content.strip():
            return message
    return None


# Process incoming chat requests by routing them to the interaction agent runtime
async def handle_chat_request(
    payload: ChatRequest,
    *,
    principal: Principal,
    thread_ledger: PostgresThreadLedger,
) -> Union[PlainTextResponse, JSONResponse]:
    """Durably accept one inbound chat message for later orchestration."""

    # Extract user message
    user_message = _extract_latest_user_message(payload)
    if user_message is None:
        return error_response("Missing user message", status_code=status.HTTP_400_BAD_REQUEST)

    user_content = user_message.content.strip()  # Already checked in _extract_latest_user_message

    logger.info("chat request", extra={"message_length": len(user_content)})

    await thread_ledger.append_message(
        principal,
        message_id=payload.turn_id,
        content=user_content,
    )

    return PlainTextResponse("", status_code=status.HTTP_202_ACCEPTED)
