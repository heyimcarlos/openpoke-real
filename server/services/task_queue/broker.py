"""RabbitMQ adapters for minimal, versioned worker wake events."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, Message
from pamqp.commands import Basic
from pydantic import ValidationError

from .models import ExecutorKind
from .outbox import (
    OutboxFailureCode,
    OutboxPublishError,
    WakeEvent,
)
from .worker import TaskWorker


class BoundedTaskWorkerWakeHandler:
    """Bound broker callbacks to configured worker execution slots."""

    def __init__(
        self,
        workers: list[TaskWorker],
    ) -> None:
        if not workers:
            raise ValueError("at least one task worker is required")
        self._available: asyncio.Queue[TaskWorker] = asyncio.Queue()
        for worker in workers:
            self._available.put_nowait(worker)

    async def __call__(self, event: WakeEvent) -> None:
        await self.poll(event.executor_kind)

    async def poll(self, executor_kind: ExecutorKind | None) -> None:
        worker = await self._available.get()
        try:
            await worker.run_once(executor_kind=executor_kind)
        finally:
            self._available.put_nowait(worker)


class RabbitMQWakePublisher:
    """Publish persistent wakes through a confirm-enabled exchange."""

    def __init__(
        self,
        exchange: Any,
        *,
        confirmation_timeout_seconds: float = 10,
    ) -> None:
        if confirmation_timeout_seconds <= 0:
            raise ValueError("publisher confirmation timeout must be positive")
        self._exchange = exchange
        self._confirmation_timeout_seconds = confirmation_timeout_seconds

    async def publish(self, event: WakeEvent) -> None:
        message = Message(
            body=json.dumps(
                event.model_dump(mode="json"),
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8"),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=str(event.event_id),
            type=f"openpoke.task_wake.v{event.event_version}",
        )
        try:
            confirmation = await self._exchange.publish(
                message,
                routing_key=event.executor_kind.value,
                mandatory=True,
                timeout=self._confirmation_timeout_seconds,
            )
        except TimeoutError as exc:
            raise OutboxPublishError(OutboxFailureCode.TIMEOUT) from exc
        except Exception as exc:
            raise OutboxPublishError(OutboxFailureCode.UNAVAILABLE) from exc
        if not isinstance(confirmation, Basic.Ack):
            raise OutboxPublishError(OutboxFailureCode.REJECTED)


class RabbitMQWakeBroker:
    """Own RabbitMQ topology and acknowledge only completed wake handling."""

    EXCHANGE = "openpoke.task_wakes.v1"
    DEAD_LETTER_EXCHANGE = "openpoke.task_wakes.transport_dlx.v1"
    DEAD_LETTER_QUEUE = "openpoke.task_wakes.transport_dlq.v1"

    def __init__(self, connection, channel, exchange) -> None:
        self._connection = connection
        self._channel = channel
        self._exchange = exchange
        self.publisher = RabbitMQWakePublisher(exchange)

    @classmethod
    async def connect(
        cls,
        url: str,
        *,
        prefetch_count: int = 1,
    ) -> RabbitMQWakeBroker:
        if prefetch_count < 1:
            raise ValueError("RabbitMQ prefetch_count must be positive")
        connection = await aio_pika.connect_robust(url)
        channel = await connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )
        await channel.set_qos(prefetch_count=prefetch_count)
        exchange = await channel.declare_exchange(
            cls.EXCHANGE,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        dead_letter_exchange = await channel.declare_exchange(
            cls.DEAD_LETTER_EXCHANGE,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        dead_letter_queue = await channel.declare_queue(
            cls.DEAD_LETTER_QUEUE,
            durable=True,
        )
        broker = cls(connection, channel, exchange)
        for executor_kind in ("agent", "synthetic"):
            await dead_letter_queue.bind(
                dead_letter_exchange,
                routing_key=executor_kind,
            )
            queue = await channel.declare_queue(
                f"openpoke.task_wakes.{executor_kind}.v1",
                durable=True,
                arguments={
                    "x-dead-letter-exchange": cls.DEAD_LETTER_EXCHANGE,
                    "x-dead-letter-routing-key": executor_kind,
                },
            )
            await queue.bind(exchange, routing_key=executor_kind)
        return broker

    async def consume(self, executor_kind, handler) -> None:
        queue = await self._channel.get_queue(
            f"openpoke.task_wakes.{executor_kind.value}.v1",
            ensure=True,
        )

        async def on_message(message) -> None:
            await self.handle_delivery(
                message,
                executor_kind=executor_kind,
                handler=handler,
            )

        await queue.consume(on_message)

    async def handle_delivery(
        self,
        message,
        *,
        executor_kind: ExecutorKind,
        handler,
    ) -> None:
        """Reject poison wakes, requeue transient handling failures."""

        try:
            event = WakeEvent.model_validate_json(message.body)
        except (ValidationError, ValueError, TypeError):
            await message.reject(requeue=False)
            return
        if (
            event.event_version != 1
            or event.executor_kind is not executor_kind
        ):
            await message.reject(requeue=False)
            return
        try:
            await handler(event)
        except Exception:
            await message.reject(requeue=False)
            return
        await message.ack()

    async def close(self) -> None:
        await self._connection.close()
