"""Kafka producer for accepted transaction events."""

import asyncio
import contextlib

from aiokafka import AIOKafkaProducer

from schemas import TransactionEvent

TOPIC = "transactions.raw"
_LINGER_MS = 5
_DEFAULT_ACK_TIMEOUT_S = 10.0


class PublishError(Exception):
    """Kafka did not acknowledge the event."""


class KafkaPublisher:
    """Publish events to ``transactions.raw`` and wait for the broker ack.

    The producer uses ``acks="all"`` and ``enable_idempotence=True``, so a
    retried produce request is not appended twice. Idempotent mode does not
    expire a batch on ``request_timeout_ms``, so :meth:`publish` applies its
    own ack timeout. A timeout still rolls the idempotency row back; a late
    ack after that is the gap recorded in ADR-006.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        linger_ms: int = _LINGER_MS,
        ack_timeout_s: float = _DEFAULT_ACK_TIMEOUT_S,
    ) -> None:
        self._ack_timeout_s = ack_timeout_s
        self._closed = False
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            linger_ms=linger_ms,
            request_timeout_ms=int(ack_timeout_s * 1000),
        )

    async def start(self) -> None:
        """Connect to the cluster and obtain a producer id."""
        await self._producer.start()

    async def ready(self) -> bool:
        """Return whether the broker answers a metadata refresh."""
        try:
            updated = await asyncio.wait_for(
                self._producer.client.force_metadata_update(),
                timeout=1,
            )
        except Exception:
            return False
        return bool(updated)

    async def aclose(self) -> None:
        """Stop the producer. A dead broker does not block shutdown."""
        if self._closed:
            return
        self._closed = True
        try:
            await asyncio.wait_for(self._producer.stop(), timeout=self._ack_timeout_s)
        except TimeoutError:
            await self._abandon()
        self._producer._closed = True

    async def _abandon(self) -> None:
        """Drop an unacked batch and close the client."""
        accumulator = getattr(self._producer, "_message_accumulator", None)
        fail_all = getattr(accumulator, "fail_all", None)
        if fail_all is not None:
            fail_all(PublishError("producer closed before the broker acknowledged the event"))
        sender = getattr(self._producer, "_sender", None)
        close_sender = getattr(sender, "close", None)
        try:
            if close_sender is not None:
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    await close_sender()
        finally:
            client = getattr(self._producer, "client", None)
            close_client = getattr(client, "close", None)
            if close_client is not None:
                with contextlib.suppress(Exception, asyncio.CancelledError):
                    await asyncio.wait_for(close_client(), timeout=self._ack_timeout_s)

    async def publish(self, event: TransactionEvent) -> None:
        """Send ``event`` and return when every in-sync replica has it."""
        try:
            await asyncio.wait_for(
                self._producer.send_and_wait(
                    TOPIC,
                    value=event.to_bytes(),
                    key=event.card_token.encode("utf-8"),
                ),
                timeout=self._ack_timeout_s,
            )
        except TimeoutError as exc:
            msg = f"timed out publishing event {event.event_id}"
            raise PublishError(msg) from exc
        except Exception as exc:
            msg = f"failed to publish event {event.event_id}"
            raise PublishError(msg) from exc
