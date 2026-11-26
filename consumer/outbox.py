"""Reliable Kafka publication for messages persisted in the database outbox."""

import logging
import time
from collections.abc import Callable

from confluent_kafka import Producer
from sqlalchemy import Engine

from consumer import db

log = logging.getLogger("consumer.outbox")


class OutboxPublisher:
    def __init__(
        self,
        engine: Engine,
        bootstrap_servers: str,
        *,
        producer: Producer | None = None,
        retry_interval_seconds: float = 1.0,
    ) -> None:
        self.engine = engine
        self.producer = producer or Producer({"bootstrap.servers": bootstrap_servers})
        self.retry_interval_seconds = retry_interval_seconds
        self.next_attempt_at = 0.0

    def publish_pending(self, *, force: bool = False) -> int:
        """Publish one outbox page and return the number confirmed by Kafka."""
        now = time.monotonic()
        if not force and now < self.next_attempt_at:
            return 0
        self.next_attempt_at = now + self.retry_interval_seconds

        events = db.load_pending_outbox(self.engine)
        if not events:
            return 0

        results: dict[int, str | None] = {}
        for event in events:
            try:
                headers = [("source_key", event.message_key)] if event.message_key else None
                self.producer.produce(
                    event.topic,
                    event.payload,
                    key=str(event.id).encode(),
                    headers=headers,
                    on_delivery=self._delivery_callback(event.id, results),
                )
                self.producer.poll(0)
            except Exception as exc:
                results[event.id] = str(exc)

        self.producer.flush(10)
        for event in events:
            results.setdefault(event.id, "Kafka delivery confirmation timed out")

        delivered = [event_id for event_id, error in results.items() if error is None]
        failed = {event_id: error for event_id, error in results.items() if error is not None}
        db.record_outbox_results(self.engine, delivered, failed)
        log.info(
            "outbox publish complete",
            extra={"delivered": len(delivered), "failed": len(failed)},
        )
        return len(delivered)

    @staticmethod
    def _delivery_callback(
        event_id: int,
        results: dict[int, str | None],
    ) -> Callable[[object, object], None]:
        def callback(error: object, message: object) -> None:
            results[event_id] = None if error is None else str(error)

        return callback
