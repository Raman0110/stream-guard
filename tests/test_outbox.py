from unittest.mock import Mock, patch

from consumer import db
from consumer.outbox import OutboxPublisher


class DeliveringProducer:
    def __init__(self, error: object | None = None) -> None:
        self.error = error
        self.messages: list[dict[str, object]] = []

    def produce(self, topic: str, payload: bytes, **kwargs: object) -> None:
        self.messages.append({"topic": topic, "payload": payload, **kwargs})
        callback = kwargs["on_delivery"]
        assert callable(callback)
        callback(self.error, object())

    def poll(self, timeout: float) -> None:
        return None

    def flush(self, timeout: float) -> int:
        return 0


def event(event_id: int = 7) -> db.PendingOutboxEvent:
    return db.PendingOutboxEvent(
        id=event_id,
        topic="sensor-readings-dlq",
        message_key=b"sensor-1",
        payload=b"bad payload",
    )


def test_confirmed_delivery_marks_outbox_published() -> None:
    producer = DeliveringProducer()
    publisher = OutboxPublisher(Mock(), "kafka:9092", producer=producer)  # type: ignore[arg-type]

    with (
        patch.object(db, "load_pending_outbox", return_value=[event()]),
        patch.object(db, "record_outbox_results") as record,
    ):
        delivered = publisher.publish_pending(force=True)

    assert delivered == 1
    assert producer.messages[0]["key"] == b"7"
    assert producer.messages[0]["headers"] == [("source_key", b"sensor-1")]
    record.assert_called_once_with(publisher.engine, [7], {})


def test_failed_delivery_remains_pending_with_error() -> None:
    producer = DeliveringProducer(error="broker unavailable")
    publisher = OutboxPublisher(Mock(), "kafka:9092", producer=producer)  # type: ignore[arg-type]

    with (
        patch.object(db, "load_pending_outbox", return_value=[event()]),
        patch.object(db, "record_outbox_results") as record,
    ):
        delivered = publisher.publish_pending(force=True)

    assert delivered == 0
    record.assert_called_once_with(publisher.engine, [], {7: "broker unavailable"})
