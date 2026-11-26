import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pytest

from common.config import Settings
from consumer import db
from consumer.service import ConsumerService
from ml.features import RollingFeatures


class FakeMessage:
    def __init__(self, payload: bytes, key: bytes = b"s1") -> None:
        self.payload = payload
        self.message_key = key

    def value(self) -> bytes:
        return self.payload

    def key(self) -> bytes:
        return self.message_key


def payload(second: int = 0) -> bytes:
    return json.dumps(
        {
            "sensor_id": "s1",
            "timestamp": datetime(2026, 1, 1, 0, 0, second, tzinfo=UTC).isoformat(),
            "temperature": 25.0 + second,
            "humidity": 50.0,
            "vibration": 0.5,
        }
    ).encode()


def bare_service() -> ConsumerService:
    service = ConsumerService.__new__(ConsumerService)
    service.cfg = SimpleNamespace(kafka_dlq_topic="sensor-readings-dlq")
    service.engine = Mock()
    service.scorer = Mock()
    service.scorer.version = "test-model"
    service.scorer.score.return_value = (np.array([0.5]), np.array([False]))
    service.features = RollingFeatures()
    service.consumer = Mock()
    service.outbox = Mock()
    service.processed = 0
    service.duplicates = 0
    service.dead_lettered = 0
    service.anomalies = 0
    return service


def settings() -> Settings:
    return Settings(
        kafka_bootstrap_servers="kafka:9092",
        kafka_topic="sensor-readings",
        kafka_dlq_topic="sensor-readings-dlq",
        postgres_user="streamguard",
        postgres_password="secret",
        postgres_db="streamguard",
        postgres_host="postgres",
        postgres_port=5432,
        num_sensors=1,
        events_per_second=1,
        random_seed=1,
        anomaly_rate=0,
        drift_per_reading=0,
        invalid_rate=0,
    )


def test_database_failure_does_not_advance_rolling_state() -> None:
    service = bare_service()
    service.features.update("s1", 24.0, 49.0, 0.4)
    original = service.features.snapshot()

    with (
        patch.object(db, "load_existing_reading_keys", return_value=set()),
        patch.object(db, "persist_batch", side_effect=RuntimeError("database unavailable")),
        pytest.raises(RuntimeError, match="database unavailable"),
    ):
        service.handle_batch([FakeMessage(payload(1))])

    assert service.features.snapshot() == original
    service.consumer.commit.assert_not_called()
    service.outbox.publish_pending.assert_not_called()


def test_stored_duplicate_is_not_scored_or_added_to_state() -> None:
    service = bare_service()
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)

    with (
        patch.object(db, "load_existing_reading_keys", return_value={("s1", timestamp)}),
        patch.object(db, "persist_batch") as persist,
    ):
        service.handle_batch([FakeMessage(payload())])

    assert service.features.snapshot() == {}
    assert service.duplicates == 1
    service.scorer.score.assert_not_called()
    persist.assert_not_called()
    service.consumer.commit.assert_called_once_with(asynchronous=False)


def test_duplicate_inside_batch_is_scored_only_once() -> None:
    service = bare_service()

    with (
        patch.object(db, "load_existing_reading_keys", return_value=set()),
        patch.object(db, "persist_batch", return_value=1) as persist,
    ):
        service.handle_batch([FakeMessage(payload()), FakeMessage(payload())])

    feature_matrix = service.scorer.score.call_args.args[0]
    assert feature_matrix.shape == (1, 9)
    assert len(persist.call_args.args[1]) == 1
    assert service.duplicates == 1
    assert len(service.features.snapshot()["s1"]) == 1


def test_success_publishes_candidate_state_after_database_commit() -> None:
    service = bare_service()

    with (
        patch.object(db, "load_existing_reading_keys", return_value=set()),
        patch.object(db, "persist_batch", return_value=1),
    ):
        service.handle_batch([FakeMessage(payload())])

    assert len(service.features.snapshot()["s1"]) == 1
    assert service.processed == 1
    service.consumer.commit.assert_called_once_with(asynchronous=False)
    service.outbox.publish_pending.assert_called_once_with(force=False)


def test_constructor_restores_recent_history() -> None:
    history = [
        ("s1", 21.0, 41.0, 0.1),
        ("s1", 22.0, 42.0, 0.2),
        ("s1", 23.0, 43.0, 0.3),
        ("s1", 24.0, 44.0, 0.4),
    ]
    kafka_consumer = Mock()

    with (
        patch.object(db, "load_recent_history", return_value=history),
        patch("consumer.service.ensure_topics"),
        patch("consumer.service.Consumer", return_value=kafka_consumer),
    ):
        service = ConsumerService(settings(), Mock(), Mock(), outbox=Mock())

    assert service.features.snapshot()["s1"] == tuple(
        (temperature, humidity, vibration) for _, temperature, humidity, vibration in history
    )
    kafka_consumer.subscribe.assert_called_once_with(["sensor-readings"])
