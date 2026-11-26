"""End-to-end: Redpanda -> consumer -> Postgres, using throwaway containers."""

import json
import threading
import time
from datetime import UTC, datetime

import pytest
from confluent_kafka import Producer
from sqlalchemy import func, select
from testcontainers.kafka import RedpandaContainer
from testcontainers.postgres import PostgresContainer

from common import tables
from common.config import Settings
from consumer.service import ConsumerService
from ml import train
from ml.model import Scorer, train_model

pytestmark = pytest.mark.integration
REDPANDA_IMAGE = "redpandadata/redpanda:v24.2.7"  # same as docker-compose.yml


def reading(sensor: str, second: int) -> dict:
    return {
        "sensor_id": sensor,
        "timestamp": datetime(2024, 1, 1, 0, 0, second, tzinfo=UTC).isoformat(),
        "temperature": 25.0,
        "humidity": 50.0,
        "vibration": 0.5,
    }


def wait_for(condition, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(0.5)
    return False


def test_consumer_stores_valid_dedups_and_dead_letters():
    with (
        PostgresContainer("postgres:16-alpine") as pg,
        RedpandaContainer(REDPANDA_IMAGE) as rp,
    ):
        bootstrap = rp.get_bootstrap_server()
        cfg = Settings(
            kafka_bootstrap_servers=bootstrap,
            kafka_topic="sensor-readings",
            kafka_dlq_topic="sensor-readings-dlq",
            postgres_user="", postgres_password="", postgres_db="",
            postgres_host="", postgres_port=0, num_sensors=1, events_per_second=1,
            random_seed=1, anomaly_rate=0, drift_per_reading=0, invalid_rate=0,
        )  # fmt: skip
        engine = tables.connect(pg.get_connection_url())
        tables.metadata.create_all(engine)
        x, _ = train.make_dataset(2000, seed=1, anomaly_rate=0.0)  # tiny model, trained in-test
        service = ConsumerService(cfg, engine, Scorer(train_model(x, seed=1)))
        thread = threading.Thread(target=service.run)
        thread.start()

        producer = Producer({"bootstrap.servers": bootstrap})
        payloads = [json.dumps(reading("s1", i)).encode() for i in range(3)]
        payloads.append(json.dumps(reading("s1", 0)).encode())  # duplicate (redelivery)
        payloads.append(b"not json")  # invalid -> DLQ
        for p in payloads:
            producer.produce("sensor-readings", p)
        producer.flush()

        def counts():
            with engine.connect() as c:
                r = c.execute(select(func.count()).select_from(tables.readings)).scalar_one()
                d = c.execute(select(func.count()).select_from(tables.dlq_events)).scalar_one()
                p = c.execute(select(func.count()).select_from(tables.predictions)).scalar_one()
                o = c.execute(
                    select(func.count())
                    .select_from(tables.outbox_events)
                    .where(tables.outbox_events.c.published_at.is_not(None))
                ).scalar_one()
            return r, d, p, o

        try:
            assert wait_for(lambda: counts() == (3, 1, 3, 1))
        finally:
            service.stop()
            thread.join(timeout=15)
        assert counts() == (
            3,
            1,
            3,
            1,
        )  # duplicate skipped, one prediction per reading, invalid event published from outbox
