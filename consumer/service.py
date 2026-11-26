"""Kafka consumer service: validate, score, persist, then commit offsets."""

import logging

import numpy as np
from confluent_kafka import Consumer, Message
from sqlalchemy import Engine

from common.config import Settings
from common.kafka_utils import ensure_topics
from common.schema import SensorReading
from consumer import db
from consumer.outbox import OutboxPublisher
from consumer.processing import parse_message
from ml.features import RollingFeatures
from ml.model import Scorer

log = logging.getLogger("consumer")
BATCH_SIZE = 100
# consume() returns after BATCH_SIZE messages OR this many seconds, whichever comes first.
# At low event rates the batch rarely fills, so this timeout is the delay every event can
# wait before being processed. Measured: 1.0 s gave ~550 ms median latency at 100 events/s.
POLL_TIMEOUT_SECONDS = 0.1


class ConsumerService:
    def __init__(
        self,
        cfg: Settings,
        engine: Engine,
        scorer: Scorer,
        *,
        outbox: OutboxPublisher | None = None,
    ) -> None:
        self.cfg = cfg
        self.engine = engine
        self.scorer = scorer
        self.features = RollingFeatures()
        history = db.load_recent_history(engine, self.features.window - 1)
        for sensor_id, temperature, humidity, vibration in history:
            self.features.update(sensor_id, temperature, humidity, vibration)
        self.running = True
        ensure_topics(cfg.kafka_bootstrap_servers, [cfg.kafka_topic, cfg.kafka_dlq_topic])
        self.consumer = Consumer(
            {
                "bootstrap.servers": cfg.kafka_bootstrap_servers,
                "group.id": "stream-consumer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.consumer.subscribe([cfg.kafka_topic])
        self.outbox = outbox or OutboxPublisher(engine, cfg.kafka_bootstrap_servers)
        self.processed = 0
        self.duplicates = 0
        self.dead_lettered = 0
        self.anomalies = 0
        log.info("rolling state restored", extra={"history_readings": len(history)})

    def stop(self) -> None:
        self.running = False

    def run(self) -> None:
        log.info("consumer started")
        try:
            while self.running:
                messages = self.consumer.consume(
                    num_messages=BATCH_SIZE,
                    timeout=POLL_TIMEOUT_SECONDS,
                )
                valid_messages = [
                    message for message in messages if self._is_valid_kafka_message(message)
                ]
                if valid_messages:
                    self.handle_batch(valid_messages)
                else:
                    self._publish_outbox()
        finally:
            self._publish_outbox(force=True)
            self.consumer.close()
            log.info("consumer stopped", extra=self.counters())

    @staticmethod
    def _is_valid_kafka_message(message: Message) -> bool:
        error = message.error()
        if error is not None:
            log.error("kafka error", extra={"error": str(error)})
            return False
        return True

    def handle_batch(self, messages: list[Message]) -> None:
        if not messages:
            return

        valid: list[SensorReading] = []
        invalid: list[db.DeadLetter] = []
        for message in messages:
            raw = message.value() or b""
            reading, error = parse_message(raw)
            if reading is not None:
                valid.append(reading)
                continue

            assert error is not None
            invalid.append(db.DeadLetter(raw, error, message.key()))

        existing = db.load_existing_reading_keys(self.engine, valid)
        seen = set(existing)
        fresh: list[SensorReading] = []
        for reading in valid:
            key = db.reading_key(reading)
            if key in seen:
                continue
            seen.add(key)
            fresh.append(reading)

        scores: list[float] = []
        flags: list[bool] = []
        candidate_features = self.features.clone()
        if fresh:
            feature_matrix = np.array(
                [
                    candidate_features.update(
                        reading.sensor_id,
                        reading.temperature,
                        reading.humidity,
                        reading.vibration,
                    )
                    for reading in fresh
                ]
            )
            score_array, flag_array = self.scorer.score(feature_matrix)
            scores, flags = score_array.tolist(), flag_array.tolist()

        inserted = 0
        if fresh or invalid:
            inserted = db.persist_batch(
                self.engine,
                fresh,
                scores,
                flags,
                self.scorer.version,
                invalid,
                self.cfg.kafka_dlq_topic,
            )

        # The candidate window becomes live only after the database transaction commits. A failed
        # write leaves self.features untouched, so redelivery computes the same feature vectors.
        self.features = candidate_features
        self.consumer.commit(asynchronous=False)

        self.processed += inserted
        self.duplicates += len(valid) - len(fresh)
        self.dead_lettered += len(invalid)
        self.anomalies += sum(flags)
        log.info("batch done", extra={"batch": len(messages), **self.counters()})
        self._publish_outbox()

    def _publish_outbox(self, *, force: bool = False) -> None:
        try:
            self.outbox.publish_pending(force=force)
        except Exception:
            # The input is already durable in PostgreSQL. Keep processing and retry the outbox on
            # the next loop instead of making Kafka DLQ availability block the main pipeline.
            log.exception("outbox publish failed")

    def counters(self) -> dict[str, int]:
        return {
            "processed": self.processed,
            "duplicates": self.duplicates,
            "dead_lettered": self.dead_lettered,
            "anomalies": self.anomalies,
        }
