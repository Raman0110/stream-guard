"""Kafka consumer service: validate, score, persist, then commit offsets."""

import logging

import numpy as np
from confluent_kafka import Consumer, Message, Producer
from sqlalchemy import Engine

from common.config import Settings
from common.kafka_utils import ensure_topics
from common.schema import SensorReading
from consumer import db
from consumer.processing import parse_message
from ml.features import RollingFeatures
from ml.model import Scorer

log = logging.getLogger("consumer")
BATCH_SIZE = 100


class ConsumerService:
    def __init__(self, cfg: Settings, engine: Engine, scorer: Scorer) -> None:
        self.cfg = cfg
        self.engine = engine
        self.scorer = scorer
        self.features = RollingFeatures()
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
        self.dlq_producer = Producer({"bootstrap.servers": cfg.kafka_bootstrap_servers})
        self.processed = 0
        self.duplicates = 0
        self.dead_lettered = 0
        self.anomalies = 0

    def stop(self) -> None:
        self.running = False

    def run(self) -> None:
        log.info("consumer started")
        while self.running:
            messages = self.consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            self.handle_batch(
                [message for message in messages if self._is_valid_kafka_message(message)]
            )
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
        invalid: list[tuple[str, str]] = []
        for message in messages:
            raw = message.value() or b""
            reading, error = parse_message(raw)
            if reading is not None:
                valid.append(reading)
                continue

            assert error is not None
            invalid.append((raw.decode("utf-8", errors="replace"), error))
            self.dlq_producer.produce(self.cfg.kafka_dlq_topic, raw, key=message.key())

        scores: list[float] = []
        flags: list[bool] = []
        if valid:
            feature_matrix = np.array(
                [
                    self.features.update(
                        reading.sensor_id,
                        reading.temperature,
                        reading.humidity,
                        reading.vibration,
                    )
                    for reading in valid
                ]
            )
            score_array, flag_array = self.scorer.score(feature_matrix)
            scores, flags = score_array.tolist(), flag_array.tolist()

        inserted = db.insert_scored_readings(self.engine, valid, scores, flags, self.scorer.version)
        db.insert_dlq(self.engine, invalid)
        self.dlq_producer.flush(10)
        self.consumer.commit(asynchronous=False)

        self.processed += inserted
        self.duplicates += len(valid) - inserted
        self.dead_lettered += len(invalid)
        self.anomalies += sum(flags)
        log.info("batch done", extra={"batch": len(messages), **self.counters()})

    def counters(self) -> dict[str, int]:
        return {
            "processed": self.processed,
            "duplicates": self.duplicates,
            "dead_lettered": self.dead_lettered,
            "anomalies": self.anomalies,
        }
