"""Long-running sensor production and Kafka publication service."""

import logging
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from confluent_kafka import Producer

from common.config import Settings
from common.kafka_utils import ensure_topics
from producer.payloads import corrupt_message, encode_message
from producer.simulator import SensorSimulator

log = logging.getLogger("producer")


@dataclass(frozen=True)
class ProducerOptions:
    rate: float
    sensors: int
    seed: int
    anomaly_rate: float
    drift_per_reading: float
    invalid_rate: float
    max_events: int


class ProducerService:
    def __init__(self, cfg: Settings, options: ProducerOptions) -> None:
        self.cfg = cfg
        self.options = options
        self.running = True
        ensure_topics(cfg.kafka_bootstrap_servers, [cfg.kafka_topic, cfg.kafka_dlq_topic])
        self.kafka = Producer({"bootstrap.servers": cfg.kafka_bootstrap_servers})
        self.simulator = SensorSimulator(
            options.sensors,
            options.seed,
            options.anomaly_rate,
            options.drift_per_reading,
        )
        # Keep invalid-message selection independent from simulated measurement values.
        self.chaos = random.Random(options.seed)

    def stop(self) -> None:
        self.running = False

    def run(self) -> None:
        log.info(
            "producer started",
            extra={"rate": self.options.rate, "sensors": self.options.sensors},
        )
        sent = 0
        interval = 1.0 / self.options.rate
        next_send = time.monotonic()
        while self.running and (self.options.max_events == 0 or sent < self.options.max_events):
            reading = self.simulator.next_reading(datetime.now(UTC))
            message = reading.to_message()
            if self.chaos.random() < self.options.invalid_rate:
                payload = corrupt_message(message, self.chaos.random())
            else:
                payload = encode_message(message)

            self.kafka.produce(
                self.cfg.kafka_topic,
                payload,
                key=reading.sensor_id,
                on_delivery=self._on_delivery,
            )
            self.kafka.poll(0)
            sent += 1
            if sent % 100 == 0:
                log.info("progress", extra={"sent": sent})
            next_send += interval
            time.sleep(max(0.0, next_send - time.monotonic()))

        self.kafka.flush(10)
        log.info("producer stopped", extra={"sent": sent})

    @staticmethod
    def _on_delivery(error: object, message: object) -> None:
        if error is not None:
            log.error("delivery failed", extra={"error": str(error)})
