"""Consumer service: Kafka -> validate -> PostgreSQL (+ dead-letter queue).

Delivery semantics: AT-LEAST-ONCE. Offsets are committed only AFTER the batch is stored
in Postgres. A crash between "store" and "commit" means the batch is redelivered; the
UNIQUE(sensor_id, ts) constraint then turns the duplicates into no-ops.
"""

import logging
import signal
from types import FrameType

from confluent_kafka import Consumer, Message, Producer
from sqlalchemy import Engine

from common.config import Settings, load_settings
from common.kafka_utils import ensure_topics
from common.logging_setup import setup_logging
from common.schema import SensorReading
from consumer import db
from consumer.processing import parse_message

log = logging.getLogger("consumer")
BATCH_SIZE = 100


class ConsumerService:
    def __init__(self, cfg: Settings, engine: Engine) -> None:
        self.cfg = cfg
        self.engine = engine
        self.running = True
        ensure_topics(cfg.kafka_bootstrap_servers, [cfg.kafka_topic, cfg.kafka_dlq_topic])
        self.consumer = Consumer(
            {
                "bootstrap.servers": cfg.kafka_bootstrap_servers,
                "group.id": "stream-consumer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,  # we commit manually, after the DB write
            }
        )
        self.consumer.subscribe([cfg.kafka_topic])
        self.dlq_producer = Producer({"bootstrap.servers": cfg.kafka_bootstrap_servers})
        self.processed = 0
        self.duplicates = 0
        self.dead_lettered = 0

    def stop(self) -> None:
        self.running = False

    def run(self) -> None:
        log.info("consumer started")
        while self.running:
            msgs = self.consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            self.handle_batch([m for m in msgs if self._ok(m)])
        self.consumer.close()  # leaves the group cleanly (graceful shutdown)
        log.info("consumer stopped", extra=self.counters())

    @staticmethod
    def _ok(msg: Message) -> bool:
        err = msg.error()
        if err is not None:
            log.error("kafka error", extra={"error": str(err)})
            return False
        return True

    def handle_batch(self, msgs: list[Message]) -> None:
        if not msgs:
            return
        valid: list[SensorReading] = []
        bad: list[tuple[str, str]] = []
        for msg in msgs:
            raw = msg.value() or b""
            reading, error = parse_message(raw)
            if reading is not None:
                valid.append(reading)
            else:
                assert error is not None
                bad.append((raw.decode("utf-8", errors="replace"), error))
                self.dlq_producer.produce(self.cfg.kafka_dlq_topic, raw, key=msg.key())

        inserted = db.insert_readings(self.engine, valid)
        db.insert_dlq(self.engine, bad)
        self.dlq_producer.flush(10)
        self.consumer.commit(asynchronous=False)  # only now is the batch "done"

        self.processed += inserted
        self.duplicates += len(valid) - inserted
        self.dead_lettered += len(bad)
        log.info("batch done", extra={"batch": len(msgs), **self.counters()})

    def counters(self) -> dict[str, int]:
        return {
            "processed": self.processed,
            "duplicates": self.duplicates,
            "dead_lettered": self.dead_lettered,
        }


def main() -> None:
    setup_logging()
    cfg = load_settings()
    service = ConsumerService(cfg, db.connect(cfg.database_url))

    def _stop(signum: int, frame: FrameType | None) -> None:
        service.stop()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    service.run()


if __name__ == "__main__":
    main()
