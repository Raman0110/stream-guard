"""Producer: publishes simulated sensor readings to Kafka at a fixed rate.

Examples:
    python -m producer.main
    python -m producer.main --anomaly-rate 0.05
    python -m producer.main --drift-per-reading 0.01 --max-events 5000
"""

import argparse
import json
import logging
import random
import signal
import time
from datetime import UTC, datetime
from types import FrameType

from confluent_kafka import Producer

from common.config import load_settings
from common.kafka_utils import ensure_topics
from common.logging_setup import setup_logging
from producer.simulator import SensorSimulator

log = logging.getLogger("producer")
_running = True


def _stop(signum: int, frame: FrameType | None) -> None:
    global _running
    _running = False


def corrupt(message: dict[str, object], rng_value: float) -> bytes:
    """Make a malformed payload, to exercise the dead-letter queue."""
    if rng_value < 0.33:
        return b"this is not json"
    if rng_value < 0.66:
        message["temperature"] = "hot"  # wrong type
        return json.dumps(message).encode()
    message.pop("sensor_id")  # missing required field
    return json.dumps(message).encode()


def main() -> None:
    cfg = load_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=float, default=cfg.events_per_second, help="events/sec")
    parser.add_argument("--sensors", type=int, default=cfg.num_sensors)
    parser.add_argument("--seed", type=int, default=cfg.random_seed)
    parser.add_argument("--anomaly-rate", type=float, default=cfg.anomaly_rate)
    parser.add_argument("--drift-per-reading", type=float, default=cfg.drift_per_reading)
    parser.add_argument("--invalid-rate", type=float, default=cfg.invalid_rate)
    parser.add_argument(
        "--max-events", type=int, default=0, help="stop after N events (0 = run forever)"
    )
    args = parser.parse_args()

    setup_logging()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    ensure_topics(cfg.kafka_bootstrap_servers, [cfg.kafka_topic, cfg.kafka_dlq_topic])
    kafka = Producer({"bootstrap.servers": cfg.kafka_bootstrap_servers})
    sim = SensorSimulator(args.sensors, args.seed, args.anomaly_rate, args.drift_per_reading)
    # A separate RNG for invalid-message injection keeps the sensor data identical per seed.
    chaos = random.Random(args.seed)

    def on_delivery(err: object, msg: object) -> None:
        if err is not None:
            log.error("delivery failed", extra={"error": str(err)})

    log.info("producer started", extra={"rate": args.rate, "sensors": args.sensors})
    sent = 0
    interval = 1.0 / args.rate
    next_send = time.monotonic()
    while _running and (args.max_events == 0 or sent < args.max_events):
        reading = sim.next_reading(datetime.now(UTC))
        message = reading.to_message()
        if chaos.random() < args.invalid_rate:
            payload = corrupt(message, chaos.random())
        else:
            payload = json.dumps(message).encode()
        # Key = sensor_id: all readings of one sensor go to the same partition, in order.
        kafka.produce(cfg.kafka_topic, payload, key=reading.sensor_id, on_delivery=on_delivery)
        kafka.poll(0)  # serve delivery callbacks
        sent += 1
        if sent % 100 == 0:
            log.info("progress", extra={"sent": sent})
        # Sleep until the next slot (absolute schedule, so the rate doesn't drift).
        next_send += interval
        time.sleep(max(0.0, next_send - time.monotonic()))

    kafka.flush(10)  # graceful shutdown: deliver anything still buffered
    log.info("producer stopped", extra={"sent": sent})


if __name__ == "__main__":
    main()
