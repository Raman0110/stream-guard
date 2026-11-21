"""Load / latency test for the running pipeline (start it first: `docker compose up -d`).

It publishes N synthetic events to Kafka at a chosen rate, waits until the consumer has
stored every one, then reports:
  * throughput (events/sec) for sending and for the whole pipeline,
  * end-to-end latency percentiles: event timestamp -> prediction stored in PostgreSQL
    (the same definition the API's /metrics/pipeline uses).

Test events use sensor ids "load-1", "load-2", ... so they can be told apart from normal
traffic, and they are deleted afterwards unless --keep is given.

Examples:
    python -m scripts.load_test --events 3000 --rate 100     # steady load: latency
    python -m scripts.load_test --events 10000 --rate 0      # unthrottled: peak throughput
"""

import argparse
import json
import os
import platform
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import numpy.typing as npt
from confluent_kafka import KafkaException, Producer
from sqlalchemy import Engine, create_engine, delete, func, select, text

from common.config import load_settings
from common.tables import predictions, readings
from producer.simulator import SensorSimulator

SENSOR_PREFIX = "load-"


def load_dotenv(path: Path) -> None:
    """Minimal .env reader so the script works from the host without extra tools.
    Real environment variables take precedence over the file."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


@dataclass(frozen=True)
class RunResult:
    sent: int
    stored: int
    send_seconds: float  # first publish -> last publish
    total_seconds: float  # first publish -> last prediction stored
    latencies_s: npt.NDArray[np.float64]  # one per stored event, clock-skew corrected
    clock_skew_s: float


def summarize(result: RunResult) -> dict[str, float | int]:
    """Turn raw measurements into the headline numbers."""
    lat_ms = result.latencies_s * 1000
    has = len(lat_ms) > 0
    return {
        "sent": result.sent,
        "stored": result.stored,
        "lost": result.sent - result.stored,
        "offered_rate_eps": result.sent / result.send_seconds if result.send_seconds > 0 else 0.0,
        "pipeline_throughput_eps": (
            result.stored / result.total_seconds if result.total_seconds > 0 else 0.0
        ),
        "latency_p50_ms": float(np.percentile(lat_ms, 50)) if has else float("nan"),
        "latency_p95_ms": float(np.percentile(lat_ms, 95)) if has else float("nan"),
        "latency_p99_ms": float(np.percentile(lat_ms, 99)) if has else float("nan"),
        "latency_max_ms": float(lat_ms.max()) if has else float("nan"),
    }


def estimate_clock_skew(engine: Engine, samples: int = 7) -> float:
    """Seconds by which the database clock is ahead of this machine's clock.

    Events are timestamped here but predictions are timestamped by the database. In Docker
    Desktop the database runs in a VM whose clock can differ from the host's, which would
    corrupt every latency. We measure the offset (median of several samples) and remove it.
    """
    offsets = []
    for _ in range(samples):
        before = datetime.now(UTC)
        with engine.connect() as conn:
            db_now = conn.execute(text("SELECT now()")).scalar_one()
        after = datetime.now(UTC)
        midpoint = before + (after - before) / 2  # best guess of when the DB read its clock
        offsets.append((db_now - midpoint).total_seconds())
    return float(statistics.median(offsets))


def publish(
    producer: Producer, topic: str, events: int, rate: float, sensors: int, seed: int
) -> tuple[float, float, int]:
    """Publish events (rate 0 = as fast as possible). Returns (t_first, t_last, failures)."""
    failures = 0

    def on_delivery(error: object, message: object) -> None:
        nonlocal failures
        if error is not None:
            failures += 1

    sim = SensorSimulator(sensors, seed)  # clean data: this test measures speed, not accuracy
    interval = 1.0 / rate if rate > 0 else 0.0
    t_first = time.time()
    next_send = time.monotonic()
    last_ts = datetime.min.replace(tzinfo=UTC)
    for _ in range(events):
        # Event timestamps must be unique: (sensor_id, ts) is the pipeline's duplicate key, and
        # the Windows clock ticks only every ~15 ms, so at high rates many events would share
        # a timestamp and be (correctly) discarded as duplicates. Force strictly increasing.
        ts = max(datetime.now(UTC), last_ts + timedelta(microseconds=1))
        last_ts = ts
        reading = sim.next_reading(ts)
        message = reading.to_message()
        message["sensor_id"] = SENSOR_PREFIX + str(message["sensor_id"]).split("-")[-1]
        payload = json.dumps(message).encode()
        while True:
            try:
                producer.produce(topic, payload, key=str(message["sensor_id"]),
                                 on_delivery=on_delivery)  # fmt: skip
                break
            except BufferError:  # local queue full: let it drain, then retry
                producer.poll(0.1)
        producer.poll(0)
        if interval:
            next_send += interval
            time.sleep(max(0.0, next_send - time.monotonic()))
    t_last = time.time()
    producer.flush(60)
    return t_first, t_last, failures


def wait_until_stored(engine: Engine, expected: int, since: datetime, timeout: float) -> int:
    """Poll until `expected` test events are stored (or timeout). Returns the count."""
    stmt = (
        select(func.count())
        .select_from(readings)
        .where(readings.c.sensor_id.like(SENSOR_PREFIX + "%"), readings.c.ts >= since)
    )
    deadline = time.time() + timeout
    count = 0
    while time.time() < deadline:
        with engine.connect() as conn:
            count = conn.execute(stmt).scalar_one()
        if count >= expected:
            break
        time.sleep(0.5)
    return int(count)


def collect_latencies(
    engine: Engine, since: datetime, skew_s: float
) -> tuple[npt.NDArray[np.float64], datetime | None]:
    """Per-event latency = (stored_at - skew) - event timestamp. Also returns last stored_at."""
    stmt = (
        select(readings.c.ts, predictions.c.scored_at)
        .select_from(readings.join(predictions, predictions.c.reading_id == readings.c.id))
        .where(readings.c.sensor_id.like(SENSOR_PREFIX + "%"), readings.c.ts >= since)
    )
    with engine.connect() as conn:
        rows = conn.execute(stmt).all()
    if not rows:
        return np.array([], dtype=np.float64), None
    latencies = np.array([(s - t).total_seconds() - skew_s for t, s in rows], dtype=np.float64)
    return latencies, max(s for _, s in rows)


def count_background(engine: Engine, since: datetime) -> int:
    """Normal (non-test) readings stored since `since`: traffic that shared the pipeline."""
    stmt = (
        select(func.count())
        .select_from(readings)
        .where(~readings.c.sensor_id.like(SENSOR_PREFIX + "%"), readings.c.ts >= since)
    )
    with engine.connect() as conn:
        return int(conn.execute(stmt).scalar_one())


def cleanup(engine: Engine, since: datetime) -> None:
    """Remove the test rows (predictions first: they reference readings)."""
    with engine.begin() as conn:
        ids = select(readings.c.id).where(
            readings.c.sensor_id.like(SENSOR_PREFIX + "%"), readings.c.ts >= since
        )
        conn.execute(delete(predictions).where(predictions.c.reading_id.in_(ids)))
        conn.execute(
            delete(readings).where(
                readings.c.sensor_id.like(SENSOR_PREFIX + "%"), readings.c.ts >= since
            )
        )


def render_report(
    stats: dict[str, float | int], args: argparse.Namespace, skew_s: float, background: int
) -> str:
    rate = "unthrottled" if args.rate == 0 else f"{args.rate:g} events/sec"
    return f"""# Load test results

Generated by `python -m scripts.load_test` on {datetime.now(UTC).isoformat(timespec="seconds")}.

## Setup
- Events: {stats["sent"]} at target rate: {rate} ({args.sensors} sensors)
- Machine: {platform.platform()}, {os.cpu_count()} logical CPUs
- The whole stack runs in Docker Compose on that one machine
- Pipeline under test: Redpanda -> consumer (validate, features, model) -> PostgreSQL

## Results
| Metric | Value |
|---|---|
| Events sent / stored / lost | {stats["sent"]} / {stats["stored"]} / {stats["lost"]} |
| Offered (send) rate | {stats["offered_rate_eps"]:.0f} events/sec |
| Pipeline throughput (send to stored) | {stats["pipeline_throughput_eps"]:.0f} events/sec |
| Latency p50 | {stats["latency_p50_ms"]:.0f} ms |
| Latency p95 | {stats["latency_p95_ms"]:.0f} ms |
| Latency p99 | {stats["latency_p99_ms"]:.0f} ms |
| Latency max | {stats["latency_max_ms"]:.0f} ms |

## Notes
- Latency = time from the event's timestamp (set when it is published) until its prediction
  is stored in PostgreSQL. It includes Kafka, validation, feature computation, scoring and
  the database write.
- The host and database clocks differed by {skew_s * 1000:+.0f} ms; latencies are corrected for it.
- When the offered rate exceeds what the consumer can process, events queue up in Kafka and
  latency grows with the backlog: an unthrottled run measures peak throughput, not latency
  under normal load.
- Background traffic: {background} normal (non-test) events were also processed during the run.
  Run `docker compose stop producer` first for a clean measurement.
- Test rows are deleted after the run.
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--events", type=int, default=3000)
    parser.add_argument("--rate", type=float, default=100.0, help="events/sec (0 = unthrottled)")
    parser.add_argument("--sensors", type=int, default=5)
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--timeout", type=float, default=120.0, help="max seconds to wait to drain")
    parser.add_argument("--report", type=Path, help="also write a markdown report to this path")
    parser.add_argument("--keep", action="store_true", help="do not delete the test rows")
    args = parser.parse_args()

    load_dotenv(Path(".env"))
    cfg = load_settings()
    engine = create_engine(cfg.database_url, pool_pre_ping=True)
    producer = Producer({"bootstrap.servers": cfg.kafka_bootstrap_servers, "linger.ms": 5})

    try:
        skew_s = estimate_clock_skew(engine)
        producer.list_topics(cfg.kafka_topic, timeout=10)  # fail fast if Kafka is unreachable
    except (KafkaException, Exception) as exc:
        print(
            f"Cannot reach the stack ({exc}). Is `docker compose up -d` running?", file=sys.stderr
        )
        return 2

    mode = "unthrottled" if args.rate == 0 else f"{args.rate:g}/s"
    print(f"Publishing {args.events} events ({mode})...")
    since = datetime.now(UTC)
    t_first, t_last, failures = publish(
        producer, cfg.kafka_topic, args.events, args.rate, args.sensors, args.seed
    )
    stored = wait_until_stored(engine, args.events - failures, since, args.timeout)
    latencies, last_stored = collect_latencies(engine, since, skew_s)
    background = count_background(engine, since)
    if not args.keep:
        cleanup(engine, since)

    total = 0.0
    if last_stored is not None:
        total = (last_stored.timestamp() - skew_s) - t_first
    result = RunResult(args.events, stored, t_last - t_first, total, latencies, skew_s)
    stats = summarize(result)

    report = render_report(stats, args, skew_s, background)
    print(report)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report)
        print(f"Report written to {args.report}")
    return 0 if stats["lost"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
