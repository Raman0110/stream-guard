"""Transactional persistence and recovery queries for the consumer."""

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Engine, func, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert

from common.schema import SensorReading
from common.tables import dlq_events, outbox_events, predictions, readings

ReadingKey = tuple[str, datetime]


@dataclass(frozen=True)
class DeadLetter:
    raw_payload: bytes
    error: str
    source_key: bytes | None


@dataclass(frozen=True)
class PendingOutboxEvent:
    id: int
    topic: str
    message_key: bytes | None
    payload: bytes


class ConcurrentReadingInsertError(RuntimeError):
    """A supposedly fresh reading was inserted concurrently by another consumer."""


def reading_key(reading: SensorReading) -> ReadingKey:
    return reading.sensor_id, reading.timestamp


def load_existing_reading_keys(engine: Engine, batch: list[SensorReading]) -> set[ReadingKey]:
    """Return idempotency keys that are already stored."""
    keys = [reading_key(reading) for reading in batch]
    if not keys:
        return set()
    stmt = select(readings.c.sensor_id, readings.c.ts).where(
        tuple_(readings.c.sensor_id, readings.c.ts).in_(keys)
    )
    with engine.connect() as conn:
        return {(sensor_id, timestamp) for sensor_id, timestamp in conn.execute(stmt)}


def load_recent_history(engine: Engine, per_sensor: int) -> list[tuple[str, float, float, float]]:
    """Load the newest readings per sensor, ordered for replay into rolling windows."""
    if per_sensor <= 0:
        return []
    rank = func.row_number().over(
        partition_by=readings.c.sensor_id,
        order_by=readings.c.ts.desc(),
    )
    ranked = select(
        readings.c.sensor_id,
        readings.c.ts,
        readings.c.temperature,
        readings.c.humidity,
        readings.c.vibration,
        rank.label("recency_rank"),
    ).subquery()
    stmt = (
        select(
            ranked.c.sensor_id,
            ranked.c.temperature,
            ranked.c.humidity,
            ranked.c.vibration,
        )
        .where(ranked.c.recency_rank <= per_sensor)
        .order_by(ranked.c.ts)
    )
    with engine.connect() as conn:
        return [
            (sensor_id, temperature, humidity, vibration)
            for sensor_id, temperature, humidity, vibration in conn.execute(stmt)
        ]


def persist_batch(
    engine: Engine,
    batch: list[SensorReading],
    scores: list[float],
    flags: list[bool],
    model_version: str,
    dead_letters: list[DeadLetter],
    dlq_topic: str,
) -> int:
    """Atomically store readings, predictions, rejected messages, and outbox events.

    Callers remove known duplicates before feature calculation. If a concurrent writer still wins
    the uniqueness race, the transaction is rolled back so tentative rolling state is not applied.
    """
    if len(batch) != len(scores) or len(batch) != len(flags):
        raise ValueError("every reading requires one score and one anomaly flag")

    inserted_count = 0
    with engine.begin() as conn:
        if batch:
            stmt = (
                insert(readings)
                .values(
                    [
                        {
                            "sensor_id": reading.sensor_id,
                            "ts": reading.timestamp,
                            "temperature": reading.temperature,
                            "humidity": reading.humidity,
                            "vibration": reading.vibration,
                        }
                        for reading in batch
                    ]
                )
                .on_conflict_do_nothing(constraint="uq_readings_sensor_ts")
                .returning(readings.c.id, readings.c.sensor_id, readings.c.ts)
            )
            inserted_rows = conn.execute(stmt).fetchall()
            inserted_count = len(inserted_rows)
            if inserted_count != len(batch):
                raise ConcurrentReadingInsertError(
                    "a reading was inserted after duplicate detection; retry the batch"
                )

            pred_by_key = {
                reading_key(reading): (score, flag)
                for reading, score, flag in zip(batch, scores, flags, strict=True)
            }
            conn.execute(
                predictions.insert(),
                [
                    {
                        "reading_id": row_id,
                        "anomaly_score": pred_by_key[(sensor_id, timestamp)][0],
                        "is_anomaly": pred_by_key[(sensor_id, timestamp)][1],
                        "model_version": model_version,
                    }
                    for row_id, sensor_id, timestamp in inserted_rows
                ],
            )

        if dead_letters:
            conn.execute(
                dlq_events.insert(),
                [
                    {
                        "raw_payload": item.raw_payload.decode("utf-8", errors="replace"),
                        "error": item.error,
                    }
                    for item in dead_letters
                ],
            )
            conn.execute(
                outbox_events.insert(),
                [
                    {
                        "topic": dlq_topic,
                        "message_key": item.source_key,
                        "payload": item.raw_payload,
                    }
                    for item in dead_letters
                ],
            )
    return inserted_count


def load_pending_outbox(engine: Engine, limit: int = 100) -> list[PendingOutboxEvent]:
    """Return unpublished outbox rows in creation order."""
    stmt = (
        select(
            outbox_events.c.id,
            outbox_events.c.topic,
            outbox_events.c.message_key,
            outbox_events.c.payload,
        )
        .where(outbox_events.c.published_at.is_(None))
        .order_by(outbox_events.c.id)
        .limit(limit)
    )
    with engine.connect() as conn:
        return [PendingOutboxEvent(*row) for row in conn.execute(stmt)]


def record_outbox_results(
    engine: Engine,
    delivered: list[int],
    failed: dict[int, str],
) -> None:
    """Mark confirmed publishes and retain failures for later retries."""
    if not delivered and not failed:
        return
    with engine.begin() as conn:
        if delivered:
            conn.execute(
                update(outbox_events)
                .where(outbox_events.c.id.in_(delivered))
                .values(
                    published_at=func.now(),
                    attempts=outbox_events.c.attempts + 1,
                    last_error=None,
                )
            )
        for event_id, error in failed.items():
            conn.execute(
                update(outbox_events)
                .where(outbox_events.c.id == event_id)
                .values(
                    attempts=outbox_events.c.attempts + 1,
                    last_error=error[:2000],
                )
            )
