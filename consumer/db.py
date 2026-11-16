"""Consumer database writes (SQLAlchemy Core). Table definitions live in common/tables.py."""

from sqlalchemy import Engine
from sqlalchemy.dialects.postgresql import insert

from common.schema import SensorReading
from common.tables import dlq_events, predictions, readings


def insert_scored_readings(
    engine: Engine,
    batch: list[SensorReading],
    scores: list[float],
    flags: list[bool],
    model_version: str,
) -> int:
    """Store readings AND their predictions in one transaction.

    Duplicates (same sensor_id + ts) are skipped and get no new prediction, because they
    already have one. Returns the number of readings actually inserted.
    """
    if not batch:
        return 0
    stmt = (
        insert(readings)
        .values(
            [
                {
                    "sensor_id": r.sensor_id,
                    "ts": r.timestamp,
                    "temperature": r.temperature,
                    "humidity": r.humidity,
                    "vibration": r.vibration,
                }
                for r in batch
            ]
        )
        .on_conflict_do_nothing(constraint="uq_readings_sensor_ts")
        .returning(readings.c.id, readings.c.sensor_id, readings.c.ts)
    )
    # Map each reading to its prediction so we can match them to the new row ids.
    pred_by_key = {
        (r.sensor_id, r.timestamp): (s, f) for r, s, f in zip(batch, scores, flags, strict=True)
    }
    with engine.begin() as conn:
        inserted = conn.execute(stmt).fetchall()
        pred_rows = []
        for row_id, sensor_id, ts in inserted:
            score, flag = pred_by_key[(sensor_id, ts)]
            pred_rows.append(
                {
                    "reading_id": row_id,
                    "anomaly_score": score,
                    "is_anomaly": flag,
                    "model_version": model_version,
                }
            )
        if pred_rows:
            conn.execute(predictions.insert(), pred_rows)
    return len(inserted)


def insert_dlq(engine: Engine, items: list[tuple[str, str]]) -> None:
    """Record dead-lettered messages as (raw_payload, error) pairs."""
    if not items:
        return
    with engine.begin() as conn:
        conn.execute(dlq_events.insert(), [{"raw_payload": p, "error": e} for p, e in items])
