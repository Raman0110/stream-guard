"""PostgreSQL access for the consumer (SQLAlchemy Core)."""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Engine,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import insert

from common.retry import retry
from common.schema import SensorReading

metadata = MetaData()

readings = Table(
    "readings",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("sensor_id", String(50), nullable=False),
    Column("ts", DateTime(timezone=True), nullable=False),
    Column("temperature", Float, nullable=False),
    Column("humidity", Float, nullable=False),
    Column("vibration", Float, nullable=False),
    Column("ingested_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    # Idempotency key: a redelivered Kafka message maps to the same (sensor, ts) and is skipped.
    UniqueConstraint("sensor_id", "ts", name="uq_readings_sensor_ts"),
)

predictions = Table(
    "predictions",
    metadata,
    # unique: exactly one prediction per stored reading
    Column("reading_id", Integer, ForeignKey("readings.id"), primary_key=True),
    Column("anomaly_score", Float, nullable=False),  # higher = more anomalous
    Column("is_anomaly", Boolean, nullable=False),
    Column("model_version", String(32), nullable=False),
    # scored_at - readings.ts = end-to-end latency (used by the pipeline metrics later)
    Column("scored_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
)

dlq_events = Table(
    "dlq_events",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("received_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column("raw_payload", Text, nullable=False),
    Column("error", Text, nullable=False),
)


def connect(url: str) -> Engine:
    """Create an engine and create tables, retrying while Postgres starts up."""
    engine = create_engine(url, pool_pre_ping=True)

    def _init() -> None:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        metadata.create_all(engine)  # simple schema management: no migrations for this project

    retry(_init, what="database connect")
    return engine


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
