"""PostgreSQL access for the consumer (SQLAlchemy Core)."""

from sqlalchemy import (
    Column,
    DateTime,
    Engine,
    Float,
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


def insert_readings(engine: Engine, batch: list[SensorReading]) -> int:
    """Insert readings, skipping duplicates. Returns the number actually inserted."""
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
        .returning(readings.c.id)
    )
    with engine.begin() as conn:
        return len(conn.execute(stmt).fetchall())


def insert_dlq(engine: Engine, items: list[tuple[str, str]]) -> None:
    """Record dead-lettered messages as (raw_payload, error) pairs."""
    if not items:
        return
    with engine.begin() as conn:
        conn.execute(dlq_events.insert(), [{"raw_payload": p, "error": e} for p, e in items])
