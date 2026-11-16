"""Database table definitions and connection helper, shared by all services.

Tables are created with `metadata.create_all` (idempotent); no migration tool is used.
"""

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

from common.retry import retry

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


# One row per (drift check, feature). Written by the monitoring job, read by the API.
drift_metrics = Table(
    "drift_metrics",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("computed_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column("feature", String(50), nullable=False),
    Column("ks_statistic", Float, nullable=False),  # Kolmogorov-Smirnov distance, 0..1
    Column("ks_pvalue", Float, nullable=False),
    Column("psi", Float, nullable=False),  # Population Stability Index
    Column("drift_detected", Boolean, nullable=False),
    Column("window_size", Integer, nullable=False),  # live readings compared
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
