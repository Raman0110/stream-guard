"""Database table definitions and connection helper, shared by all services."""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Engine,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    text,
)

from common.config import Settings
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
Index("ix_predictions_is_anomaly", predictions.c.is_anomaly)

dlq_events = Table(
    "dlq_events",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("received_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column("raw_payload", Text, nullable=False),
    Column("error", Text, nullable=False),
)
Index("ix_dlq_events_received_at", dlq_events.c.received_at)


# Durable messages waiting to be published to Kafka. Writing this row in the same transaction as
# the DLQ record lets the input offset be committed without risking loss when Kafka is unavailable.
outbox_events = Table(
    "outbox_events",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("topic", String(255), nullable=False),
    Column("message_key", LargeBinary, nullable=True),
    Column("payload", LargeBinary, nullable=False),
    Column("created_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=True),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("last_error", Text, nullable=True),
)
Index("ix_outbox_events_published_at", outbox_events.c.published_at)


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
Index("ix_drift_metrics_feature_computed_at", drift_metrics.c.feature, drift_metrics.c.computed_at)
Index("ix_readings_ingested_at", readings.c.ingested_at)


def connect(
    url: str,
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
    pool_timeout_seconds: float = 30.0,
    pool_recycle_seconds: int = 1800,
) -> Engine:
    """Create an engine and verify connectivity, retrying while Postgres starts up.

    Schema changes are owned by Alembic and run before application services start.
    """
    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout_seconds,
        pool_recycle=pool_recycle_seconds,
    )

    def _init() -> None:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    retry(_init, what="database connect")
    return engine


def connect_from_settings(settings: Settings) -> Engine:
    """Create an engine using the shared database and connection-pool configuration."""
    return connect(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout_seconds=settings.db_pool_timeout_seconds,
        pool_recycle_seconds=settings.db_pool_recycle_seconds,
    )
