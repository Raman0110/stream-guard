"""Central configuration, read from environment variables.

Every service imports `load_settings()` instead of calling os.environ itself,
so defaults and variable names live in exactly one place.
"""

import os
from dataclasses import dataclass
from urllib.parse import quote


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_dlq_topic: str
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_host: str
    postgres_port: int
    num_sensors: int
    events_per_second: float
    random_seed: int
    anomaly_rate: float
    drift_per_reading: float
    invalid_rate: float
    model_path: str = "models/model.joblib"
    baseline_features_path: str = "models/baseline_features.csv"
    drift_window_size: int = 1000
    drift_check_interval_seconds: float = 30.0
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_pool_timeout_seconds: float = 30.0
    db_pool_recycle_seconds: int = 1800
    readings_retention_days: int = 90
    dlq_retention_days: int = 30
    drift_retention_days: int = 90
    outbox_retention_days: int = 7
    retention_batch_size: int = 10_000

    @property
    def database_url(self) -> str:
        """SQLAlchemy connection URL built from the Postgres settings."""
        return (
            f"postgresql+psycopg2://{quote(self.postgres_user, safe='')}:"
            f"{quote(self.postgres_password, safe='')}@{self.postgres_host}:{self.postgres_port}/"
            f"?dbname={quote(self.postgres_db, safe='')}"
        )


def load_settings() -> Settings:
    """Build Settings from the current environment (re-read on every call)."""
    env = os.environ
    return Settings(
        kafka_bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        kafka_topic=env.get("KAFKA_TOPIC", "sensor-readings"),
        kafka_dlq_topic=env.get("KAFKA_DLQ_TOPIC", "sensor-readings-dlq"),
        postgres_user=env.get("POSTGRES_USER", "streamguard"),
        # No real default for the password: an empty value fails loudly at connect time.
        postgres_password=env.get("POSTGRES_PASSWORD", ""),
        postgres_db=env.get("POSTGRES_DB", "streamguard"),
        postgres_host=env.get("POSTGRES_HOST", "localhost"),
        postgres_port=int(env.get("POSTGRES_PORT", "5432")),
        num_sensors=int(env.get("NUM_SENSORS", "5")),
        events_per_second=float(env.get("EVENTS_PER_SECOND", "10")),
        random_seed=int(env.get("RANDOM_SEED", "42")),
        anomaly_rate=float(env.get("ANOMALY_RATE", "0")),
        drift_per_reading=float(env.get("DRIFT_PER_READING", "0")),
        invalid_rate=float(env.get("INVALID_RATE", "0")),
        model_path=env.get("MODEL_PATH", "models/model.joblib"),
        baseline_features_path=env.get("BASELINE_FEATURES_PATH", "models/baseline_features.csv"),
        drift_window_size=int(env.get("DRIFT_WINDOW_SIZE", "1000")),
        drift_check_interval_seconds=float(env.get("DRIFT_CHECK_INTERVAL_SECONDS", "30")),
        db_pool_size=int(env.get("DB_POOL_SIZE", "5")),
        db_max_overflow=int(env.get("DB_MAX_OVERFLOW", "10")),
        db_pool_timeout_seconds=float(env.get("DB_POOL_TIMEOUT_SECONDS", "30")),
        db_pool_recycle_seconds=int(env.get("DB_POOL_RECYCLE_SECONDS", "1800")),
        readings_retention_days=int(env.get("READINGS_RETENTION_DAYS", "90")),
        dlq_retention_days=int(env.get("DLQ_RETENTION_DAYS", "30")),
        drift_retention_days=int(env.get("DRIFT_RETENTION_DAYS", "90")),
        outbox_retention_days=int(env.get("OUTBOX_RETENTION_DAYS", "7")),
        retention_batch_size=int(env.get("RETENTION_BATCH_SIZE", "10000")),
    )
