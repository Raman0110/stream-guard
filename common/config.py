"""Central configuration, read from environment variables.

Every service imports `load_settings()` instead of calling os.environ itself,
so defaults and variable names live in exactly one place.
"""

import os
from dataclasses import dataclass


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

    @property
    def database_url(self) -> str:
        """SQLAlchemy connection URL built from the Postgres settings."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
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
    )
