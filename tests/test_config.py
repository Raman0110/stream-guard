import pytest
from sqlalchemy.engine import make_url

from common.config import load_settings


def test_defaults(monkeypatch):
    for name in (
        "NUM_SENSORS",
        "KAFKA_TOPIC",
        "POSTGRES_PORT",
        "DRIFT_WINDOW_SIZE",
        "DRIFT_CHECK_INTERVAL_SECONDS",
        "DB_POOL_SIZE",
        "DB_MAX_OVERFLOW",
        "DB_POOL_TIMEOUT_SECONDS",
        "DB_POOL_RECYCLE_SECONDS",
        "READINGS_RETENTION_DAYS",
        "DLQ_RETENTION_DAYS",
        "DRIFT_RETENTION_DAYS",
        "OUTBOX_RETENTION_DAYS",
        "RETENTION_BATCH_SIZE",
        "API_AUTH_ENABLED",
        "API_KEY",
        "API_KEY_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    s = load_settings()
    assert s.num_sensors == 5
    assert s.kafka_topic == "sensor-readings"
    assert s.postgres_port == 5432
    assert s.drift_window_size == 1000
    assert s.drift_check_interval_seconds == 30
    assert s.db_pool_size == 5
    assert s.db_max_overflow == 10
    assert s.db_pool_timeout_seconds == 30
    assert s.db_pool_recycle_seconds == 1800
    assert s.readings_retention_days == 90
    assert s.dlq_retention_days == 30
    assert s.drift_retention_days == 90
    assert s.outbox_retention_days == 7
    assert s.retention_batch_size == 10_000
    assert s.api_auth_enabled is False
    assert s.api_key == ""


def test_env_override_and_database_url(monkeypatch):
    monkeypatch.setenv("NUM_SENSORS", "8")
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_DB", "d")
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    monkeypatch.setenv("DRIFT_WINDOW_SIZE", "500")
    monkeypatch.setenv("DRIFT_CHECK_INTERVAL_SECONDS", "5")
    s = load_settings()
    assert s.num_sensors == 8
    assert s.database_url == "postgresql+psycopg2://u:p@db:6543/?dbname=d"
    assert s.drift_window_size == 500
    assert s.drift_check_interval_seconds == 5


def test_database_url_encodes_credentials(monkeypatch):
    monkeypatch.setenv("POSTGRES_USER", "service user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss/word:100%")
    monkeypatch.setenv("POSTGRES_DB", "sensor data")
    settings = load_settings()
    parsed = make_url(settings.database_url)

    assert parsed.username == "service user"
    assert parsed.password == "p@ss/word:100%"
    assert parsed.query["dbname"] == "sensor data"


def test_database_pool_settings(monkeypatch):
    monkeypatch.setenv("DB_POOL_SIZE", "8")
    monkeypatch.setenv("DB_MAX_OVERFLOW", "12")
    monkeypatch.setenv("DB_POOL_TIMEOUT_SECONDS", "4.5")
    monkeypatch.setenv("DB_POOL_RECYCLE_SECONDS", "600")

    settings = load_settings()

    assert settings.db_pool_size == 8
    assert settings.db_max_overflow == 12
    assert settings.db_pool_timeout_seconds == 4.5
    assert settings.db_pool_recycle_seconds == 600


def test_retention_settings(monkeypatch):
    monkeypatch.setenv("READINGS_RETENTION_DAYS", "365")
    monkeypatch.setenv("DLQ_RETENTION_DAYS", "60")
    monkeypatch.setenv("DRIFT_RETENTION_DAYS", "180")
    monkeypatch.setenv("OUTBOX_RETENTION_DAYS", "14")
    monkeypatch.setenv("RETENTION_BATCH_SIZE", "2500")

    settings = load_settings()

    assert settings.readings_retention_days == 365
    assert settings.dlq_retention_days == 60
    assert settings.drift_retention_days == 180
    assert settings.outbox_retention_days == 14
    assert settings.retention_batch_size == 2500


def test_api_key_loaded_from_secret_file(monkeypatch, tmp_path):
    secret_file = tmp_path / "api_key"
    secret_file.write_text("a" * 48 + "\n", encoding="utf-8")
    monkeypatch.setenv("API_AUTH_ENABLED", "true")
    monkeypatch.setenv("API_KEY_FILE", str(secret_file))
    monkeypatch.delenv("API_KEY", raising=False)

    settings = load_settings()

    assert settings.api_auth_enabled is True
    assert settings.api_key == "a" * 48


def test_api_key_sources_are_mutually_exclusive(monkeypatch, tmp_path):
    secret_file = tmp_path / "api_key"
    secret_file.write_text("file-key", encoding="utf-8")
    monkeypatch.setenv("API_AUTH_ENABLED", "true")
    monkeypatch.setenv("API_KEY", "direct-key")
    monkeypatch.setenv("API_KEY_FILE", str(secret_file))

    with pytest.raises(ValueError, match="only one"):
        load_settings()


def test_invalid_auth_boolean_is_rejected(monkeypatch):
    monkeypatch.setenv("API_AUTH_ENABLED", "sometimes")

    with pytest.raises(ValueError, match="invalid boolean"):
        load_settings()
