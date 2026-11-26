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
