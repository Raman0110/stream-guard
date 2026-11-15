from common.config import load_settings


def test_defaults(monkeypatch):
    for name in ("NUM_SENSORS", "KAFKA_TOPIC", "POSTGRES_PORT"):
        monkeypatch.delenv(name, raising=False)
    s = load_settings()
    assert s.num_sensors == 5
    assert s.kafka_topic == "sensor-readings"
    assert s.postgres_port == 5432


def test_env_override_and_database_url(monkeypatch):
    monkeypatch.setenv("NUM_SENSORS", "8")
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_DB", "d")
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    s = load_settings()
    assert s.num_sensors == 8
    assert s.database_url == "postgresql+psycopg2://u:p@db:6543/d"
