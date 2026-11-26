from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer


def test_initial_migration_creates_schema_and_indexes(monkeypatch) -> None:
    with PostgresContainer("postgres:16-alpine") as postgres:
        url = make_url(postgres.get_connection_url())
        monkeypatch.setenv("POSTGRES_USER", url.username or "")
        monkeypatch.setenv("POSTGRES_PASSWORD", url.password or "")
        monkeypatch.setenv("POSTGRES_HOST", url.host or "")
        monkeypatch.setenv("POSTGRES_PORT", str(url.port or 5432))
        monkeypatch.setenv("POSTGRES_DB", url.database or "")

        config = Config("alembic.ini")
        command.upgrade(config, "head")

        engine = create_engine(url)
        inspector = inspect(engine)
        assert set(inspector.get_table_names()) >= {
            "alembic_version",
            "readings",
            "predictions",
            "dlq_events",
            "outbox_events",
            "drift_metrics",
        }
        assert {index["name"] for index in inspector.get_indexes("readings")} >= {
            "ix_readings_ingested_at"
        }
        assert {index["name"] for index in inspector.get_indexes("drift_metrics")} >= {
            "ix_drift_metrics_feature_computed_at"
        }
        engine.dispose()
