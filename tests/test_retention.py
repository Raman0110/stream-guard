from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from testcontainers.postgres import PostgresContainer

from common import tables
from scripts.retention import RetentionPolicy, preview_expired, purge_expired


def _count(engine, table) -> int:
    with engine.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(table)).scalar_one())


def test_retention_preview_and_batched_cleanup():
    with PostgresContainer("postgres:16-alpine") as pg:
        engine = tables.connect(pg.get_connection_url())
        tables.metadata.create_all(engine)
        now = datetime.now(UTC)
        old = now - timedelta(days=120)
        recent = now - timedelta(days=1)
        with engine.begin() as conn:
            reading_ids = list(
                conn.execute(
                    tables.readings.insert()
                    .returning(tables.readings.c.id)
                    .values(
                        [
                            {
                                "sensor_id": "old-1",
                                "ts": old,
                                "temperature": 1,
                                "humidity": 1,
                                "vibration": 1,
                                "ingested_at": old,
                            },
                            {
                                "sensor_id": "old-2",
                                "ts": old + timedelta(seconds=1),
                                "temperature": 1,
                                "humidity": 1,
                                "vibration": 1,
                                "ingested_at": old,
                            },
                            {
                                "sensor_id": "recent",
                                "ts": recent,
                                "temperature": 1,
                                "humidity": 1,
                                "vibration": 1,
                                "ingested_at": recent,
                            },
                        ]
                    )
                ).scalars()
            )
            conn.execute(
                tables.predictions.insert(),
                [
                    {
                        "reading_id": row_id,
                        "anomaly_score": 0.1,
                        "is_anomaly": False,
                        "model_version": "test",
                    }
                    for row_id in reading_ids
                ],
            )
            conn.execute(
                tables.dlq_events.insert(),
                [
                    {"received_at": old, "raw_payload": "old", "error": "bad"},
                    {"received_at": recent, "raw_payload": "recent", "error": "bad"},
                ],
            )
            conn.execute(
                tables.drift_metrics.insert(),
                [
                    {
                        "computed_at": old,
                        "feature": "temperature",
                        "ks_statistic": 0.1,
                        "ks_pvalue": 0.5,
                        "psi": 0.1,
                        "drift_detected": False,
                        "window_size": 10,
                    },
                    {
                        "computed_at": recent,
                        "feature": "temperature",
                        "ks_statistic": 0.1,
                        "ks_pvalue": 0.5,
                        "psi": 0.1,
                        "drift_detected": False,
                        "window_size": 10,
                    },
                ],
            )
            conn.execute(
                tables.outbox_events.insert(),
                [
                    {
                        "topic": "dlq",
                        "payload": b"published-old",
                        "created_at": old,
                        "published_at": old,
                    },
                    {
                        "topic": "dlq",
                        "payload": b"pending-old",
                        "created_at": old,
                        "published_at": None,
                    },
                    {
                        "topic": "dlq",
                        "payload": b"published-recent",
                        "created_at": recent,
                        "published_at": recent,
                    },
                ],
            )

        policy = RetentionPolicy(90, 30, 90, 7, batch_size=1)
        assert preview_expired(engine, policy, now=now) == {
            "readings": 2,
            "dlq_events": 1,
            "drift_metrics": 1,
            "outbox_events": 1,
        }
        assert purge_expired(engine, policy, now=now) == {
            "readings": 2,
            "dlq_events": 1,
            "drift_metrics": 1,
            "outbox_events": 1,
        }
        assert _count(engine, tables.readings) == 1
        assert _count(engine, tables.predictions) == 1
        assert _count(engine, tables.dlq_events) == 1
        assert _count(engine, tables.drift_metrics) == 1
        assert _count(engine, tables.outbox_events) == 2
        assert preview_expired(engine, policy, now=now) == {
            "readings": 0,
            "dlq_events": 0,
            "drift_metrics": 0,
            "outbox_events": 0,
        }
        engine.dispose()
