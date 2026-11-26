"""API tests: FastAPI TestClient against a real (throwaway) PostgreSQL container."""

from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from api.main import create_app
from common import tables
from ml import train
from ml.model import Scorer, train_model

pytestmark = pytest.mark.integration  # needs Docker for the Postgres container

NOW = datetime.now(UTC)


@pytest.fixture(scope="module")
def engine():
    with PostgresContainer("postgres:16-alpine") as pg:
        yield tables.connect(pg.get_connection_url())


@pytest.fixture(scope="module")
def scorer():
    x, _ = train.make_dataset(3000, seed=1, anomaly_rate=0.0)
    return Scorer(train_model(x, seed=1))


@pytest.fixture
def client(engine, scorer):
    with engine.begin() as conn:  # every test starts from empty tables
        conn.execute(
            text(
                "TRUNCATE readings, predictions, dlq_events, outbox_events, drift_metrics "
                "RESTART IDENTITY CASCADE"
            )
        )
    with TestClient(create_app(engine, scorer)) as c:
        yield c


def seed_reading(engine, sensor="s1", age_s=10.0, anomaly=False, temp=25.0, latency_s=1.0):
    """Insert one reading + prediction. `age_s` = how long ago the event happened."""
    ts = NOW - timedelta(seconds=age_s)
    with engine.begin() as conn:
        rid = conn.execute(
            tables.readings.insert()
            .values(
                sensor_id=sensor,
                ts=ts,
                temperature=temp,
                humidity=50.0,
                vibration=0.5,
                ingested_at=NOW - timedelta(seconds=age_s - latency_s),
            )  # fmt: skip
            .returning(tables.readings.c.id)
        ).scalar_one()
        conn.execute(
            tables.predictions.insert().values(
                reading_id=rid,
                anomaly_score=0.9 if anomaly else 0.4,
                is_anomaly=anomaly,
                model_version="test",
                scored_at=ts + timedelta(seconds=latency_s),
            )  # fmt: skip
        )


# ---------- health & docs ----------


def test_health_ok(client, scorer):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "database": "ok", "model_version": scorer.version}


def test_health_503_when_database_down(scorer):
    dead = create_engine("postgresql+psycopg2://x:y@127.0.0.1:1/none")
    with TestClient(create_app(dead, scorer)) as c:
        assert c.get("/health").status_code == 503


def test_openapi_docs_enabled(client):
    assert client.get("/docs").status_code == 200
    paths = client.get("/openapi.json").json()["paths"]
    for p in ("/health", "/readings/recent", "/anomalies", "/metrics/pipeline",
              "/metrics/drift", "/predict"):  # fmt: skip
        assert p in paths


# ---------- /readings/recent ----------


def test_recent_newest_first_and_limit(client, engine):
    for age in (30, 20, 10):
        seed_reading(engine, age_s=age)
    rows = client.get("/readings/recent", params={"limit": 2}).json()
    assert len(rows) == 2
    assert rows[0]["ts"] > rows[1]["ts"]
    assert {"sensor_id", "temperature", "anomaly_score", "is_anomaly"} <= rows[0].keys()


def test_recent_filters_by_sensor(client, engine):
    seed_reading(engine, sensor="a")
    seed_reading(engine, sensor="b", age_s=5)
    rows = client.get("/readings/recent", params={"sensor_id": "a"}).json()
    assert [r["sensor_id"] for r in rows] == ["a"]


@pytest.mark.parametrize("limit", [0, 1001, -5])
def test_recent_rejects_bad_limit(client, limit):
    assert client.get("/readings/recent", params={"limit": limit}).status_code == 422


def test_recent_empty(client):
    assert client.get("/readings/recent").json() == []


# ---------- /anomalies ----------


def test_anomalies_only_flagged(client, engine):
    seed_reading(engine, anomaly=False, age_s=20)
    seed_reading(engine, anomaly=True, age_s=10, temp=99.0)
    rows = client.get("/anomalies").json()
    assert len(rows) == 1 and rows[0]["is_anomaly"] and rows[0]["temperature"] == 99.0


def test_anomalies_since_filter(client, engine):
    seed_reading(engine, anomaly=True, age_s=3600)
    seed_reading(engine, anomaly=True, age_s=10)
    since = (NOW - timedelta(minutes=5)).isoformat()
    assert len(client.get("/anomalies", params={"since": since}).json()) == 1
    assert len(client.get("/anomalies").json()) == 2  # default window is 24h


def test_anomalies_naive_timestamp_treated_as_utc(client, engine):
    seed_reading(engine, anomaly=True, age_s=10)
    naive = (NOW - timedelta(minutes=5)).replace(tzinfo=None).isoformat()
    assert len(client.get("/anomalies", params={"since": naive}).json()) == 1


def test_anomalies_invalid_since(client):
    assert client.get("/anomalies", params={"since": "yesterday"}).status_code == 422


# ---------- /metrics/pipeline ----------


def test_pipeline_metrics_empty(client):
    m = client.get("/metrics/pipeline").json()
    assert m["readings_in_window"] == 0 and m["throughput_per_sec"] == 0
    assert m["latency_p50_s"] is None and m["latency_p95_s"] is None
    assert m["dlq_total"] == 0


def test_pipeline_metrics_values(client, engine):
    for i in range(6):
        seed_reading(engine, age_s=10 + i, latency_s=2.0, anomaly=(i == 0))
    seed_reading(engine, age_s=500, latency_s=2.0)  # outside the 60 s window
    with engine.begin() as conn:
        conn.execute(tables.dlq_events.insert().values(raw_payload="x", error="bad"))
    m = client.get("/metrics/pipeline", params={"window_seconds": 60}).json()
    assert m["readings_in_window"] == 6
    assert m["throughput_per_sec"] == pytest.approx(6 / 60)
    assert m["latency_p50_s"] == pytest.approx(2.0, abs=0.01)
    assert m["latency_p95_s"] == pytest.approx(2.0, abs=0.01)
    assert m["anomalies_in_window"] == 1
    assert m["readings_total"] == 7 and m["dlq_total"] == 1


# ---------- /metrics/drift ----------


def seed_drift(engine, feature, drifted, minutes_ago):
    with engine.begin() as conn:
        conn.execute(
            tables.drift_metrics.insert().values(
                computed_at=NOW - timedelta(minutes=minutes_ago),
                feature=feature,
                ks_statistic=0.5,
                ks_pvalue=0.001,
                psi=0.4,
                drift_detected=drifted,
                window_size=500,
            )  # fmt: skip
        )


def test_drift_no_data(client):
    assert client.get("/metrics/drift").json() == {
        "status": "no_data", "computed_at": None, "features": [],
    }  # fmt: skip


def test_drift_uses_latest_row_per_feature(client, engine):
    seed_drift(engine, "temperature", drifted=True, minutes_ago=30)  # old: drifted
    seed_drift(engine, "temperature", drifted=False, minutes_ago=1)  # latest: fine
    seed_drift(engine, "humidity", drifted=False, minutes_ago=1)
    body = client.get("/metrics/drift").json()
    assert body["status"] == "ok"
    assert {f["feature"] for f in body["features"]} == {"temperature", "humidity"}


def test_drift_status_flags_when_any_feature_drifted(client, engine):
    seed_drift(engine, "temperature", drifted=True, minutes_ago=1)
    seed_drift(engine, "humidity", drifted=False, minutes_ago=1)
    assert client.get("/metrics/drift").json()["status"] == "drift"


# ---------- POST /predict ----------

NORMAL = {"sensor_id": "s1", "temperature": 25.0, "humidity": 50.0, "vibration": 0.5}


def test_predict_normal_reading(client, scorer):
    r = client.post("/predict", json=NORMAL)
    assert r.status_code == 200
    body = r.json()
    assert body["is_anomaly"] is False
    assert body["model_version"] == scorer.version
    assert body["history_readings_used"] == 0


def test_predict_extreme_reading_is_flagged(client):
    body = client.post("/predict", json={**NORMAL, "temperature": 90.0, "vibration": 8.0}).json()
    assert body["is_anomaly"] is True
    assert body["anomaly_score"] > body["threshold"]


def test_predict_uses_stored_history(client, engine):
    for age in (40, 30, 20):
        seed_reading(engine, sensor="s1", age_s=age)
    assert client.post("/predict", json=NORMAL).json()["history_readings_used"] == 3


@pytest.mark.parametrize(
    "bad",
    [{"temperature": 999}, {"humidity": -1}, {"sensor_id": ""}, {"extra": 1}],
)
def test_predict_validates_input(client, bad):
    assert client.post("/predict", json={**NORMAL, **bad}).status_code == 422


def test_predict_missing_field(client):
    body = {k: v for k, v in NORMAL.items() if k != "humidity"}
    assert client.post("/predict", json=body).status_code == 422
