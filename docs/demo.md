# Interview demo runbook

This is a five-minute path through StreamGuard. Start the stack a minute or two before presenting
so the dashboard already contains telemetry, anomalies, dead letters, and a drift decision.

## Before the interview

```bash
docker compose -f docker-compose.yml -f docker-compose.demo.yml up -d --build
docker compose ps
```

Open these tabs:

1. Dashboard: http://localhost:8501
2. Swagger UI: http://localhost:8000/docs
3. Repository: `docker-compose.yml` and `consumer/main.py`

## Presentation flow

### 0:00–0:45 — Problem and architecture

“StreamGuard is a real-time IoT anomaly-detection pipeline. A simulator publishes readings to
Redpanda, a consumer validates and scores them with a versioned Isolation Forest, PostgreSQL stores
the results, and FastAPI plus Streamlit expose the system.”

Show `docker compose ps`, then briefly show the service definitions in `docker-compose.yml`.

### 0:45–2:00 — Live pipeline

Open the dashboard. Point out:

- the reading counter and approximately 10 events/second throughput;
- sub-second processing latency;
- red anomaly markers and the recent-alert table;
- the increasing dead-letter count from intentionally malformed events.

Explain that schema-invalid data goes to the DLQ, while valid but unusual data reaches the ML model.

### 2:00–2:45 — Reliability

Open `consumer/main.py`. Show that Kafka auto-commit is disabled and offsets are committed only after
database writes. Then mention the `(sensor_id, timestamp)` uniqueness constraint. Together these give
at-least-once delivery with idempotent processing.

### 2:45–3:30 — Machine learning

Explain the nine features: three raw measurements, three rolling means, and three rolling standard
deviations. The five-reading buffer is independent per sensor. The Isolation Forest is trained on
clean synthetic data, and the anomaly threshold is the 99th percentile of clean training scores.

Use the dashboard form to submit an extreme reading such as temperature `95`, humidity `50`, and
vibration `0.5`. Show the returned score, threshold, model version, and anomaly decision.

### 3:30–4:15 — Drift monitoring

Scroll to Distribution drift. Explain that the worker compares the newest 1,000 feature vectors with
the saved training baseline using KS distance and PSI. Demo mode gradually shifts sensor means, so the
state moves from `BASELINE STABLE` to `DRIFT DETECTED`.

### 4:15–5:00 — API, tests, and limitations

Open Swagger and execute `/health` or `/metrics/pipeline`. Finish with:

“The project has unit, API, model, statistical, and Docker integration coverage. The current suite has
77 passing tests. The data is synthetic, so I treat the model metrics as pipeline validation rather
than production accuracy.”

If asked what comes next, mention Kafka partitioning, rolling-state restoration, Alembic migrations,
authentication, external secrets, and cloud deployment.

## Useful verification commands

```bash
docker compose logs --tail 20 consumer monitoring
docker compose exec postgres psql -U streamguard -c "select count(*) from readings"
docker compose exec postgres psql -U streamguard -c "select count(*) from dlq_events"
python -m pytest -q
```

Return to normal, non-demo traffic with:

```bash
docker compose up -d
```
