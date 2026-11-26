# Operational walkthrough

This is a five-minute path through StreamGuard. Start the stack a minute or two before reviewing it
so the dashboard already contains telemetry, anomalies, dead letters, and a drift decision.

## Start the fault-injection scenario

```bash
docker compose -f docker-compose.yml -f docker-compose.scenario.yml up -d --build
docker compose ps
```

Open these pages:

1. Dashboard: http://localhost:8501
2. Swagger UI: http://localhost:8000/docs
3. Repository files: `docker-compose.yml` and `consumer/main.py`

## System review flow

### 0:00-0:45 - Problem and architecture

StreamGuard is a real-time IoT anomaly-detection pipeline. A simulator publishes readings to
Redpanda, a consumer validates and scores them with a versioned Isolation Forest, PostgreSQL stores
the results, and FastAPI plus Streamlit expose the system.

Show `docker compose ps`, then briefly review the service definitions in `docker-compose.yml`.

### 0:45-2:00 - Live pipeline

Open the dashboard and observe:

- the reading counter and approximately 10 events/second throughput;
- sub-second processing latency;
- red anomaly markers and the recent-alert table;
- the increasing dead-letter count from intentionally malformed events.

Schema-invalid data goes to the DLQ, while valid but unusual data reaches the ML model.

### 2:00-2:45 - Reliability

Open `consumer/service.py`. Kafka auto-commit is disabled and offsets are committed only after
database writes. Existing readings are filtered before feature calculation, rolling state is
restored on startup, and tentative state is applied only after a successful transaction. Invalid
payloads are persisted with an outbox event so Kafka DLQ publication can be retried safely.

### 2:45-3:30 - Machine learning

Review the nine features: three raw measurements, three rolling means, and three rolling standard
deviations. The five-reading buffer is independent per sensor. The Isolation Forest is trained on
clean synthetic data, and its threshold is the 99th percentile of clean training scores.

Use the dashboard form to submit an extreme reading such as temperature `95`, humidity `50`, and
vibration `0.5`. Observe the returned score, threshold, model version, and anomaly decision.

### 3:30-4:15 - Drift monitoring

Scroll to Distribution drift. The worker compares the newest 1,000 feature vectors with the saved
training baseline using KS distance and PSI. The scenario gradually shifts sensor means, so the state
moves from `BASELINE STABLE` to `DRIFT DETECTED`.

### 4:15-5:00 - API, tests, and limitations

Open Swagger and execute `/health` or `/metrics/pipeline`. The project has unit, API, model,
statistical, and Docker integration coverage. The data is synthetic, so model metrics validate the
controlled pipeline behavior rather than production accuracy on real equipment.

Potential extensions include Kafka partitioning, authentication, external secrets, automated
backup and retention policies, and cloud deployment.

## Useful verification commands

```bash
docker compose logs --tail 20 consumer monitoring
docker compose logs migrate
docker compose exec postgres psql -U streamguard -c "select count(*) from readings"
docker compose exec postgres psql -U streamguard -c "select count(*) from dlq_events"
docker compose exec postgres psql -U streamguard -c "select version_num from alembic_version"
python -m pytest -q
```

Return to normal traffic with:

```bash
docker compose up -d
```
