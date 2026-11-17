# StreamGuard

Real-time anomaly detection pipeline for simulated IoT sensor data, with data-quality and
distribution-drift monitoring plus a live operations dashboard.

## Quick start

```bash
cp .env.example .env     # then set POSTGRES_PASSWORD
docker compose up -d --build   # start the complete normal-data stack
docker compose exec postgres psql -U streamguard -c "select count(*) from readings"
```

Open the live dashboard at http://localhost:8501 and API documentation at
http://localhost:8000/docs. The dashboard shows live sensor traces, anomaly alerts,
throughput, latency, dead-letter totals, model status, and an interactive prediction form.

## Interview demo mode

Start the whole stack with visible anomalies, malformed messages, and gradual drift:

```bash
docker compose -f docker-compose.yml -f docker-compose.demo.yml up -d --build
# or: make demo
```

The reading and prediction counters begin moving immediately. Anomaly markers and dead-letter
counts appear as the injected events arrive. The drift worker evaluates the newest 1,000 readings
every 10 seconds in demo mode, so its status changes after enough shifted readings accumulate.

Return to the normal-data profile with `docker compose up -d`.

The first `up` trains the Isolation Forest automatically (artifacts in `models/`, metrics in
[reports/evaluation.md](reports/evaluation.md)). Every stored reading gets a row in `predictions`:

```bash
docker compose exec postgres psql -U streamguard -c "select is_anomaly, count(*) from predictions group by 1"
```

## API

Interactive docs (OpenAPI): http://localhost:8000/docs

| Endpoint | Purpose |
|---|---|
| `GET /health` | database + model status |
| `GET /readings/recent?sensor_id=&limit=` | latest readings with scores |
| `GET /anomalies?since=` | flagged readings (default: last 24 h) |
| `GET /metrics/pipeline?window_seconds=` | throughput, p50/p95 latency, DLQ count |
| `GET /metrics/drift` | latest KS/PSI drift scores per feature |
| `POST /predict` | score one reading on demand |

```bash
curl localhost:8000/metrics/pipeline
curl -X POST localhost:8000/predict -H 'content-type: application/json'   -d '{"sensor_id":"sensor-1","temperature":95,"humidity":50,"vibration":0.5}'
```

Retrain manually with `make train`, then `docker compose restart consumer`.

## Drift monitoring

The trainer saves 5,000 clean feature vectors as `models/baseline_features.csv`. The monitoring
worker rebuilds the same nine rolling features from recent PostgreSQL readings and compares each
feature with that baseline. A feature is marked as drifted when either PSI is at least 0.20 or the
KS distance is at least 0.15 with a p-value below 0.01. Results are stored in `drift_metrics` and
served by `/metrics/drift`.

See [docs/architecture.md](docs/architecture.md) for the full data flow and reliability model, and
[docs/demo.md](docs/demo.md) for the five-minute interview walkthrough.

## Development

```bash
python -m venv .venv && .venv/Scripts/activate   # Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt
make lint && make test     # integration tests need Docker running
```
