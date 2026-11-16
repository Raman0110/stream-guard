# StreamGuard

Real-time anomaly detection pipeline for simulated IoT sensor data, with data-quality
monitoring and a live operations dashboard. (Work in progress: milestone 5 of 7.)

## Quick start

```bash
cp .env.example .env     # then set POSTGRES_PASSWORD
docker compose up -d --build   # Redpanda + PostgreSQL + trainer (one-shot) + producer + consumer
docker compose exec postgres psql -U streamguard -c "select count(*) from readings"
```

Open the live dashboard at http://localhost:8501 and API documentation at
http://localhost:8000/docs. The dashboard shows live sensor traces, anomaly alerts,
throughput, latency, dead-letter totals, model status, and an interactive prediction form.

Inject anomalies and malformed messages (they land in `dlq_events` / topic `sensor-readings-dlq`):

```bash
ANOMALY_RATE=0.05 INVALID_RATE=0.1 docker compose up -d producer
```

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
| `GET /metrics/drift` | latest drift scores per feature (filled by milestone 5) |
| `POST /predict` | score one reading on demand |

```bash
curl localhost:8000/metrics/pipeline
curl -X POST localhost:8000/predict -H 'content-type: application/json'   -d '{"sensor_id":"sensor-1","temperature":95,"humidity":50,"vibration":0.5}'
```

Retrain manually with `make train`, then `docker compose restart consumer`.

## Development

```bash
python -m venv .venv && .venv/Scripts/activate   # Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt
make lint && make test     # integration tests need Docker running
```
