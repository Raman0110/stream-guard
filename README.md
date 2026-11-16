# StreamGuard

Real-time anomaly detection pipeline for simulated IoT sensor data, with data-quality
and model-drift monitoring. (Work in progress: milestone 3 of 7.)

## Quick start

```bash
cp .env.example .env     # then set POSTGRES_PASSWORD
docker compose up -d --build   # Redpanda + PostgreSQL + trainer (one-shot) + producer + consumer
docker compose exec postgres psql -U streamguard -c "select count(*) from readings"
```

Inject anomalies and malformed messages (they land in `dlq_events` / topic `sensor-readings-dlq`):

```bash
ANOMALY_RATE=0.05 INVALID_RATE=0.1 docker compose up -d producer
```

The first `up` trains the Isolation Forest automatically (artifacts in `models/`, metrics in
[reports/evaluation.md](reports/evaluation.md)). Every stored reading gets a row in `predictions`:

```bash
docker compose exec postgres psql -U streamguard -c "select is_anomaly, count(*) from predictions group by 1"
```

Retrain manually with `make train`, then `docker compose restart consumer`.

## Development

```bash
python -m venv .venv && .venv/Scripts/activate   # Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt
make lint && make test     # integration tests need Docker running
```
