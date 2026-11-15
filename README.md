# StreamGuard

Real-time anomaly detection pipeline for simulated IoT sensor data, with data-quality
and model-drift monitoring. (Work in progress: milestone 2 of 7.)

## Quick start

```bash
cp .env.example .env     # then set POSTGRES_PASSWORD
docker compose up -d --build   # Redpanda + PostgreSQL + producer + consumer
docker compose exec postgres psql -U streamguard -c "select count(*) from readings"
```

Inject anomalies and malformed messages (they land in `dlq_events` / topic `sensor-readings-dlq`):

```bash
ANOMALY_RATE=0.05 INVALID_RATE=0.1 docker compose up -d producer
```

## Development

```bash
python -m venv .venv && .venv/Scripts/activate   # Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt
make lint && make test     # integration tests need Docker running
```
