# StreamGuard

Real-time anomaly detection pipeline for simulated IoT sensor data, with data-quality
and model-drift monitoring. (Work in progress: milestone 1 of 7.)

## Quick start (infrastructure only, for now)

```bash
cp .env.example .env     # then set POSTGRES_PASSWORD
docker compose up -d     # Redpanda + PostgreSQL
```
