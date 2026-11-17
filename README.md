# StreamGuard

StreamGuard is a containerized, real-time anomaly-detection pipeline for IoT sensor data. It
simulates measurements from industrial equipment, streams them through Redpanda using the Kafka
protocol, validates and scores them with a versioned Isolation Forest model, stores the results in
PostgreSQL, monitors distribution drift, and exposes the system through FastAPI and Streamlit.

## What the system analyzes

The simulator represents sensors attached to equipment such as a motor, pump, or manufacturing
machine. Each sensor produces three measurements:

| Measurement | Normal simulated behavior | Example operational meaning |
|---|---:|---|
| Temperature | approximately 25 °C | overheating or abnormal cooling |
| Humidity | approximately 50% | changing environmental conditions |
| Vibration | approximately 0.5 mm/s | imbalance, looseness, or mechanical wear |

These values naturally fluctuate, so one small change is not automatically a fault. StreamGuard
analyzes both the current values and their recent behavior to answer two different questions:

1. **Is this particular reading or recent sensor pattern unusual?** This is anomaly detection.
2. **Has the overall live-data distribution moved away from the training baseline?** This is drift
   detection.

### Sudden anomalies

A sharp temperature, humidity, or vibration change can represent abnormal equipment behavior. The
Isolation Forest assigns every valid reading an anomaly score. A reading is flagged when its score
is higher than the threshold learned from clean training data.

For example, a temperature near 25 °C may receive a score below the threshold, while a temperature
of 95 °C may be flagged as anomalous. The model considers all nine input features together rather
than applying a single hard-coded limit to each measurement.

### Stuck sensors

A repeated value can be suspicious even when the value itself looks normal:

```text
vibration: 0.51, 0.51, 0.51, 0.51, 0.51
```

StreamGuard maintains an independent five-reading window for every sensor. It calculates the rolling
mean and standard deviation of each measurement. A stuck sensor produces very low rolling variation,
giving the model information that would not be visible from one reading alone.

### Gradual distribution drift

A sensor population can change slowly without producing one extreme event:

```text
temperature: 25.0 → 25.3 → 25.7 → 26.2 → 27.0 → 28.5
```

The monitoring worker compares the newest live feature vectors with the clean training baseline. It
calculates the two-sample Kolmogorov-Smirnov statistic and Population Stability Index for every
feature. This identifies conditions where the model's current inputs no longer resemble the data on
which it was trained.

### Invalid data versus anomalous data

Data-quality failures and ML anomalies follow different paths:

| Input | Classification | Handling |
|---|---|---|
| `temperature = 95.0` | Valid but unusual | Sent to the model and potentially flagged |
| `temperature = "hot"` | Invalid type | Sent to the dead-letter queue |
| Missing `sensor_id` | Invalid schema | Sent to the dead-letter queue |
| Non-JSON payload | Invalid message | Sent to the dead-letter queue |

This separation ensures that unusual numeric values reach the anomaly detector while malformed data
does not contaminate model processing.

## Data flow

```text
Sensor simulator
      │
      ▼
Kafka producer ──► Redpanda topic ──► Consumer
                                         │
                         ┌───────────────┴────────────────┐
                         │                                │
                    valid reading                   invalid message
                         │                                │
                         ▼                                ▼
              rolling feature calculation        dead-letter topic
                         │                         + dlq_events table
                         ▼
              Isolation Forest scoring
                         │
                         ▼
             PostgreSQL readings/predictions
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
        FastAPI service       Drift monitor
              │                     │
              └──────────┬──────────┘
                         ▼
                Streamlit dashboard
```

The simulator replaces physical hardware for local development. It can be replaced with a real
Kafka-producing IoT source without redesigning the consumer, model, database, API, monitoring, or
dashboard layers.

## Feature engineering and model

Every reading is transformed into nine model features:

```text
temperature, humidity, vibration,
temperature_mean, humidity_mean, vibration_mean,
temperature_std, humidity_std, vibration_std
```

Training and live inference use the same `RollingFeatures` implementation to prevent
training-serving skew. The model artifact contains the fitted Isolation Forest, anomaly threshold,
model version, feature names, window size, training metadata, and baseline statistics.

The default training process uses 20,000 clean simulated readings. The threshold is the 99th
percentile of anomaly scores from that clean data. Evaluation uses a separate 10,000-reading dataset
with injected spikes and stuck-value sequences. Current synthetic evaluation results are documented
in [reports/evaluation.md](reports/evaluation.md). These results validate system behavior on
controlled data; they are not claims of production accuracy on real equipment.

## Components

| Service | Responsibility |
|---|---|
| Producer | Generates sensor readings and publishes them to Redpanda |
| Redpanda | Provides Kafka-compatible event streaming and offset tracking |
| Consumer | Validates, creates features, scores, and persists events |
| Trainer | Builds and evaluates the model and writes its artifacts |
| PostgreSQL | Stores readings, predictions, rejected events, and drift metrics |
| Monitoring worker | Calculates KS and PSI drift statistics periodically |
| FastAPI | Exposes readings, anomalies, metrics, health, and prediction endpoints |
| Streamlit | Displays live telemetry and operational status |

## Processing guarantees

The consumer uses at-least-once processing:

- Kafka automatic offset commits are disabled.
- A batch is written to PostgreSQL before its offset is committed.
- Readings and their predictions are stored in one transaction.
- A unique `(sensor_id, timestamp)` constraint makes redelivery idempotent.
- Invalid events are retained in both the Kafka dead-letter topic and PostgreSQL.

If the consumer crashes after a database write but before committing its offset, Kafka redelivers the
message. PostgreSQL safely ignores the duplicate while preserving the original prediction.

## Quick start

Create the local environment file and start the complete stack:

```bash
cp .env.example .env
# Set POSTGRES_PASSWORD in .env
docker compose up -d --build
```

Open:

- Dashboard: http://localhost:8501
- API documentation: http://localhost:8000/docs

Confirm that readings and predictions are being stored:

```bash
docker compose exec postgres psql -U streamguard -c \
  "select is_anomaly, count(*) from predictions group by is_anomaly"
```

## Configuring generated conditions

The producer behavior is controlled through `.env`:

```dotenv
NUM_SENSORS=5
EVENTS_PER_SECOND=10
RANDOM_SEED=42
ANOMALY_RATE=0.04
INVALID_RATE=0.02
DRIFT_PER_READING=0.001
```

- `ANOMALY_RATE` controls the probability of spikes or stuck-value sequences.
- `INVALID_RATE` controls the probability of malformed messages.
- `DRIFT_PER_READING` controls the gradual shift applied to sensor means.
- Set all three values to `0` for normal-data generation.

Recreate the producer after changing these values:

```bash
docker compose up -d --force-recreate producer
```

## Dashboard

The dashboard refreshes automatically and displays:

- live temperature, humidity, and vibration traces;
- highlighted anomaly locations and recent alerts;
- throughput and total processed readings;
- P50 and P95 end-to-end latency;
- dead-letter totals;
- model version and pipeline health;
- KS/PSI drift status for every feature;
- an interactive form for scoring a custom reading.

## API

| Endpoint | Purpose |
|---|---|
| `GET /health` | Verify database connectivity and loaded model version |
| `GET /readings/recent?sensor_id=&limit=` | Return recent readings with scores |
| `GET /anomalies?since=` | Return flagged readings from a selected period |
| `GET /metrics/pipeline?window_seconds=` | Return throughput, latency, anomaly, and DLQ metrics |
| `GET /metrics/drift` | Return the newest KS/PSI result for every feature |
| `POST /predict` | Score one reading without storing it |

Example prediction:

```bash
curl -X POST http://localhost:8000/predict \
  -H "content-type: application/json" \
  -d '{"sensor_id":"sensor-1","temperature":95,"humidity":50,"vibration":0.5}'
```

## Drift monitoring

Training saves 5,000 clean feature vectors to `models/baseline_features.csv`. The monitoring worker
rebuilds the same nine features from the newest PostgreSQL readings and compares them with this
baseline. A feature is marked as drifted when either:

- PSI is at least `0.20`; or
- KS distance is at least `0.15` and its p-value is below `0.01`.

Results are written to the `drift_metrics` table and served through `/metrics/drift`.

## Development and testing

```bash
python -m venv .venv
.venv/Scripts/activate
pip install -r requirements-dev.txt
make lint
make test
```

The test suite covers simulation, schemas, rolling features, model training and scoring, consumer
processing, duplicate handling, dead-letter routing, API behavior, drift statistics, dashboard API
access, and container-backed integration. Docker must be running for tests that use Testcontainers.

See [docs/architecture.md](docs/architecture.md) for more detail about service boundaries, storage,
and delivery guarantees. Follow [docs/walkthrough.md](docs/walkthrough.md) for a concise operational
review of the running system.
