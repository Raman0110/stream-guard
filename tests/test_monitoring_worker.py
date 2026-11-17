from datetime import UTC, datetime, timedelta

import numpy as np
from sqlalchemy import create_engine, func, select

from common.tables import drift_metrics, metadata, readings
from ml.features import FEATURES
from monitoring.main import load_live_features, run_check
from producer.simulator import SensorSimulator


def test_worker_rebuilds_features_and_stores_complete_check() -> None:
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    simulator = SensorSimulator(num_sensors=5, seed=42)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = []
    for index in range(150):
        reading = simulator.next_reading(start + timedelta(seconds=index))
        rows.append(
            {
                "sensor_id": reading.sensor_id,
                "ts": reading.timestamp,
                "temperature": reading.temperature,
                "humidity": reading.humidity,
                "vibration": reading.vibration,
            }
        )
    with engine.begin() as conn:
        conn.execute(readings.insert(), rows)

    live = load_live_features(engine, limit=120)
    assert live.shape == (120, len(FEATURES))

    results = run_check(engine, live.copy(), window_size=120)
    assert len(results) == len(FEATURES)
    assert not any(result.drift_detected for result in results)
    with engine.connect() as conn:
        stored = conn.execute(select(func.count()).select_from(drift_metrics)).scalar_one()
    assert stored == len(FEATURES)


def test_worker_waits_for_minimum_live_window() -> None:
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    baseline = np.ones((100, len(FEATURES)))
    assert run_check(engine, baseline, window_size=100) == []
