from datetime import UTC, datetime, timedelta

import numpy as np
from sqlalchemy import create_engine, func, select

from common.tables import drift_metrics, metadata, readings
from ml.features import FEATURES, WINDOW
from monitoring.repository import load_live_features
from monitoring.service import run_check
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
    assert live.shape == (120, len(FEATURES))  # 150 readings minus warm-up, capped at the limit

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


def _store_simulated(engine, count: int, sensors: int = 5) -> None:
    simulator = SensorSimulator(num_sensors=sensors, seed=7)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = []
    for index in range(count):
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


def test_live_features_exclude_incomplete_warmup_windows() -> None:
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    _store_simulated(engine, count=100, sensors=5)

    live = load_live_features(engine, limit=1000)

    # Each of the 5 sensors loses its first WINDOW-1 readings to warm-up.
    assert live.shape[0] == 100 - 5 * (WINDOW - 1)
    std_columns = live[:, 6:]  # temperature_std, humidity_std, vibration_std
    assert (std_columns > 0).all()  # a lone first reading would have std exactly 0


def test_live_features_return_at_most_the_requested_window() -> None:
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    _store_simulated(engine, count=200)
    assert load_live_features(engine, limit=50).shape == (50, len(FEATURES))


def test_live_features_empty_database() -> None:
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    assert load_live_features(engine, limit=100).shape == (0, len(FEATURES))
