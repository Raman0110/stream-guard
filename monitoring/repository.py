"""Database reads and writes used by distribution-drift monitoring."""

from datetime import UTC, datetime

import numpy as np
from sqlalchemy import Engine, select

from common.tables import drift_metrics, readings
from ml.features import FEATURES, RollingFeatures
from monitoring.drift import DriftResult, FloatArray


def load_live_features(engine: Engine, limit: int) -> FloatArray:
    """Rebuild rolling features for the newest readings in chronological order."""
    stmt = (
        select(
            readings.c.sensor_id,
            readings.c.temperature,
            readings.c.humidity,
            readings.c.vibration,
        )
        .order_by(readings.c.ts.desc())
        .limit(limit)
    )
    with engine.connect() as conn:
        rows = list(reversed(conn.execute(stmt).all()))

    features = RollingFeatures()
    vectors = [
        features.update(sensor, temperature, humidity, vibration)
        for sensor, temperature, humidity, vibration in rows
    ]
    if not vectors:
        return np.empty((0, len(FEATURES)), dtype=np.float64)
    return np.asarray(vectors, dtype=np.float64)


def store_results(engine: Engine, results: list[DriftResult], window_size: int) -> None:
    """Store a complete drift check as one timestamped database batch."""
    computed_at = datetime.now(UTC)
    rows = [
        {
            "computed_at": computed_at,
            "feature": result.feature,
            "ks_statistic": result.ks_statistic,
            "ks_pvalue": result.ks_pvalue,
            "psi": result.psi,
            "drift_detected": result.drift_detected,
            "window_size": window_size,
        }
        for result in results
    ]
    with engine.begin() as conn:
        conn.execute(drift_metrics.insert(), rows)
