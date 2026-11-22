"""Database reads and writes used by distribution-drift monitoring."""

from datetime import UTC, datetime

import numpy as np
from sqlalchemy import Engine, select

from common.tables import drift_metrics, readings
from ml.features import FEATURES, WINDOW, RollingFeatures
from monitoring.drift import DriftResult, FloatArray

# Extra rows fetched so that each sensor's incomplete warm-up readings can be discarded while
# still returning `limit` complete feature vectors (enough for roughly 20 sensors).
WARMUP_MARGIN = 20 * WINDOW


def load_live_features(engine: Engine, limit: int) -> FloatArray:
    """Rebuild rolling features for the newest readings, in chronological order.

    Each sensor's first WINDOW-1 readings in the fetched range have an incomplete rolling window
    (for example a standard deviation of exactly 0), which the model never sees in steady state,
    so those vectors are dropped. At most `limit` vectors are returned.
    """
    stmt = (
        select(
            readings.c.sensor_id,
            readings.c.temperature,
            readings.c.humidity,
            readings.c.vibration,
        )
        .order_by(readings.c.ts.desc())
        .limit(limit + WARMUP_MARGIN)
    )
    with engine.connect() as conn:
        rows = list(reversed(conn.execute(stmt).all()))

    features = RollingFeatures()
    seen: dict[str, int] = {}
    vectors = []
    for sensor, temperature, humidity, vibration in rows:
        vector = features.update(sensor, temperature, humidity, vibration)
        seen[sensor] = seen.get(sensor, 0) + 1
        if seen[sensor] >= WINDOW:  # this sensor's window is now full
            vectors.append(vector)
    if not vectors:
        return np.empty((0, len(FEATURES)), dtype=np.float64)
    return np.asarray(vectors[-limit:], dtype=np.float64)


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
