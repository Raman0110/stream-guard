"""Periodic PostgreSQL worker that compares live features with the training baseline."""

import logging
import signal
from datetime import UTC, datetime
from pathlib import Path
from threading import Event

import numpy as np
import numpy.typing as npt
import pandas as pd
from sqlalchemy import Engine, select

from common.config import load_settings
from common.logging_setup import setup_logging
from common.tables import connect, drift_metrics, readings
from ml.features import FEATURES, RollingFeatures
from monitoring.drift import DriftResult, calculate_drift

FloatArray = npt.NDArray[np.float64]
MIN_LIVE_READINGS = 100
log = logging.getLogger("monitoring")


def load_baseline(path: Path) -> FloatArray:
    """Load and validate the exact feature sample saved during model training."""
    frame = pd.read_csv(path)
    missing = [feature for feature in FEATURES if feature not in frame.columns]
    if missing:
        raise ValueError(f"baseline is missing features: {', '.join(missing)}")
    return np.asarray(frame[FEATURES].to_numpy(), dtype=np.float64)


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
        features.update(sensor, temp, humidity, vibration)
        for sensor, temp, humidity, vibration in rows
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


def run_check(engine: Engine, baseline: FloatArray, window_size: int) -> list[DriftResult]:
    """Run and persist one check, returning no results until enough live data exists."""
    current = load_live_features(engine, window_size)
    if len(current) < MIN_LIVE_READINGS:
        log.info(
            "not enough readings for drift check",
            extra={"available": len(current), "required": MIN_LIVE_READINGS},
        )
        return []
    results = calculate_drift(baseline, current)
    store_results(engine, results, len(current))
    log.info(
        "drift check complete",
        extra={
            "window_size": len(current),
            "drifted_features": [result.feature for result in results if result.drift_detected],
        },
    )
    return results


def main() -> None:
    setup_logging()
    cfg = load_settings()
    engine = connect(cfg.database_url)
    baseline = load_baseline(Path(cfg.baseline_features_path))
    stopped = Event()

    def stop(signum: int, frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    log.info(
        "drift monitor started",
        extra={
            "baseline_rows": len(baseline),
            "window_size": cfg.drift_window_size,
            "interval_s": cfg.drift_check_interval_seconds,
        },
    )
    while not stopped.is_set():
        try:
            run_check(engine, baseline, cfg.drift_window_size)
        except Exception:
            log.exception("drift check failed")
        stopped.wait(cfg.drift_check_interval_seconds)
    log.info("drift monitor stopped")


if __name__ == "__main__":
    main()
