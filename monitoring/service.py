"""Application service for loading baselines and running drift checks."""

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import Engine

from ml.features import FEATURES
from monitoring.drift import DriftResult, FloatArray, calculate_drift
from monitoring.repository import load_live_features, store_results

MIN_LIVE_READINGS = 100
log = logging.getLogger("monitoring")


def load_baseline(path: Path) -> FloatArray:
    """Load and validate the exact feature sample saved during model training."""
    frame = pd.read_csv(path)
    missing = [feature for feature in FEATURES if feature not in frame.columns]
    if missing:
        raise ValueError(f"baseline is missing features: {', '.join(missing)}")
    return np.asarray(frame[FEATURES].to_numpy(), dtype=np.float64)


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
