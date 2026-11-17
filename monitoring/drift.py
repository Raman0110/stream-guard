"""Distribution-drift calculations shared by the worker and unit tests."""

from dataclasses import dataclass

import numpy as np
import numpy.typing as npt
from scipy.stats import ks_2samp

from ml.features import FEATURES

FloatArray = npt.NDArray[np.float64]
KS_STAT_THRESHOLD = 0.15
KS_PVALUE_THRESHOLD = 0.01
PSI_THRESHOLD = 0.20
PSI_BUCKETS = 10
EPSILON = 1e-6


@dataclass(frozen=True)
class DriftResult:
    feature: str
    ks_statistic: float
    ks_pvalue: float
    psi: float
    drift_detected: bool


def population_stability_index(
    baseline: FloatArray, current: FloatArray, buckets: int = PSI_BUCKETS
) -> float:
    """Calculate PSI using quantile buckets learned only from the baseline sample."""
    if baseline.size == 0 or current.size == 0:
        raise ValueError("PSI samples must not be empty")

    # Infinite outer edges ensure every current value belongs to a bucket. Duplicate
    # quantiles are removed because rolling standard deviations often contain zeros.
    quantiles = np.linspace(0, 1, buckets + 1)[1:-1]
    inner_edges = np.unique(np.quantile(baseline, quantiles))
    edges = np.concatenate(([-np.inf], inner_edges, [np.inf]))
    expected, _ = np.histogram(baseline, bins=edges)
    actual, _ = np.histogram(current, bins=edges)
    expected_pct = np.clip(expected / baseline.size, EPSILON, None)
    actual_pct = np.clip(actual / current.size, EPSILON, None)
    return float(np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct)))


def calculate_drift(baseline: FloatArray, current: FloatArray) -> list[DriftResult]:
    """Compare two feature matrices and return one drift decision per feature."""
    expected_shape = len(FEATURES)
    if baseline.ndim != 2 or current.ndim != 2:
        raise ValueError("drift samples must be two-dimensional")
    if baseline.shape[1] != expected_shape or current.shape[1] != expected_shape:
        raise ValueError(f"drift samples must contain {expected_shape} features")
    if len(baseline) == 0 or len(current) == 0:
        raise ValueError("drift samples must not be empty")

    results = []
    for index, feature in enumerate(FEATURES):
        reference = baseline[:, index]
        live = current[:, index]
        ks = ks_2samp(reference, live)
        psi = population_stability_index(reference, live)
        ks_drift = (
            float(ks.statistic) >= KS_STAT_THRESHOLD and float(ks.pvalue) < KS_PVALUE_THRESHOLD
        )
        results.append(
            DriftResult(
                feature=feature,
                ks_statistic=float(ks.statistic),
                ks_pvalue=float(ks.pvalue),
                psi=psi,
                drift_detected=ks_drift or psi >= PSI_THRESHOLD,
            )
        )
    return results
