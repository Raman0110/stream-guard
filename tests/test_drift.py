import numpy as np
import pytest

from ml.features import FEATURES
from monitoring.drift import calculate_drift, population_stability_index


def sample(seed: int, rows: int = 1000) -> np.ndarray:
    return np.random.default_rng(seed).normal(size=(rows, len(FEATURES)))


def test_psi_is_zero_for_identical_samples() -> None:
    values = np.linspace(-3, 3, 1000)
    assert population_stability_index(values, values) == pytest.approx(0.0)


def test_psi_detects_shift() -> None:
    baseline = np.random.default_rng(1).normal(size=2000)
    shifted = np.random.default_rng(2).normal(loc=2.0, size=1000)
    assert population_stability_index(baseline, shifted) > 0.2


def test_calculate_drift_is_stable_for_same_distribution() -> None:
    baseline = sample(1, 5000)
    current = sample(2)
    results = calculate_drift(baseline, current)
    assert [result.feature for result in results] == FEATURES
    assert not any(result.drift_detected for result in results)


def test_calculate_drift_flags_shifted_features() -> None:
    baseline = sample(1, 5000)
    current = sample(2)
    current[:, 0] += 2.0
    current[:, 4] += 1.5
    results = calculate_drift(baseline, current)
    flags = {result.feature: result.drift_detected for result in results}
    assert flags["temperature"]
    assert flags["humidity_mean"]
    assert not flags["vibration"]


def test_calculate_drift_validates_shape() -> None:
    with pytest.raises(ValueError, match="9 features"):
        calculate_drift(np.ones((10, 8)), np.ones((10, 8)))
