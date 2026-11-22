"""Simple z-score baselines to compare the Isolation Forest against.

A z-score detector measures how many standard deviations a value is from its training mean and
flags a reading when its largest |z| exceeds a threshold. To make the comparison fair, the
threshold is calibrated exactly like the Isolation Forest's: the same quantile (99%) of the scores
on the clean training data, so every method starts with the same ~1% false-alarm rate.
"""

import numpy as np
import numpy.typing as npt

from ml.model import THRESHOLD_QUANTILE

FloatArray = npt.NDArray[np.float64]
BoolArray = npt.NDArray[np.bool_]


class ZScoreDetector:
    """Flags a reading when any selected feature is unusually far from its training mean."""

    def __init__(self, columns: list[int] | None = None) -> None:
        self.columns = columns  # None = all columns
        self.mean: FloatArray = np.array([])
        self.std: FloatArray = np.array([])
        self.threshold = 0.0

    def _select(self, x: FloatArray) -> FloatArray:
        return x if self.columns is None else x[:, self.columns]

    def fit(self, x_train: FloatArray) -> "ZScoreDetector":
        selected = self._select(x_train)
        self.mean = selected.mean(axis=0)
        self.std = np.maximum(selected.std(axis=0), 1e-9)  # avoid dividing by zero
        self.threshold = float(np.quantile(self.score(x_train), THRESHOLD_QUANTILE))
        return self

    def score(self, x: FloatArray) -> FloatArray:
        """Largest absolute z-score across the selected features."""
        z = np.abs((self._select(x) - self.mean) / self.std)
        return np.asarray(z.max(axis=1), dtype=np.float64)

    def predict(self, x: FloatArray) -> BoolArray:
        return np.asarray(self.score(x) > self.threshold, dtype=np.bool_)
