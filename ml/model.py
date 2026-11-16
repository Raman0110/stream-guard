"""Model artifact (model + threshold + metadata) and the Scorer used at serving time."""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import numpy.typing as npt
import sklearn
from sklearn.ensemble import IsolationForest

from ml.features import FEATURES, WINDOW

# The flag fires for readings scoring worse than 99% of the clean training data.
FloatArray = npt.NDArray[np.float64]
BoolArray = npt.NDArray[np.bool_]
THRESHOLD_QUANTILE = 0.99


@dataclass
class ModelArtifact:
    model: IsolationForest
    threshold: float  # anomaly_score above this => flagged
    metadata: dict[str, Any]


def anomaly_scores(model: IsolationForest, x: FloatArray) -> FloatArray:
    """Higher = more anomalous. (sklearn's score_samples is the opposite, so we negate it.)"""
    return -np.asarray(model.score_samples(x))


def data_statistics(x: FloatArray) -> dict[str, dict[str, float]]:
    """Per-feature summary of the training data (the baseline for drift monitoring)."""
    stats = {}
    for i, name in enumerate(FEATURES):
        col = x[:, i]
        stats[name] = {
            "mean": float(col.mean()),
            "std": float(col.std()),
            "min": float(col.min()),
            "max": float(col.max()),
            "p01": float(np.quantile(col, 0.01)),
            "p50": float(np.quantile(col, 0.50)),
            "p99": float(np.quantile(col, 0.99)),
        }
    return stats


def train_model(x: FloatArray, seed: int) -> ModelArtifact:
    """Fit an Isolation Forest on clean data and derive the anomaly threshold from it."""
    model = IsolationForest(n_estimators=200, random_state=seed)
    model.fit(x)
    threshold = float(np.quantile(anomaly_scores(model, x), THRESHOLD_QUANTILE))
    now = datetime.now(UTC)
    metadata = {
        "model_version": now.strftime("%Y%m%dT%H%M%SZ"),
        "trained_at": now.isoformat(),
        "algorithm": "IsolationForest",
        "sklearn_version": sklearn.__version__,
        "features": FEATURES,
        "window": WINDOW,
        "n_training_samples": int(len(x)),
        "threshold_quantile": THRESHOLD_QUANTILE,
        "threshold": threshold,
        "seed": seed,
        "data_statistics": data_statistics(x),
    }
    return ModelArtifact(model, threshold, metadata)


def save_artifact(artifact: ModelArtifact, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, path)


def load_artifact(path: Path) -> ModelArtifact:
    artifact = joblib.load(path)
    assert isinstance(artifact, ModelArtifact)
    return artifact


class Scorer:
    """Scores feature vectors with a loaded artifact."""

    def __init__(self, artifact: ModelArtifact) -> None:
        self.artifact = artifact
        self.version: str = artifact.metadata["model_version"]

    def score(self, x: FloatArray) -> tuple[FloatArray, BoolArray]:
        """x has shape (n, 9). Returns (anomaly_scores, is_anomaly flags)."""
        scores = anomaly_scores(self.artifact.model, x)
        return scores, scores > self.artifact.threshold
