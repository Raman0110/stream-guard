"""Train the Isolation Forest, evaluate it, and write the artifacts.

python -m ml.train                 # train + evaluate + write reports/evaluation.md
python -m ml.train --if-missing    # skip if a model already exists (used by docker compose)
"""

import argparse
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd

from common.logging_setup import setup_logging
from ml.baselines import ZScoreDetector
from ml.features import FEATURES, RollingFeatures
from ml.model import ModelArtifact, Scorer, save_artifact, train_model
from producer.simulator import SensorSimulator

FloatArray = npt.NDArray[np.float64]
BoolArray = npt.NDArray[np.bool_]
log = logging.getLogger("ml.train")
MODELS_DIR = Path("models")
REPORT_PATH = Path("reports/evaluation.md")
FOREST = "Isolation Forest (9 features)"
START = datetime(2024, 1, 1, tzinfo=UTC)  # timestamps are irrelevant to the features


def make_dataset_with_kinds(
    n: int, seed: int, anomaly_rate: float, num_sensors: int = 5
) -> tuple[FloatArray, BoolArray, npt.NDArray[np.str_]]:
    """Simulate n readings -> (features, anomaly labels, anomaly kinds).

    kinds[i] is "" for normal readings, else "spike", "stuck_start" or "stuck_repeat".
    """
    sim = SensorSimulator(num_sensors, seed, anomaly_rate)
    features = RollingFeatures()
    rows, labels, kinds = [], [], []
    for i in range(n):
        r = sim.next_reading(START + timedelta(seconds=i))
        rows.append(features.update(r.sensor_id, r.temperature, r.humidity, r.vibration))
        labels.append(r.is_anomaly)
        kinds.append(r.anomaly_kind or "")
    return np.array(rows), np.array(labels, dtype=bool), np.array(kinds)


def make_dataset(
    n: int, seed: int, anomaly_rate: float, num_sensors: int = 5
) -> tuple[FloatArray, BoolArray]:
    """Simulate n readings -> (feature matrix, labels). Label 1 = injected anomaly."""
    x, y, _ = make_dataset_with_kinds(n, seed, anomaly_rate, num_sensors)
    return x, y


def metrics_from_flags(flagged: BoolArray, y: BoolArray) -> dict[str, Any]:
    """Precision / recall / F1 of a boolean anomaly flag against the injected labels."""
    tp = int((flagged & y).sum())
    fp = int((flagged & ~y).sum())
    fn = int((~flagged & y).sum())
    tn = int((~flagged & ~y).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def recall_by_kind(flagged: BoolArray, kinds: npt.NDArray[np.str_]) -> dict[str, dict[str, Any]]:
    """Share of each injected anomaly type that was flagged."""
    out = {}
    for kind in ("spike", "stuck_start", "stuck_repeat"):
        mask = kinds == kind
        total = int(mask.sum())
        detected = int((flagged & mask).sum())
        out[kind] = {
            "detected": detected,
            "total": total,
            "recall": detected / total if total else 0.0,
        }
    return out


def evaluate(artifact: ModelArtifact, x: FloatArray, y: BoolArray) -> dict[str, Any]:
    """Precision / recall / F1 of the Isolation Forest against the injected labels."""
    _, flagged = Scorer(artifact).score(x)
    return metrics_from_flags(flagged, y)


def method_flags(
    artifact: ModelArtifact, x_train: FloatArray, x_test: FloatArray
) -> dict[str, BoolArray]:
    """Anomaly flags on the test set from the Isolation Forest and two z-score baselines.

    All three are calibrated to the same ~1% false-alarm rate on the clean training data.
    """
    _, forest_flags = Scorer(artifact).score(x_test)
    raw = ZScoreDetector(columns=[0, 1, 2]).fit(x_train)  # the 3 raw measurements only
    full = ZScoreDetector().fit(x_train)  # all 9 features, same inputs as the forest
    return {
        FOREST: forest_flags,
        "Z-score, 3 raw values": raw.predict(x_test),
        "Z-score, 9 features": full.predict(x_test),
    }


def write_report(
    path: Path,
    artifact: ModelArtifact,
    comparison: dict[str, dict[str, Any]],
    kind_recall: dict[str, dict[str, dict[str, Any]]],
    n_test: int,
    rate: float,
) -> None:
    md = artifact.metadata
    m = comparison[FOREST]
    kinds = kind_recall[FOREST]
    z9 = kind_recall["Z-score, 9 features"]
    n_anom = sum(k["total"] for k in kinds.values())
    comparison_rows = "\n".join(
        f"| {name} | {c['precision']:.3f} | {c['recall']:.3f} | {c['f1']:.3f} |"
        for name, c in comparison.items()
    )
    labels = {
        "spike": "Spike (one feature far from normal)",
        "stuck_start": "Stuck, first reading (copy of a normal value)",
        "stuck_repeat": "Stuck, repeated readings",
    }
    kind_rows = "\n".join(
        f"| {labels[k]} | {kinds[k]['total']} | "
        f"{kinds[k]['detected']} ({kinds[k]['recall']:.0%}) | "
        f"{z9[k]['detected']} ({z9[k]['recall']:.0%}) |"
        for k in labels
    )
    stuck_share = kinds["stuck_repeat"]["total"] / n_anom if n_anom else 0.0
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""# Model evaluation

Generated by `python -m ml.train` on {md["trained_at"]}. Do not edit by hand.

## Model
- Algorithm: Isolation Forest (model version `{md["model_version"]}`)
- Training data: {md["n_training_samples"]} clean simulated readings (seed {md["seed"]})
- Features ({len(md["features"])}): {", ".join(md["features"])}
  (rolling window = {md["window"]} readings per sensor)
- Anomaly threshold: score > {md["threshold"]:.4f}
  (the {md["threshold_quantile"]:.0%} quantile of training scores)

## Test set
{n_test} readings from a different seed, with injected anomalies
(rate {rate:.0%}; spikes and stuck values). {n_anom} readings are labelled anomalous.

| Metric | Value |
|---|---|
| Precision | {m["precision"]:.3f} |
| Recall | {m["recall"]:.3f} |
| F1 | {m["f1"]:.3f} |

| | Flagged | Not flagged |
|---|---|---|
| **Truly anomalous** | {m["tp"]} | {m["fn"]} |
| **Normal** | {m["fp"]} | {m["tn"]} |

## Comparison with simple baselines
Each method's threshold is the {md["threshold_quantile"]:.0%} quantile of its own scores on the
clean training data, so all three start with the same false-alarm rate. The z-score methods flag
a reading when any feature is unusually far (in standard deviations) from its training mean.

| Method | Precision | Recall | F1 |
|---|---:|---:|---:|
{comparison_rows}

## Recall by anomaly type
| Anomaly type | Readings | Isolation Forest detected | Z-score (9 features) detected |
|---|---:|---:|---:|
{kind_rows}

## Caveats
- The data is synthetic, so these numbers show the pipeline works, not real-world accuracy.
- Labels are per reading. Rolling features carry a spike forward for up to {md["window"]} readings,
  so some normal readings right after an anomaly are also flagged (this lowers precision).
- Each stuck-value episode labels several consecutive readings, so repeated stuck readings are
  {stuck_share:.0%} of all labelled anomalies. That is why overall recall is dominated by how well
  they are detected.
- The first reading of a "stuck value" episode is an exact copy of a normal reading, so it is
  undetectable by design; only the repeats are.
- The window size ({md["window"]}) was chosen by comparing 3, 5, 6 and 10 on this
  synthetic test set, so the figures above are slightly optimistic for that choice.
"""
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=20000, help="training readings")
    parser.add_argument("--test-samples", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--models-dir", type=Path, default=MODELS_DIR)
    parser.add_argument("--report", type=Path, default=REPORT_PATH)
    parser.add_argument("--if-missing", action="store_true", help="do nothing if a model exists")
    args = parser.parse_args()
    setup_logging()

    model_path = args.models_dir / "model.joblib"
    if args.if_missing and model_path.exists():
        log.info("model already exists, skipping training", extra={"path": str(model_path)})
        return

    x_train, _ = make_dataset(args.samples, args.seed, anomaly_rate=0.0)  # clean baseline
    artifact = train_model(x_train, args.seed)
    save_artifact(artifact, model_path)
    (args.models_dir / "model_metadata.json").write_text(json.dumps(artifact.metadata, indent=2))
    # Baseline feature values: the reference sample that drift monitoring compares against.
    pd.DataFrame(x_train[:5000], columns=FEATURES).to_csv(
        args.models_dir / "baseline_features.csv", index=False
    )

    rate = 0.05
    x_test, y_test, kinds = make_dataset_with_kinds(
        args.test_samples, args.seed + 1, anomaly_rate=rate
    )
    flags = method_flags(artifact, x_train, x_test)
    comparison = {name: metrics_from_flags(f, y_test) for name, f in flags.items()}
    kind_recall = {name: recall_by_kind(f, kinds) for name, f in flags.items()}
    write_report(args.report, artifact, comparison, kind_recall, args.test_samples, rate)
    metrics = comparison[FOREST]
    log.info(
        "training done", extra={"model_version": artifact.metadata["model_version"], **metrics}
    )


if __name__ == "__main__":
    main()
