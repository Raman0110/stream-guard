import numpy as np
import pytest

from ml import train
from ml.baselines import ZScoreDetector
from ml.model import train_model


def test_zscore_calibrated_to_one_percent_false_alarms_on_training_data():
    rng = np.random.default_rng(0)
    x = rng.normal(0, 1, (20000, 3))
    detector = ZScoreDetector().fit(x)
    assert detector.predict(x).mean() == pytest.approx(0.01, abs=0.002)


def test_zscore_flags_far_values_and_not_typical_ones():
    rng = np.random.default_rng(0)
    detector = ZScoreDetector().fit(rng.normal(10, 2, (5000, 2)))
    typical = np.array([[10.0, 10.0]])
    far = np.array([[10.0, 40.0]])  # 15 sigma on the second column
    assert not detector.predict(typical)[0]
    assert detector.predict(far)[0]
    assert detector.score(far)[0] > detector.score(typical)[0]


def test_zscore_column_selection_ignores_other_columns():
    rng = np.random.default_rng(0)
    x = rng.normal(0, 1, (5000, 3))
    detector = ZScoreDetector(columns=[0]).fit(x)
    outlier_in_ignored_column = np.array([[0.0, 100.0, 100.0]])
    assert not detector.predict(outlier_in_ignored_column)[0]


def test_zscore_constant_column_does_not_divide_by_zero():
    x = np.column_stack([np.ones(100), np.arange(100.0)])
    detector = ZScoreDetector().fit(x)
    assert np.isfinite(detector.score(x)).all()


def test_metrics_from_flags_known_values():
    flagged = np.array([True, True, False, False, True])
    truth = np.array([True, False, True, False, True])
    m = train.metrics_from_flags(flagged, truth)
    assert (m["tp"], m["fp"], m["fn"], m["tn"]) == (2, 1, 1, 1)
    assert m["precision"] == pytest.approx(2 / 3)
    assert m["recall"] == pytest.approx(2 / 3)
    assert m["f1"] == pytest.approx(2 / 3)


def test_metrics_from_flags_nothing_flagged_is_zero_not_an_error():
    m = train.metrics_from_flags(np.zeros(4, dtype=bool), np.array([True, False, False, False]))
    assert m["precision"] == 0 and m["recall"] == 0 and m["f1"] == 0


def test_recall_by_kind_counts_each_type_separately():
    kinds = np.array(["spike", "spike", "stuck_start", "stuck_repeat", "stuck_repeat", ""])
    flagged = np.array([True, False, False, True, True, True])
    r = train.recall_by_kind(flagged, kinds)
    assert r["spike"] == {"detected": 1, "total": 2, "recall": 0.5}
    assert r["stuck_start"]["recall"] == 0.0
    assert r["stuck_repeat"]["recall"] == 1.0  # the flagged normal reading ("") is not counted


def test_recall_by_kind_handles_missing_kind():
    r = train.recall_by_kind(np.array([True]), np.array([""]))
    assert r["spike"]["total"] == 0 and r["spike"]["recall"] == 0.0


def test_dataset_kinds_are_consistent_with_labels():
    x, y, kinds = train.make_dataset_with_kinds(3000, seed=5, anomaly_rate=0.1)
    assert len(x) == len(y) == len(kinds) == 3000
    assert ((kinds != "") == y).all()  # a kind is present exactly when labelled anomalous
    assert set(kinds[y]) <= {"spike", "stuck_start", "stuck_repeat"}
    assert {"spike", "stuck_start", "stuck_repeat"} <= set(kinds)  # all three occur


def test_make_dataset_unchanged_by_kind_tracking():
    x1, y1 = train.make_dataset(1000, seed=9, anomaly_rate=0.05)
    x2, y2, _ = train.make_dataset_with_kinds(1000, seed=9, anomaly_rate=0.05)
    assert np.array_equal(x1, x2) and np.array_equal(y1, y2)


def test_isolation_forest_beats_zscore_baselines_on_f1():
    """The comparison reported in reports/evaluation.md: the forest should win on F1."""
    x_train, _ = train.make_dataset(8000, seed=42, anomaly_rate=0.0)
    artifact = train_model(x_train, seed=42)
    x, y = train.make_dataset(6000, seed=43, anomaly_rate=0.05)
    flags = train.method_flags(artifact, x_train, x)
    f1 = {name: train.metrics_from_flags(f, y)["f1"] for name, f in flags.items()}
    assert f1[train.FOREST] > f1["Z-score, 3 raw values"]
    assert f1[train.FOREST] > f1["Z-score, 9 features"]


def test_report_contains_comparison_and_per_type_sections(tmp_path, monkeypatch):
    report = tmp_path / "report.md"
    argv = ["train", "--samples", "3000", "--test-samples", "3000",
            "--models-dir", str(tmp_path), "--report", str(report)]  # fmt: skip
    monkeypatch.setattr("sys.argv", argv)
    train.main()
    text = report.read_text()
    assert "## Comparison with simple baselines" in text
    assert "Z-score, 9 features" in text and "Z-score, 3 raw values" in text
    assert "## Recall by anomaly type" in text
    assert "Stuck, first reading" in text and "Stuck, repeated readings" in text
