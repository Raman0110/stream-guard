import json

import numpy as np
import pytest

from ml import train
from ml.model import Scorer, load_artifact, save_artifact, train_model


@pytest.fixture(scope="module")
def artifact():
    x, _ = train.make_dataset(3000, seed=1, anomaly_rate=0.0)
    return train_model(x, seed=1)


def test_metadata_contents(artifact):
    md = artifact.metadata
    assert len(md["features"]) == 9
    assert md["n_training_samples"] == 3000
    assert set(md["data_statistics"]) == set(md["features"])
    assert "mean" in md["data_statistics"]["temperature"]
    json.dumps(md)  # must be JSON-serialisable


def test_normal_vs_extreme_scoring(artifact):
    scorer = Scorer(artifact)
    normal = [[25, 50, 0.5, 25, 50, 0.5, 1.5, 5, 0.1]]
    extreme = [[80, 5, 5.0, 40, 30, 2.0, 20, 20, 1.5]]
    scores, flags = scorer.score(np.array(normal + extreme, dtype=float))
    assert scores[1] > scores[0]
    assert not flags[0] and flags[1]


def test_baseline_false_positive_rate_is_low(artifact):
    x, _ = train.make_dataset(3000, seed=2, anomaly_rate=0.0)
    _, flags = Scorer(artifact).score(x)
    assert flags.mean() < 0.05  # threshold is the 99% training quantile


def test_save_and_load_roundtrip(artifact, tmp_path):
    path = tmp_path / "m.joblib"
    save_artifact(artifact, path)
    loaded = load_artifact(path)
    x = np.array([[25, 50, 0.5, 25, 50, 0.5, 1.5, 5, 0.1]], dtype=float)
    assert Scorer(loaded).score(x)[0][0] == pytest.approx(Scorer(artifact).score(x)[0][0])
    assert loaded.metadata["model_version"] == artifact.metadata["model_version"]


def test_evaluate_beats_chance_on_labeled_data(artifact):
    x, y = train.make_dataset(4000, seed=3, anomaly_rate=0.05)
    m = train.evaluate(artifact, x, y)
    assert m["tp"] + m["fn"] == y.sum()
    assert m["precision"] > 0.3 and m["recall"] > 0.3


def test_cli_writes_all_artifacts_and_if_missing_skips(tmp_path, monkeypatch):
    report = tmp_path / "report.md"
    argv = ["train", "--samples", "2000", "--test-samples", "1000",
            "--models-dir", str(tmp_path), "--report", str(report)]  # fmt: skip
    monkeypatch.setattr("sys.argv", argv)
    train.main()
    assert (tmp_path / "model.joblib").exists()
    assert (tmp_path / "model_metadata.json").exists()
    assert (tmp_path / "baseline_features.csv").exists()
    assert "Precision" in report.read_text()

    report.unlink()
    monkeypatch.setattr("sys.argv", [*argv, "--if-missing"])
    train.main()
    assert not report.exists()  # skipped: nothing regenerated
