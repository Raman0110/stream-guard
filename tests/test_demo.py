import urllib.error

import pytest

from scripts import demo
from scripts.demo import PHASES, drift_summary, pipeline_line, wait_for_api, wait_for_drift


def drift_payload(status, flagged=()):
    feats = [{"feature": f, "drift_detected": True} for f in flagged]
    feats.append({"feature": "humidity", "drift_detected": False})
    return {"status": status, "features": feats}


def test_phases_run_in_the_intended_order():
    anomalies = [float(p.producer_env["ANOMALY_RATE"]) for p in PHASES]
    drift = [float(p.producer_env["DRIFT_PER_READING"]) for p in PHASES]
    assert anomalies[0] == 0 and drift[0] == 0  # 1: clean
    assert anomalies[1] > 0 and drift[1] == 0  # 2: anomalies, no drift yet
    assert drift[2] > 0  # 3: drift


def test_phase_env_values_are_strings_and_cover_all_fault_knobs():
    for phase in PHASES:
        assert set(phase.producer_env) == {"ANOMALY_RATE", "INVALID_RATE", "DRIFT_PER_READING"}
        assert all(isinstance(v, str) for v in phase.producer_env.values())


def test_monitor_is_tuned_to_react_quickly():
    assert int(demo.MONITOR_ENV["DRIFT_WINDOW_SIZE"]) < 1000
    assert int(demo.MONITOR_ENV["DRIFT_CHECK_INTERVAL_SECONDS"]) <= 10


def test_drift_summary():
    assert drift_summary({"status": "ok", "features": []}) == "ok"
    assert drift_summary({"status": "no_data", "features": []}) == "no_data"
    text = drift_summary(drift_payload("drift", ["temperature", "humidity_mean"]))
    assert "2 feature(s)" in text and "temperature" in text and "humidity_mean" in text


def test_wait_for_drift_returns_as_soon_as_drift_appears():
    responses = iter([drift_payload("ok"), drift_payload("ok"), drift_payload("drift", ["t"])])
    sleeps = []
    result = wait_for_drift(lambda path: next(responses), 100, 5, sleeps.append, lambda: 0.0)
    assert result is not None and result["status"] == "drift"
    assert sleeps == [5, 5]  # polled three times, slept between


def test_wait_for_drift_gives_up_after_timeout():
    now = [0.0]

    def sleep(seconds):
        now[0] += seconds

    result = wait_for_drift(lambda path: drift_payload("ok"), 20, 5, sleep, lambda: now[0])
    assert result is None
    assert now[0] >= 20


def test_wait_for_api_succeeds_after_retries(monkeypatch):
    monkeypatch.setattr(demo.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def flaky(path):
        calls["n"] += 1
        if calls["n"] < 3:
            raise urllib.error.URLError("not up")
        return {"status": "ok"}

    assert wait_for_api(10, flaky) is True and calls["n"] == 3


def test_wait_for_api_times_out(monkeypatch):
    monkeypatch.setattr(demo.time, "sleep", lambda s: None)

    def down(path):
        raise urllib.error.URLError("down")

    assert wait_for_api(0, down) is False


def test_pipeline_line_formats_metrics_and_missing_latency():
    metrics = {"throughput_per_sec": 10.04, "latency_p50_s": 0.5,
               "anomalies_in_window": 4, "dlq_total": 7}  # fmt: skip
    line = pipeline_line(lambda path: metrics)
    assert "10.0 events/s" in line and "500 ms" in line and "4 anomalies" in line
    assert "n/a" in pipeline_line(lambda path: {**metrics, "latency_p50_s": None})


def test_compose_passes_env_overrides(monkeypatch):
    captured = {}

    def fake_run(cmd, env, check):
        captured.update(cmd=cmd, env=env, check=check)

    monkeypatch.setattr(demo.subprocess, "run", fake_run)
    demo.set_producer({"ANOMALY_RATE": "0.5"})
    assert captured["cmd"][:3] == ["docker", "compose", "up"]
    assert "producer" in captured["cmd"] and "--force-recreate" in captured["cmd"]
    assert captured["env"]["ANOMALY_RATE"] == "0.5"
    assert captured["env"]["DRIFT_WINDOW_SIZE"] == "500"
    assert captured["check"] is True


@pytest.mark.parametrize("path", ["/health", "/metrics/drift"])
def test_fetch_uses_api_base_url(monkeypatch, path):
    seen = {}

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b'{"ok": true}'

    def fake_urlopen(url, timeout):
        seen["url"] = url
        return Resp()

    monkeypatch.setattr(demo.urllib.request, "urlopen", fake_urlopen)
    assert demo.fetch(path) == {"ok": True}
    assert seen["url"] == demo.API + path
