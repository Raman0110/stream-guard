import json
import os
from datetime import UTC, datetime
from types import SimpleNamespace

import numpy as np
import pytest

from common.schema import SensorReading
from scripts import load_test
from scripts.load_test import RunResult, load_dotenv, publish, render_report, summarize


class FakeProducer:
    """Stands in for confluent_kafka.Producer: records messages, can simulate a full queue."""

    def __init__(self, buffer_errors: int = 0) -> None:
        self.messages: list[tuple[str, bytes, str]] = []
        self.buffer_errors = buffer_errors
        self.flushed = False

    def produce(self, topic, payload, key=None, on_delivery=None):
        if self.buffer_errors > 0:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        self.messages.append((topic, payload, key))
        if on_delivery:
            on_delivery(None, None)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        self.flushed = True
        return 0


def test_summarize_percentiles_and_throughput():
    latencies = np.linspace(0.1, 1.0, 100)  # 100 ms .. 1000 ms
    s = summarize(RunResult(sent=100, stored=100, send_seconds=2.0, total_seconds=4.0,
                            latencies_s=latencies, clock_skew_s=0.0))  # fmt: skip
    assert s["lost"] == 0
    assert s["offered_rate_eps"] == pytest.approx(50)
    assert s["pipeline_throughput_eps"] == pytest.approx(25)
    assert s["latency_p50_ms"] == pytest.approx(550, abs=5)
    assert s["latency_p95_ms"] == pytest.approx(955, abs=10)
    assert s["latency_p50_ms"] < s["latency_p95_ms"] < s["latency_p99_ms"] <= s["latency_max_ms"]


def test_summarize_reports_lost_events_and_handles_no_data():
    s = summarize(RunResult(10, 0, 1.0, 0.0, np.array([]), 0.0))
    assert s["lost"] == 10
    assert s["pipeline_throughput_eps"] == 0
    assert np.isnan(s["latency_p50_ms"])


def test_publish_sends_valid_tagged_events():
    fake = FakeProducer()
    t_first, t_last, failures = publish(fake, "topic", events=20, rate=0, sensors=3, seed=1)
    assert len(fake.messages) == 20 and failures == 0 and fake.flushed
    assert t_last >= t_first
    ids = set()
    for topic, payload, key in fake.messages:
        assert topic == "topic"
        reading = SensorReading.model_validate_json(payload)  # must pass the consumer's schema
        assert reading.sensor_id.startswith(load_test.SENSOR_PREFIX) and key == reading.sensor_id
        ids.add(reading.sensor_id)
    assert ids == {"load-1", "load-2", "load-3"}


def test_publish_retries_when_local_queue_is_full():
    fake = FakeProducer(buffer_errors=3)
    publish(fake, "t", events=5, rate=0, sensors=1, seed=1)
    assert len(fake.messages) == 5  # nothing dropped


def test_publish_respects_target_rate():
    fake = FakeProducer()
    t_first, t_last, _ = publish(fake, "t", events=10, rate=100, sensors=1, seed=1)
    assert t_last - t_first >= 0.08  # 10 events at 100/s take about 0.1 s


def test_load_dotenv_sets_missing_but_keeps_existing(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("# comment\n\nLT_NEW=abc\nLT_QUOTED='x y'\nLT_EXISTING=file\nbroken line\n")
    monkeypatch.delenv("LT_NEW", raising=False)
    monkeypatch.delenv("LT_QUOTED", raising=False)
    monkeypatch.setenv("LT_EXISTING", "real")
    load_dotenv(env)
    assert os.environ["LT_NEW"] == "abc"
    assert os.environ["LT_QUOTED"] == "x y"
    assert os.environ["LT_EXISTING"] == "real"
    for k in ("LT_NEW", "LT_QUOTED"):
        monkeypatch.delenv(k, raising=False)


def test_load_dotenv_missing_file_is_fine(tmp_path):
    load_dotenv(tmp_path / "nope.env")


def test_render_report_contains_headline_numbers():
    stats = summarize(RunResult(100, 100, 1.0, 1.0, np.full(100, 0.25), 0.0))
    args = SimpleNamespace(rate=100.0, sensors=5)
    md = render_report(stats, args, skew_s=0.012, background=7)
    assert "100 / 100 / 0" in md and "250 ms" in md and "+12 ms" in md
    assert "100 events/sec" in md
    assert "7 normal (non-test) events" in md


def test_unthrottled_label():
    stats = summarize(RunResult(1, 1, 1.0, 1.0, np.array([0.1]), 0.0))
    assert "unthrottled" in render_report(stats, SimpleNamespace(rate=0, sensors=1), 0.0, 0)


def test_timestamps_are_timezone_aware_utc():
    fake = FakeProducer()
    publish(fake, "t", events=1, rate=0, sensors=1, seed=1)
    ts = json.loads(fake.messages[0][1])["timestamp"]
    assert datetime.fromisoformat(ts).tzinfo == UTC


def test_event_timestamps_are_unique_even_with_a_coarse_clock(monkeypatch):
    """Regression: a coarse clock (Windows ~15 ms) made fast runs reuse timestamps, and the
    consumer then dropped the events as duplicates of (sensor_id, ts)."""
    frozen = datetime(2024, 1, 1, tzinfo=UTC)

    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return frozen

    monkeypatch.setattr(load_test, "datetime", FrozenDatetime)
    fake = FakeProducer()
    publish(fake, "t", events=200, rate=0, sensors=1, seed=1)
    stamps = [json.loads(payload)["timestamp"] for _, payload, _ in fake.messages]
    assert len(set(stamps)) == 200
    assert stamps == sorted(stamps)
