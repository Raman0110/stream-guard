from datetime import UTC, datetime, timedelta

import pandas as pd

from dashboard.presentation import format_latency, readings_frame, telemetry_figure


def test_format_latency_uses_readable_units() -> None:
    assert format_latency(None) == "—"
    assert format_latency(0.042) == "42 ms"
    assert format_latency(1.25) == "1.25 s"


def test_readings_frame_sorts_timestamps() -> None:
    later = datetime(2026, 1, 1, 0, 0, 2, tzinfo=UTC)
    earlier = later - timedelta(seconds=1)
    rows = [
        {
            "sensor_id": "sensor-1",
            "ts": later.isoformat(),
            "temperature": 25.0,
            "humidity": 50.0,
            "vibration": 0.2,
            "anomaly_score": 0.4,
            "is_anomaly": False,
        },
        {
            "sensor_id": "sensor-1",
            "ts": earlier.isoformat(),
            "temperature": 24.9,
            "humidity": 50.1,
            "vibration": 0.2,
            "anomaly_score": 0.4,
            "is_anomaly": False,
        },
    ]

    frame = readings_frame(rows)

    assert frame["ts"].tolist() == [pd.Timestamp(earlier), pd.Timestamp(later)]


def test_telemetry_figure_keeps_sensor_series_separate() -> None:
    frame = pd.DataFrame(
        [
            {
                "sensor_id": sensor_id,
                "ts": datetime(2026, 1, 1, 0, 0, index, tzinfo=UTC),
                "temperature": 25.0 + index,
                "humidity": 50.0 + index,
                "vibration": 0.2 + index / 100,
                "is_anomaly": index == 1,
            }
            for sensor_id in ("sensor-1", "sensor-2")
            for index in range(2)
        ]
    )

    figure = telemetry_figure(frame)
    line_traces = [trace for trace in figure.data if trace.mode == "lines"]

    assert len(line_traces) == 6  # two sensors across three measurement panels
    assert {trace.name for trace in line_traces} == {"sensor-1", "sensor-2"}
    assert all(len(trace.x) == 2 for trace in line_traces)
