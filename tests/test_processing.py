import json

from consumer.processing import parse_message

GOOD = {
    "sensor_id": "s1",
    "timestamp": "2024-01-01T00:00:00Z",
    "temperature": 20.0,
    "humidity": 40.0,
    "vibration": 0.4,
}


def test_valid_message():
    reading, error = parse_message(json.dumps(GOOD).encode())
    assert error is None and reading is not None and reading.sensor_id == "s1"


def test_not_json():
    reading, error = parse_message(b"garbage")
    assert reading is None and error


def test_wrong_type_reports_field():
    reading, error = parse_message(json.dumps({**GOOD, "temperature": "hot"}).encode())
    assert reading is None and error is not None and "temperature" in error


def test_missing_field_reports_field():
    bad = {k: v for k, v in GOOD.items() if k != "sensor_id"}
    reading, error = parse_message(json.dumps(bad).encode())
    assert reading is None and error is not None and "sensor_id" in error


def test_binary_garbage():
    reading, error = parse_message(b"\xff\xfe\x00")
    assert reading is None and error
