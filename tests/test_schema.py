import pytest
from pydantic import ValidationError

from common.schema import SensorReading

VALID = {
    "sensor_id": "sensor-1",
    "timestamp": "2024-01-01T00:00:00+00:00",
    "temperature": 25.0,
    "humidity": 50.0,
    "vibration": 0.5,
}


def test_valid_reading():
    assert SensorReading(**VALID).sensor_id == "sensor-1"


@pytest.mark.parametrize(
    "change",
    [
        {"temperature": "hot"},
        {"temperature": 999},
        {"humidity": -1},
        {"vibration": -0.1},
        {"sensor_id": ""},
        {"timestamp": "2024-01-01T00:00:00"},  # no timezone
        {"timestamp": "yesterday"},
        {"extra_field": 1},
    ],
)
def test_invalid_reading_rejected(change):
    with pytest.raises(ValidationError):
        SensorReading(**{**VALID, **change})


def test_missing_field_rejected():
    data = {k: v for k, v in VALID.items() if k != "humidity"}
    with pytest.raises(ValidationError):
        SensorReading(**data)


def test_nan_rejected():
    with pytest.raises(ValidationError):
        SensorReading.model_validate_json(
            '{"sensor_id":"s","timestamp":"2024-01-01T00:00:00Z",'
            '"temperature":NaN,"humidity":1,"vibration":1}'
        )
