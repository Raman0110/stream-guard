import json

from producer.payloads import corrupt_message, encode_message


def message() -> dict[str, object]:
    return {"sensor_id": "s1", "temperature": 25.0}


def test_encode_message_produces_json_bytes() -> None:
    assert json.loads(encode_message(message())) == message()


def test_corruption_variants() -> None:
    assert corrupt_message(message(), 0.1) == b"this is not json"
    assert json.loads(corrupt_message(message(), 0.5))["temperature"] == "hot"
    assert "sensor_id" not in json.loads(corrupt_message(message(), 0.9))
