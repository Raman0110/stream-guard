"""Serialization and fault-injection helpers for producer messages."""

import json


def encode_message(message: dict[str, object]) -> bytes:
    return json.dumps(message).encode()


def corrupt_message(message: dict[str, object], random_value: float) -> bytes:
    """Create one of three malformed payloads for dead-letter handling."""
    if random_value < 0.33:
        return b"this is not json"
    if random_value < 0.66:
        message["temperature"] = "hot"
        return encode_message(message)
    message.pop("sensor_id")
    return encode_message(message)
