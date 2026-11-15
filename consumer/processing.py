"""Pure message-handling logic (no Kafka/DB), so it is trivially unit-testable."""

from pydantic import ValidationError

from common.schema import SensorReading


def parse_message(raw: bytes) -> tuple[SensorReading | None, str | None]:
    """Validate a raw Kafka payload. Returns (reading, None) or (None, error_text)."""
    try:
        return SensorReading.model_validate_json(raw), None
    except ValidationError as exc:
        # Covers both invalid JSON and schema violations.
        return None, "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())
