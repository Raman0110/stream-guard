"""Reusable database query builders for API routes."""

from typing import Any

from sqlalchemy import Select, select

from common.tables import predictions, readings


def readings_with_predictions() -> Select[Any]:
    return select(
        readings.c.sensor_id,
        readings.c.ts,
        readings.c.temperature,
        readings.c.humidity,
        readings.c.vibration,
        predictions.c.anomaly_score,
        predictions.c.is_anomaly,
    ).join_from(readings, predictions, predictions.c.reading_id == readings.c.id)
