"""Reading and anomaly query routes."""

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import Engine

from api.dependencies import get_engine
from api.queries import readings_with_predictions
from api.schemas import ReadingOut
from common.tables import predictions, readings

router = APIRouter(tags=["data"])


@router.get("/readings/recent", response_model=list[ReadingOut])
def recent_readings(
    sensor_id: str | None = Query(None, description="Only this sensor"),
    limit: int = Query(50, ge=1, le=1000),
    db: Engine = Depends(get_engine),
) -> list[ReadingOut]:
    """Return recent readings with anomaly scores, newest first."""
    stmt = readings_with_predictions().order_by(readings.c.ts.desc()).limit(limit)
    if sensor_id:
        stmt = stmt.where(readings.c.sensor_id == sensor_id)
    with db.connect() as conn:
        return [ReadingOut(**row._mapping) for row in conn.execute(stmt)]


@router.get("/anomalies", response_model=list[ReadingOut])
def anomalies(
    since: datetime | None = Query(None, description="ISO timestamp; default: last 24 hours"),
    limit: int = Query(100, ge=1, le=1000),
    db: Engine = Depends(get_engine),
) -> list[ReadingOut]:
    """Return flagged readings since a selected time, newest first."""
    if since is None:
        since = datetime.now(UTC) - timedelta(hours=24)
    elif since.tzinfo is None:
        since = since.replace(tzinfo=UTC)
    stmt = (
        readings_with_predictions()
        .where(predictions.c.is_anomaly, readings.c.ts >= since)
        .order_by(readings.c.ts.desc())
        .limit(limit)
    )
    with db.connect() as conn:
        return [ReadingOut(**row._mapping) for row in conn.execute(stmt)]
