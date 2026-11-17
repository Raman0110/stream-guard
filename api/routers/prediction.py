"""On-demand model scoring route."""

import numpy as np
from fastapi import APIRouter, Depends
from sqlalchemy import Engine, select

from api.dependencies import get_engine, get_scorer
from api.schemas import PredictOut
from common.schema import SensorValues
from common.tables import readings
from ml.features import WINDOW, RollingFeatures
from ml.model import Scorer

router = APIRouter(tags=["model"])


@router.post("/predict", response_model=PredictOut)
def predict(
    body: SensorValues,
    db: Engine = Depends(get_engine),
    model: Scorer = Depends(get_scorer),
) -> PredictOut:
    """Score one reading using its sensor's recent history without storing it."""
    stmt = (
        select(readings.c.temperature, readings.c.humidity, readings.c.vibration)
        .where(readings.c.sensor_id == body.sensor_id)
        .order_by(readings.c.ts.desc())
        .limit(WINDOW - 1)
    )
    with db.connect() as conn:
        history = list(reversed(conn.execute(stmt).all()))

    features = RollingFeatures()
    for temperature, humidity, vibration in history:
        features.update(body.sensor_id, temperature, humidity, vibration)
    vector = features.update(body.sensor_id, body.temperature, body.humidity, body.vibration)
    scores, flags = model.score(np.array([vector]))
    return PredictOut(
        anomaly_score=float(scores[0]),
        is_anomaly=bool(flags[0]),
        threshold=model.artifact.threshold,
        model_version=model.version,
        history_readings_used=len(history),
    )
