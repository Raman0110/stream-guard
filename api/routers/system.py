"""System health routes."""

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import Engine, text

from api.dependencies import get_engine, get_scorer
from api.schemas import HealthOut
from ml.model import Scorer

router = APIRouter(tags=["system"])
log = logging.getLogger("api")


@router.get("/health", response_model=HealthOut)
def health(db: Engine = Depends(get_engine), model: Scorer = Depends(get_scorer)) -> HealthOut:
    """Verify that the database answers and a model is loaded."""
    try:
        with db.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        log.error("health check failed", extra={"error": str(exc)})
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return HealthOut(status="ok", database="ok", model_version=model.version)
