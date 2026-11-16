"""REST API over the pipeline's PostgreSQL data. Interactive docs: /docs (OpenAPI: /openapi.json).

Run locally:  uvicorn api.main:app --reload
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from sqlalchemy import Engine, Select, func, select, text

from api.schemas import (
    DriftFeatureOut,
    DriftOut,
    HealthOut,
    PipelineMetricsOut,
    PredictOut,
    ReadingOut,
)
from common.config import load_settings
from common.logging_setup import setup_logging
from common.schema import SensorValues
from common.tables import connect, dlq_events, drift_metrics, predictions, readings
from ml.features import WINDOW, RollingFeatures
from ml.model import Scorer, load_artifact

log = logging.getLogger("api")


def create_app(engine: Engine | None = None, scorer: Scorer | None = None) -> FastAPI:
    """App factory. Tests pass in their own engine/scorer; in production they are built
    from environment settings at startup."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if engine is None or scorer is None:
            setup_logging()
            cfg = load_settings()
            app.state.engine = connect(cfg.database_url)  # retries while Postgres starts
            app.state.scorer = Scorer(load_artifact(Path(cfg.model_path)))
        yield

    app = FastAPI(
        title="StreamGuard API",
        description="Anomaly detection pipeline: readings, anomalies, metrics, drift, scoring.",
        lifespan=lifespan,
    )
    app.state.engine = engine
    app.state.scorer = scorer

    def get_engine(request: Request) -> Engine:
        engine_: Engine = request.app.state.engine
        return engine_

    def get_scorer(request: Request) -> Scorer:
        scorer_: Scorer = request.app.state.scorer
        return scorer_

    @app.get("/health", response_model=HealthOut, tags=["system"])
    def health(db: Engine = Depends(get_engine), model: Scorer = Depends(get_scorer)) -> HealthOut:
        """Liveness check: verifies the database answers and a model is loaded."""
        try:
            with db.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as exc:
            log.error("health check failed", extra={"error": str(exc)})
            raise HTTPException(status_code=503, detail="database unavailable") from exc
        return HealthOut(status="ok", database="ok", model_version=model.version)

    @app.get("/readings/recent", response_model=list[ReadingOut], tags=["data"])
    def recent_readings(
        sensor_id: str | None = Query(None, description="Only this sensor"),
        limit: int = Query(50, ge=1, le=1000),
        db: Engine = Depends(get_engine),
    ) -> list[ReadingOut]:
        """Most recent readings with their anomaly scores, newest first."""
        stmt = _readings_query().order_by(readings.c.ts.desc()).limit(limit)
        if sensor_id:
            stmt = stmt.where(readings.c.sensor_id == sensor_id)
        with db.connect() as conn:
            return [ReadingOut(**row._mapping) for row in conn.execute(stmt)]

    @app.get("/anomalies", response_model=list[ReadingOut], tags=["data"])
    def anomalies(
        since: datetime | None = Query(None, description="ISO timestamp; default: last 24 hours"),
        limit: int = Query(100, ge=1, le=1000),
        db: Engine = Depends(get_engine),
    ) -> list[ReadingOut]:
        """Readings flagged as anomalous since a point in time, newest first."""
        if since is None:
            since = datetime.now(UTC) - timedelta(hours=24)
        elif since.tzinfo is None:
            since = since.replace(tzinfo=UTC)  # assume UTC if no timezone was given
        stmt = (
            _readings_query()
            .where(predictions.c.is_anomaly, readings.c.ts >= since)
            .order_by(readings.c.ts.desc())
            .limit(limit)
        )
        with db.connect() as conn:
            return [ReadingOut(**row._mapping) for row in conn.execute(stmt)]

    @app.get("/metrics/pipeline", response_model=PipelineMetricsOut, tags=["metrics"])
    def pipeline_metrics(
        window_seconds: int = Query(60, ge=1, le=86400, description="Look-back window"),
        db: Engine = Depends(get_engine),
    ) -> PipelineMetricsOut:
        """Throughput, end-to-end latency (event time -> scored) and dead-letter count."""
        # Use the database clock so API and DB clocks can't disagree.
        cutoff = func.now() - timedelta(seconds=window_seconds)
        latency = func.extract("epoch", predictions.c.scored_at - readings.c.ts)
        window_stmt = (
            select(
                func.count(),
                func.count().filter(predictions.c.is_anomaly),
                func.percentile_cont(0.5).within_group(latency),
                func.percentile_cont(0.95).within_group(latency),
            )
            .select_from(readings.join(predictions, predictions.c.reading_id == readings.c.id))
            .where(readings.c.ingested_at >= cutoff)
        )
        with db.connect() as conn:
            n, n_anom, p50, p95 = conn.execute(window_stmt).one()
            total = conn.execute(select(func.count()).select_from(readings)).scalar_one()
            dlq = conn.execute(select(func.count()).select_from(dlq_events)).scalar_one()
        return PipelineMetricsOut(
            window_seconds=window_seconds,
            readings_in_window=n,
            throughput_per_sec=n / window_seconds,
            latency_p50_s=None if p50 is None else float(p50),
            latency_p95_s=None if p95 is None else float(p95),
            anomalies_in_window=n_anom,
            readings_total=total,
            dlq_total=dlq,
        )

    @app.get("/metrics/drift", response_model=DriftOut, tags=["metrics"])
    def drift_status(db: Engine = Depends(get_engine)) -> DriftOut:
        """Latest drift scores per feature. Overall status is 'drift' if any feature drifted."""
        # DISTINCT ON keeps only the newest row for each feature.
        stmt = (
            select(drift_metrics)
            .distinct(drift_metrics.c.feature)
            .order_by(drift_metrics.c.feature, drift_metrics.c.computed_at.desc())
        )
        with db.connect() as conn:
            rows = conn.execute(stmt).all()
        if not rows:
            return DriftOut(status="no_data", computed_at=None, features=[])
        features = [DriftFeatureOut(**row._mapping) for row in rows]
        return DriftOut(
            status="drift" if any(f.drift_detected for f in features) else "ok",
            computed_at=max(row.computed_at for row in rows),
            features=features,
        )

    @app.post("/predict", response_model=PredictOut, tags=["model"])
    def predict(
        body: SensorValues,
        db: Engine = Depends(get_engine),
        model: Scorer = Depends(get_scorer),
    ) -> PredictOut:
        """Score one reading on demand (nothing is stored).

        The model needs the sensor's recent history for its rolling features, so the last
        few stored readings of that sensor are loaded to fill the window.
        """
        stmt = (
            select(readings.c.temperature, readings.c.humidity, readings.c.vibration)
            .where(readings.c.sensor_id == body.sensor_id)
            .order_by(readings.c.ts.desc())
            .limit(WINDOW - 1)
        )
        with db.connect() as conn:
            history = list(reversed(conn.execute(stmt).all()))  # oldest first
        features = RollingFeatures()
        for t, h, v in history:
            features.update(body.sensor_id, t, h, v)
        vector = features.update(body.sensor_id, body.temperature, body.humidity, body.vibration)
        scores, flags = model.score(np.array([vector]))
        return PredictOut(
            anomaly_score=float(scores[0]),
            is_anomaly=bool(flags[0]),
            threshold=model.artifact.threshold,
            model_version=model.version,
            history_readings_used=len(history),
        )

    return app


def _readings_query() -> Select[Any]:
    """SELECT of readings joined with their predictions (shared by two endpoints)."""
    return select(
        readings.c.sensor_id,
        readings.c.ts,
        readings.c.temperature,
        readings.c.humidity,
        readings.c.vibration,
        predictions.c.anomaly_score,
        predictions.c.is_anomaly,
    ).join_from(readings, predictions, predictions.c.reading_id == readings.c.id)


app = create_app()  # what uvicorn serves: `uvicorn api.main:app`
