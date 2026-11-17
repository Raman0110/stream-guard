"""Pipeline and distribution-drift metric routes."""

from datetime import timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy import Engine, func, select

from api.dependencies import get_engine
from api.schemas import DriftFeatureOut, DriftOut, PipelineMetricsOut
from common.tables import dlq_events, drift_metrics, predictions, readings

router = APIRouter(tags=["metrics"])


@router.get("/metrics/pipeline", response_model=PipelineMetricsOut)
def pipeline_metrics(
    window_seconds: int = Query(60, ge=1, le=86400, description="Look-back window"),
    db: Engine = Depends(get_engine),
) -> PipelineMetricsOut:
    """Return throughput, latency, anomaly, and dead-letter statistics."""
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
        count, anomaly_count, p50, p95 = conn.execute(window_stmt).one()
        total = conn.execute(select(func.count()).select_from(readings)).scalar_one()
        dlq = conn.execute(select(func.count()).select_from(dlq_events)).scalar_one()
    return PipelineMetricsOut(
        window_seconds=window_seconds,
        readings_in_window=count,
        throughput_per_sec=count / window_seconds,
        latency_p50_s=None if p50 is None else float(p50),
        latency_p95_s=None if p95 is None else float(p95),
        anomalies_in_window=anomaly_count,
        readings_total=total,
        dlq_total=dlq,
    )


@router.get("/metrics/drift", response_model=DriftOut)
def drift_status(db: Engine = Depends(get_engine)) -> DriftOut:
    """Return the newest drift result for each monitored feature."""
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
        status="drift" if any(feature.drift_detected for feature in features) else "ok",
        computed_at=max(row.computed_at for row in rows),
        features=features,
    )
