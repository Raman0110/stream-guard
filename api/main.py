"""FastAPI application factory and process composition root."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from sqlalchemy import Engine

from api.routers import metrics, prediction, readings, system
from common.config import load_settings
from common.logging_setup import setup_logging
from common.tables import connect_from_settings
from ml.model import Scorer, load_artifact


def create_app(engine: Engine | None = None, scorer: Scorer | None = None) -> FastAPI:
    """Build the API; tests can inject isolated database and model dependencies."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        created_engine: Engine | None = None
        try:
            if engine is None or scorer is None:
                setup_logging()
                cfg = load_settings()
                created_engine = connect_from_settings(cfg)
                app.state.engine = created_engine
                app.state.scorer = Scorer(load_artifact(Path(cfg.model_path)))
            yield
        finally:
            if created_engine is not None:
                created_engine.dispose()

    app = FastAPI(
        title="StreamGuard API",
        description="Anomaly detection pipeline: readings, anomalies, metrics, drift, scoring.",
        lifespan=lifespan,
    )
    app.state.engine = engine
    app.state.scorer = scorer
    app.include_router(system.router)
    app.include_router(readings.router)
    app.include_router(metrics.router)
    app.include_router(prediction.router)
    return app


app = create_app()
