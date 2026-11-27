"""FastAPI application factory and process composition root."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI
from sqlalchemy import Engine

from api.routers import metrics, prediction, readings, system
from api.security import require_api_key, validate_auth_config
from common.config import load_settings
from common.logging_setup import setup_logging
from common.tables import connect_from_settings
from ml.model import Scorer, load_artifact


def create_app(
    engine: Engine | None = None,
    scorer: Scorer | None = None,
    *,
    auth_enabled: bool | None = None,
    api_key: str | None = None,
) -> FastAPI:
    """Build the API; tests can inject isolated database and model dependencies."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        created_engine: Engine | None = None
        try:
            cfg = None
            if engine is None or scorer is None:
                setup_logging()
                cfg = load_settings()
            if engine is None:
                assert cfg is not None
                created_engine = connect_from_settings(cfg)
                app.state.engine = created_engine
            if scorer is None:
                assert cfg is not None
                app.state.scorer = Scorer(load_artifact(Path(cfg.model_path)))
            enabled = cfg.api_auth_enabled if auth_enabled is None and cfg else bool(auth_enabled)
            configured_key = cfg.api_key if api_key is None and cfg else (api_key or "")
            validate_auth_config(enabled, configured_key)
            app.state.auth_enabled = enabled
            app.state.api_key = configured_key
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
    app.state.auth_enabled = False
    app.state.api_key = ""
    app.include_router(system.router)
    protected = [Depends(require_api_key)]
    app.include_router(readings.router, dependencies=protected)
    app.include_router(metrics.router, dependencies=protected)
    app.include_router(prediction.router, dependencies=protected)
    return app


app = create_app()
