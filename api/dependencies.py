"""FastAPI dependencies backed by application state."""

from fastapi import Request
from sqlalchemy import Engine

from ml.model import Scorer


def get_engine(request: Request) -> Engine:
    engine: Engine = request.app.state.engine
    return engine


def get_scorer(request: Request) -> Scorer:
    scorer: Scorer = request.app.state.scorer
    return scorer
