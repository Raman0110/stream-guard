"""Small, typed HTTP client for the StreamGuard dashboard.

Keeping API access outside the Streamlit page makes failures predictable and the
data-contract code independently testable.
"""

from collections.abc import Mapping
from typing import Any, TypedDict, cast

import requests

QueryValue = str | bytes | int | float | None
JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


class HealthResponse(TypedDict):
    status: str
    database: str
    model_version: str


class PipelineMetricsResponse(TypedDict):
    window_seconds: int
    readings_in_window: int
    throughput_per_sec: float
    latency_p50_s: float | None
    latency_p95_s: float | None
    anomalies_in_window: int
    readings_total: int
    dlq_total: int


class ReadingResponse(TypedDict):
    sensor_id: str
    ts: str
    temperature: float
    humidity: float
    vibration: float
    anomaly_score: float
    is_anomaly: bool


class DriftFeatureResponse(TypedDict):
    feature: str
    ks_statistic: float
    ks_pvalue: float
    psi: float
    drift_detected: bool


class DriftResponse(TypedDict):
    status: str
    computed_at: str | None
    features: list[DriftFeatureResponse]


class PredictResponse(TypedDict):
    anomaly_score: float
    is_anomaly: bool
    threshold: float
    model_version: str
    history_readings_used: int


class StreamGuardAPI:
    def __init__(self, base_url: str, timeout: float = 3.0, api_key: str = "") -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.headers = {"X-API-Key": api_key} if api_key else {}

    def get(self, path: str, **params: QueryValue) -> Any:
        response = requests.get(
            f"{self.base_url}{path}",
            params=params,
            headers=self.headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def post(self, path: str, payload: Mapping[str, JsonValue]) -> Any:
        response = requests.post(
            f"{self.base_url}{path}",
            json=payload,
            headers=self.headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def health(self) -> HealthResponse:
        return cast(HealthResponse, self.get("/health"))

    def metrics(self, window_seconds: int = 60) -> PipelineMetricsResponse:
        return cast(
            PipelineMetricsResponse,
            self.get("/metrics/pipeline", window_seconds=window_seconds),
        )

    def readings(self, limit: int = 500) -> list[ReadingResponse]:
        return cast(list[ReadingResponse], self.get("/readings/recent", limit=limit))

    def anomalies(self, limit: int = 100) -> list[ReadingResponse]:
        return cast(list[ReadingResponse], self.get("/anomalies", limit=limit))

    def drift(self) -> DriftResponse:
        return cast(DriftResponse, self.get("/metrics/drift"))

    def predict(self, payload: Mapping[str, JsonValue]) -> PredictResponse:
        return cast(PredictResponse, self.post("/predict", payload))
