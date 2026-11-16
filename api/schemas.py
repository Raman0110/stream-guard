"""Response models. They define the JSON shape and feed the OpenAPI docs."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class ReadingOut(BaseModel):
    sensor_id: str
    ts: datetime
    temperature: float
    humidity: float
    vibration: float
    anomaly_score: float
    is_anomaly: bool


class HealthOut(BaseModel):
    model_config = ConfigDict(protected_namespaces=())  # allow the field name `model_version`

    status: Literal["ok"]
    database: Literal["ok"]
    model_version: str


class PipelineMetricsOut(BaseModel):
    window_seconds: int
    readings_in_window: int
    throughput_per_sec: float
    latency_p50_s: float | None  # None when there is no data in the window
    latency_p95_s: float | None
    anomalies_in_window: int
    readings_total: int
    dlq_total: int


class DriftFeatureOut(BaseModel):
    feature: str
    ks_statistic: float
    ks_pvalue: float
    psi: float
    drift_detected: bool


class DriftOut(BaseModel):
    status: Literal["no_data", "ok", "drift"]
    computed_at: datetime | None
    features: list[DriftFeatureOut]


class PredictOut(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    anomaly_score: float
    is_anomaly: bool
    threshold: float
    model_version: str
    history_readings_used: int  # stored readings of this sensor used to fill the rolling window
