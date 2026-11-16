"""Small HTTP client for the StreamGuard dashboard.

Keeping API access outside the Streamlit page makes failures predictable and the
data-contract code independently testable.
"""

from typing import Any

import requests


class StreamGuardAPI:
    def __init__(self, base_url: str, timeout: float = 3.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get(self, path: str, **params: object) -> Any:
        response = requests.get(f"{self.base_url}{path}", params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def post(self, path: str, payload: dict[str, object]) -> Any:
        response = requests.post(f"{self.base_url}{path}", json=payload, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def health(self) -> dict[str, object]:
        return self.get("/health")

    def metrics(self, window_seconds: int = 60) -> dict[str, object]:
        return self.get("/metrics/pipeline", window_seconds=window_seconds)

    def readings(self, limit: int = 500) -> list[dict[str, object]]:
        return self.get("/readings/recent", limit=limit)

    def anomalies(self, limit: int = 100) -> list[dict[str, object]]:
        return self.get("/anomalies", limit=limit)

    def drift(self) -> dict[str, object]:
        return self.get("/metrics/drift")

    def predict(self, payload: dict[str, object]) -> dict[str, object]:
        return self.post("/predict", payload)
