"""Rolling-window feature computation, shared by training and the live consumer.

Using ONE implementation in both places prevents "training/serving skew": the model
is trained on exactly the features it will see in production.
"""

from collections import deque

import numpy as np

WINDOW = 5  # most recent readings per sensor used for rolling statistics (5 gave the best F1)
BASE = ["temperature", "humidity", "vibration"]
FEATURES = BASE + [f"{b}_mean" for b in BASE] + [f"{b}_std" for b in BASE]


class RollingFeatures:
    """Keeps the last WINDOW readings of every sensor and turns each new reading
    into a 9-number feature vector: raw values, rolling means, rolling stds."""

    def __init__(self, window: int = WINDOW) -> None:
        self.window = window
        self._buffers: dict[str, deque[tuple[float, float, float]]] = {}

    def clone(self) -> "RollingFeatures":
        """Return an independent copy for tentative batch processing.

        Consumers can score against the copy and publish it only after durable storage succeeds.
        That keeps retries from advancing the live window twice.
        """
        cloned = RollingFeatures(self.window)
        cloned._buffers = {
            sensor_id: deque(values, maxlen=self.window)
            for sensor_id, values in self._buffers.items()
        }
        return cloned

    def snapshot(self) -> dict[str, tuple[tuple[float, float, float], ...]]:
        """Expose an immutable state snapshot for checkpointing and verification."""
        return {sensor_id: tuple(values) for sensor_id, values in self._buffers.items()}

    def update(
        self, sensor_id: str, temperature: float, humidity: float, vibration: float
    ) -> list[float]:
        buf = self._buffers.setdefault(sensor_id, deque(maxlen=self.window))
        buf.append((temperature, humidity, vibration))
        values = np.array(buf)  # shape (n_in_window, 3)
        means = values.mean(axis=0)
        stds = values.std(axis=0)  # 0.0 while the window holds a single reading
        return [temperature, humidity, vibration, *means, *stds]
