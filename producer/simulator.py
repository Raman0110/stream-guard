"""Sensor simulator: generates normal readings, plus optional anomalies and drift.

Also used by ml/train.py (milestone 3) to build the baseline training set, so the
"normal" distribution is defined in exactly one place. Fully deterministic for a given seed.
"""

from dataclasses import dataclass
from datetime import datetime

import numpy as np

# Normal operating behaviour: (mean, std) per feature.
TEMP_MEAN, TEMP_STD = 25.0, 1.5
HUM_MEAN, HUM_STD = 50.0, 5.0
VIB_MEAN, VIB_STD = 0.5, 0.1
STUCK_LENGTH = 5  # a "stuck" sensor repeats its last value this many times


@dataclass
class SimulatedReading:
    sensor_id: str
    timestamp: datetime
    temperature: float
    humidity: float
    vibration: float
    is_anomaly: bool  # ground-truth label: for evaluation only, NOT sent to Kafka
    # Anomaly type (also evaluation-only): "spike", "stuck_start" (first reading of a stuck
    # episode, an exact copy of a normal reading) or "stuck_repeat"; None for normal readings.
    anomaly_kind: str | None = None

    def to_message(self) -> dict[str, object]:
        """The JSON payload that goes on the topic (no label!)."""
        return {
            "sensor_id": self.sensor_id,
            "timestamp": self.timestamp.isoformat(),
            "temperature": self.temperature,
            "humidity": self.humidity,
            "vibration": self.vibration,
        }


class SensorSimulator:
    def __init__(
        self,
        num_sensors: int = 5,
        seed: int = 42,
        anomaly_rate: float = 0.0,
        drift_per_reading: float = 0.0,
    ) -> None:
        self.rng = np.random.default_rng(seed)
        self.sensor_ids = [f"sensor-{i + 1}" for i in range(num_sensors)]
        self.anomaly_rate = anomaly_rate
        self.drift_per_reading = drift_per_reading
        self.drift = 0.0  # accumulated shift of the mean
        self._turn = 0  # round-robin pointer over sensors
        self._stuck_left: dict[str, int] = {}
        self._last: dict[str, tuple[float, float, float]] = {}

    def next_reading(self, timestamp: datetime) -> SimulatedReading:
        """Emit one reading, cycling through the sensors."""
        sensor_id = self.sensor_ids[self._turn % len(self.sensor_ids)]
        self._turn += 1
        self.drift += self.drift_per_reading

        is_anomaly = False
        kind: str | None = None
        if self._stuck_left.get(sensor_id, 0) > 0:
            # Continue an ongoing stuck-value anomaly: repeat the last values exactly.
            self._stuck_left[sensor_id] -= 1
            temp, hum, vib = self._last[sensor_id]
            is_anomaly = True
            kind = "stuck_repeat"
        else:
            temp = self.rng.normal(TEMP_MEAN + self.drift, TEMP_STD)
            hum = self.rng.normal(HUM_MEAN + self.drift, HUM_STD)
            vib = self.rng.normal(VIB_MEAN + 0.01 * self.drift, VIB_STD)
            if self.rng.random() < self.anomaly_rate:
                is_anomaly = True
                if self.rng.random() < 0.5 and sensor_id in self._last:
                    self._stuck_left[sensor_id] = STUCK_LENGTH - 1
                    temp, hum, vib = self._last[sensor_id]
                    kind = "stuck_start"
                else:
                    temp, hum, vib = self._spike(temp, hum, vib)
                    kind = "spike"

        # Clamp to the schema's valid ranges so anomalies are not rejected as invalid.
        temp = float(np.clip(temp, -50, 150))
        hum = float(np.clip(hum, 0, 100))
        vib = float(np.clip(vib, 0, 100))
        self._last[sensor_id] = (temp, hum, vib)
        return SimulatedReading(sensor_id, timestamp, temp, hum, vib, is_anomaly, kind)

    def _spike(self, temp: float, hum: float, vib: float) -> tuple[float, float, float]:
        """Push one randomly chosen feature far from normal (8-12 standard deviations)."""
        sign = self.rng.choice([-1, 1])
        size = self.rng.uniform(8, 12)
        feature = self.rng.integers(0, 3)
        if feature == 0:
            temp += sign * size * TEMP_STD
        elif feature == 1:
            hum += sign * size * HUM_STD
        else:
            vib += abs(size) * VIB_STD  # vibration is >= 0, so only spike upward
        return temp, hum, vib
