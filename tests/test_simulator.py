from datetime import UTC, datetime

from common.schema import SensorReading
from producer.simulator import SensorSimulator

T0 = datetime(2024, 1, 1, tzinfo=UTC)


def run(sim, n=500):
    return [sim.next_reading(T0) for _ in range(n)]


def test_same_seed_same_data():
    a = run(SensorSimulator(seed=1, anomaly_rate=0.1))
    b = run(SensorSimulator(seed=1, anomaly_rate=0.1))
    assert [r.temperature for r in a] == [r.temperature for r in b]


def test_different_seed_differs():
    a = run(SensorSimulator(seed=1), 20)
    b = run(SensorSimulator(seed=2), 20)
    assert [r.temperature for r in a] != [r.temperature for r in b]


def test_no_anomalies_when_rate_zero():
    assert not any(r.is_anomaly for r in run(SensorSimulator(anomaly_rate=0.0)))


def test_anomalies_injected_and_still_schema_valid():
    readings = run(SensorSimulator(anomaly_rate=0.2), 1000)
    assert sum(r.is_anomaly for r in readings) > 50
    for r in readings:  # anomalies must not be rejected by validation
        SensorReading.model_validate(r.to_message())


def test_message_has_no_label():
    assert "is_anomaly" not in run(SensorSimulator(), 1)[0].to_message()


def test_drift_shifts_mean():
    sim = SensorSimulator(seed=3, drift_per_reading=0.01)
    temps = [r.temperature for r in run(sim, 1000)]
    assert sum(temps[-100:]) / 100 > sum(temps[:100]) / 100 + 5


def test_round_robin_sensors():
    ids = {r.sensor_id for r in run(SensorSimulator(num_sensors=3), 9)}
    assert ids == {"sensor-1", "sensor-2", "sensor-3"}
