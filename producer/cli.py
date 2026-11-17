"""Command-line parsing for the producer process."""

import argparse

from common.config import Settings
from producer.service import ProducerOptions


def parse_options(cfg: Settings) -> ProducerOptions:
    parser = argparse.ArgumentParser(description="Publish simulated sensor readings to Kafka.")
    parser.add_argument("--rate", type=float, default=cfg.events_per_second, help="events/sec")
    parser.add_argument("--sensors", type=int, default=cfg.num_sensors)
    parser.add_argument("--seed", type=int, default=cfg.random_seed)
    parser.add_argument("--anomaly-rate", type=float, default=cfg.anomaly_rate)
    parser.add_argument("--drift-per-reading", type=float, default=cfg.drift_per_reading)
    parser.add_argument("--invalid-rate", type=float, default=cfg.invalid_rate)
    parser.add_argument(
        "--max-events", type=int, default=0, help="stop after N events (0 = run forever)"
    )
    args = parser.parse_args()
    return ProducerOptions(
        rate=args.rate,
        sensors=args.sensors,
        seed=args.seed,
        anomaly_rate=args.anomaly_rate,
        drift_per_reading=args.drift_per_reading,
        invalid_rate=args.invalid_rate,
        max_events=args.max_events,
    )
