"""Composition root for the sensor producer process."""

import signal
from types import FrameType

from common.config import load_settings
from common.logging_setup import setup_logging
from producer.cli import parse_options
from producer.service import ProducerService


def main() -> None:
    cfg = load_settings()
    options = parse_options(cfg)
    setup_logging()
    service = ProducerService(cfg, options)

    def stop(signum: int, frame: FrameType | None) -> None:
        service.stop()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    service.run()


if __name__ == "__main__":
    main()
