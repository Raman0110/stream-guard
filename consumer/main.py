"""Composition root for the Kafka consumer process."""

import logging
import signal
from pathlib import Path
from types import FrameType

from common.config import load_settings
from common.logging_setup import setup_logging
from common.tables import connect
from consumer.service import ConsumerService
from ml.model import Scorer, load_artifact

log = logging.getLogger("consumer")


def main() -> None:
    setup_logging()
    cfg = load_settings()
    scorer = Scorer(load_artifact(Path(cfg.model_path)))
    log.info("model loaded", extra={"model_version": scorer.version})
    service = ConsumerService(cfg, connect(cfg.database_url), scorer)

    def stop(signum: int, frame: FrameType | None) -> None:
        service.stop()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    service.run()


if __name__ == "__main__":
    main()
