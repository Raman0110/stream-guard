"""Composition root and scheduler for the distribution-drift worker."""

import logging
import signal
from pathlib import Path
from threading import Event
from types import FrameType

from common.config import load_settings
from common.logging_setup import setup_logging
from common.tables import connect_from_settings
from monitoring.service import load_baseline, run_check

log = logging.getLogger("monitoring")


def main() -> None:
    setup_logging()
    cfg = load_settings()
    engine = connect_from_settings(cfg)
    try:
        baseline = load_baseline(Path(cfg.baseline_features_path))
        stopped = Event()

        def stop(signum: int, frame: FrameType | None) -> None:
            stopped.set()

        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        log.info(
            "drift monitor started",
            extra={
                "baseline_rows": len(baseline),
                "window_size": cfg.drift_window_size,
                "interval_s": cfg.drift_check_interval_seconds,
            },
        )

        while not stopped.is_set():
            try:
                run_check(engine, baseline, cfg.drift_window_size)
            except Exception:
                log.exception("drift check failed")
            stopped.wait(cfg.drift_check_interval_seconds)
    finally:
        engine.dispose()
        log.info("drift monitor stopped")


if __name__ == "__main__":
    main()
