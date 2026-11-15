"""Tiny retry-with-exponential-backoff helper for flaky startup dependencies."""

import logging
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")
log = logging.getLogger(__name__)


def retry(
    fn: Callable[[], T],
    *,
    attempts: int = 10,
    base_delay: float = 1.0,
    max_delay: float = 15.0,
    what: str = "operation",
) -> T:
    """Call fn until it succeeds; re-raise the last error after `attempts` tries."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt == attempts:
                raise
            delay = min(base_delay * 2 ** (attempt - 1), max_delay)
            log.warning(
                "%s failed, retrying",
                what,
                extra={"attempt": attempt, "retry_in_s": delay, "error": str(exc)},
            )
            time.sleep(delay)
    raise AssertionError("unreachable")  # keeps mypy happy
