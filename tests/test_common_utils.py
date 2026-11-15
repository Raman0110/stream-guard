import json
import logging

import pytest

from common.logging_setup import JsonFormatter
from common.retry import retry


def test_retry_succeeds_after_failures():
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("not yet")
        return "ok"

    assert retry(flaky, attempts=5, base_delay=0) == "ok"
    assert len(calls) == 3


def test_retry_gives_up_and_raises():
    def always_fails():
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        retry(always_fails, attempts=2, base_delay=0)


def test_json_formatter_includes_extra_fields():
    record = logging.LogRecord("svc", logging.INFO, "f.py", 1, "hello %s", ("x",), None)
    record.processed = 7
    out = json.loads(JsonFormatter().format(record))
    assert out["message"] == "hello x" and out["processed"] == 7 and out["level"] == "INFO"
