"""Guided demo: normal traffic -> anomalies -> gradual drift, ending with a drift alert.

    make demo            (or: python -m scripts.demo)

Stages (only the producer is restarted between them, with different fault settings):
  1. Normal traffic: no faults. The drift monitor should report "ok".
  2. Anomalies and malformed messages: the model flags anomalies, bad messages hit the DLQ.
  3. Gradual drift: sensor means creep upward. Once the live window differs enough from the
     training baseline, the drift monitor flags it (visible in the dashboard and the API).

Watch it at http://localhost:8501 (dashboard) and http://localhost:8000/docs (API).
Return to normal afterwards with `make demo-reset`.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

API = "http://localhost:8000"
DASHBOARD = "http://localhost:8501"

# Make the drift monitor react quickly: small window, frequent checks (defaults: 1000 / 30 s).
MONITOR_ENV = {"DRIFT_WINDOW_SIZE": "500", "DRIFT_CHECK_INTERVAL_SECONDS": "10"}


@dataclass(frozen=True)
class Phase:
    name: str
    explanation: str
    producer_env: dict[str, str]
    duration_s: int


PHASES = [
    Phase(
        "1/3 Normal traffic",
        "No faults. Establishes a clean live window for the drift monitor.",
        {"ANOMALY_RATE": "0", "INVALID_RATE": "0", "DRIFT_PER_READING": "0"},
        60,
    ),
    Phase(
        "2/3 Anomalies and malformed messages",
        "4% of readings become spikes/stuck values; 2% of messages are malformed.",
        {"ANOMALY_RATE": "0.04", "INVALID_RATE": "0.02", "DRIFT_PER_READING": "0"},
        40,
    ),
    Phase(
        "3/3 Gradual drift",
        "Sensor means creep upward (0.3 degrees/sec). Waiting for the drift monitor to flag it.",
        {"ANOMALY_RATE": "0.01", "INVALID_RATE": "0", "DRIFT_PER_READING": "0.03"},
        180,  # maximum wait; the phase ends as soon as drift is detected
    ),
]


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def compose(*args: str, env: dict[str, str]) -> None:
    """Run `docker compose ...`. Variables in `env` override .env (compose substitution)."""
    subprocess.run(["docker", "compose", *args], env={**os.environ, **env}, check=True)


def fetch(path: str) -> Any:
    with urllib.request.urlopen(API + path, timeout=5) as response:
        return json.load(response)  # parsed JSON: dict or list


def wait_for_api(timeout: float, fetcher: Callable[[str], Any] = fetch) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            fetcher("/health")
            return True
        except (urllib.error.URLError, OSError, ValueError):
            time.sleep(2)
    return False


def drift_summary(drift: dict[str, Any]) -> str:
    """One-line description of /metrics/drift."""
    flagged = [f["feature"] for f in drift.get("features", []) if f["drift_detected"]]
    if drift["status"] == "drift":
        return f"DRIFT in {len(flagged)} feature(s): {', '.join(flagged)}"
    return str(drift["status"])


def wait_for_drift(
    fetcher: Callable[[str], Any],
    timeout: float,
    interval: float = 5.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any] | None:
    """Poll /metrics/drift until status is "drift"; None if it never is within `timeout`."""
    deadline = clock() + timeout
    while True:
        drift: dict[str, Any] = fetcher("/metrics/drift")
        log(f"  drift status: {drift_summary(drift)}")
        if drift["status"] == "drift":
            return drift
        if clock() >= deadline:
            return None
        sleep(interval)


def pipeline_line(fetcher: Callable[[str], Any] = fetch) -> str:
    m = fetcher("/metrics/pipeline?window_seconds=60")
    p50 = m["latency_p50_s"]
    latency = "n/a" if p50 is None else f"{p50 * 1000:.0f} ms"
    return (
        f"{m['throughput_per_sec']:.1f} events/s, p50 latency {latency}, "
        f"{m['anomalies_in_window']} anomalies in last 60 s, "
        f"{m['dlq_total']} dead-lettered in total"
    )


def set_producer(env: dict[str, str]) -> None:
    compose("up", "-d", "--no-deps", "--force-recreate", "producer", env={**MONITOR_ENV, **env})


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n")[0])
    parser.add_argument("--no-build", action="store_true", help="skip building images")
    args = parser.parse_args()

    log("Starting the system (docker compose up)...")
    first = PHASES[0].producer_env
    compose("up", "-d", *([] if args.no_build else ["--build"]), env={**MONITOR_ENV, **first})
    if not wait_for_api(timeout=120):
        log("The API did not become ready. Check `docker compose ps` and `docker compose logs`.")
        return 2
    log(f"Dashboard: {DASHBOARD}    API docs: {API}/docs")

    drift: dict[str, Any] | None = None
    for i, phase in enumerate(PHASES):
        log(f"=== {phase.name} ===")
        log(phase.explanation)
        set_producer(phase.producer_env)
        if i < len(PHASES) - 1:
            time.sleep(phase.duration_s)
            log(f"  pipeline: {pipeline_line()}")
            log(f"  drift status: {drift_summary(fetch('/metrics/drift'))}")
        else:
            drift = wait_for_drift(fetch, timeout=phase.duration_s)

    if drift is None:
        log("Drift was NOT detected within the time limit. See docker compose logs monitoring.")
        return 1
    log("Drift detected, as intended. The system is still drifting so you can show the dashboard.")
    log("Return to normal traffic with: make demo-reset")
    return 0


if __name__ == "__main__":
    sys.exit(main())
