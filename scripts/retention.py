"""Preview or apply bounded retention cleanup for StreamGuard database records."""

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import Connection, Engine, delete, func, select
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.schema import Column, Table

from common.config import Settings, load_settings
from common.tables import (
    connect_from_settings,
    dlq_events,
    drift_metrics,
    outbox_events,
    predictions,
    readings,
)


@dataclass(frozen=True)
class RetentionPolicy:
    readings_days: int
    dlq_days: int
    drift_days: int
    outbox_days: int
    batch_size: int

    def __post_init__(self) -> None:
        values = (self.readings_days, self.dlq_days, self.drift_days, self.outbox_days)
        if any(days <= 0 for days in values):
            raise ValueError("retention periods must be positive numbers of days")
        if self.batch_size <= 0:
            raise ValueError("retention batch size must be positive")

    @classmethod
    def from_settings(cls, settings: Settings) -> "RetentionPolicy":
        return cls(
            readings_days=settings.readings_retention_days,
            dlq_days=settings.dlq_retention_days,
            drift_days=settings.drift_retention_days,
            outbox_days=settings.outbox_retention_days,
            batch_size=settings.retention_batch_size,
        )


def _conditions(
    policy: RetentionPolicy, now: datetime
) -> dict[str, tuple[Table, ColumnElement[bool]]]:
    return {
        "readings": (
            readings,
            readings.c.ingested_at < now - timedelta(days=policy.readings_days),
        ),
        "dlq_events": (
            dlq_events,
            dlq_events.c.received_at < now - timedelta(days=policy.dlq_days),
        ),
        "drift_metrics": (
            drift_metrics,
            drift_metrics.c.computed_at < now - timedelta(days=policy.drift_days),
        ),
        "outbox_events": (
            outbox_events,
            (outbox_events.c.published_at.is_not(None))
            & (outbox_events.c.published_at < now - timedelta(days=policy.outbox_days)),
        ),
    }


def preview_expired(
    engine: Engine, policy: RetentionPolicy, *, now: datetime | None = None
) -> dict[str, int]:
    """Count rows eligible for deletion without modifying the database."""
    effective_now = now or datetime.now(UTC)
    with engine.connect() as conn:
        return {
            name: int(
                conn.execute(select(func.count()).select_from(table).where(condition)).scalar_one()
            )
            for name, (table, condition) in _conditions(policy, effective_now).items()
        }


def _delete_batch(
    conn: Connection,
    table: Table,
    id_column: Column[int],
    condition: ColumnElement[bool],
    batch_size: int,
) -> int:
    ids = list(
        conn.execute(
            select(id_column).where(condition).order_by(id_column).limit(batch_size)
        ).scalars()
    )
    if not ids:
        return 0
    if table is readings:
        conn.execute(delete(predictions).where(predictions.c.reading_id.in_(ids)))
    conn.execute(delete(table).where(id_column.in_(ids)))
    return len(ids)


def purge_expired(
    engine: Engine, policy: RetentionPolicy, *, now: datetime | None = None
) -> dict[str, int]:
    """Delete expired rows in small transactions and return per-table totals."""
    effective_now = now or datetime.now(UTC)
    deleted: dict[str, int] = {}
    for name, (table, condition) in _conditions(policy, effective_now).items():
        total = 0
        while True:
            with engine.begin() as conn:
                batch = _delete_batch(conn, table, table.c.id, condition, policy.batch_size)
            total += batch
            if batch < policy.batch_size:
                break
        deleted[name] = total
    return deleted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="delete eligible rows; without this flag only counts are shown",
    )
    args = parser.parse_args()
    settings = load_settings()
    policy = RetentionPolicy.from_settings(settings)
    engine = connect_from_settings(settings)
    try:
        result = purge_expired(engine, policy) if args.apply else preview_expired(engine, policy)
    finally:
        engine.dispose()
    print(
        json.dumps({"mode": "apply" if args.apply else "preview", "rows": result}, sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
