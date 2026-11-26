"""Create the StreamGuard storage schema and operational indexes.

Revision ID: 0001
Revises: None
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _create_index_if_missing(name: str, table: str, columns: list[str]) -> None:
    existing = {index["name"] for index in inspect(op.get_bind()).get_indexes(table)}
    if name not in existing:
        op.create_index(name, table, columns, unique=False)


def upgrade() -> None:
    existing = set(inspect(op.get_bind()).get_table_names())

    if "readings" not in existing:
        op.create_table(
            "readings",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("sensor_id", sa.String(length=50), nullable=False),
            sa.Column("ts", sa.DateTime(timezone=True), nullable=False),
            sa.Column("temperature", sa.Float(), nullable=False),
            sa.Column("humidity", sa.Float(), nullable=False),
            sa.Column("vibration", sa.Float(), nullable=False),
            sa.Column(
                "ingested_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.UniqueConstraint("sensor_id", "ts", name="uq_readings_sensor_ts"),
        )

    if "predictions" not in existing:
        op.create_table(
            "predictions",
            sa.Column(
                "reading_id",
                sa.Integer(),
                sa.ForeignKey("readings.id"),
                primary_key=True,
            ),
            sa.Column("anomaly_score", sa.Float(), nullable=False),
            sa.Column("is_anomaly", sa.Boolean(), nullable=False),
            sa.Column("model_version", sa.String(length=32), nullable=False),
            sa.Column(
                "scored_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
        )

    if "dlq_events" not in existing:
        op.create_table(
            "dlq_events",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column(
                "received_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.Column("raw_payload", sa.Text(), nullable=False),
            sa.Column("error", sa.Text(), nullable=False),
        )

    if "outbox_events" not in existing:
        op.create_table(
            "outbox_events",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("topic", sa.String(length=255), nullable=False),
            sa.Column("message_key", sa.LargeBinary(), nullable=True),
            sa.Column("payload", sa.LargeBinary(), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
            sa.Column("last_error", sa.Text(), nullable=True),
        )

    if "drift_metrics" not in existing:
        op.create_table(
            "drift_metrics",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column(
                "computed_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.Column("feature", sa.String(length=50), nullable=False),
            sa.Column("ks_statistic", sa.Float(), nullable=False),
            sa.Column("ks_pvalue", sa.Float(), nullable=False),
            sa.Column("psi", sa.Float(), nullable=False),
            sa.Column("drift_detected", sa.Boolean(), nullable=False),
            sa.Column("window_size", sa.Integer(), nullable=False),
        )

    _create_index_if_missing("ix_readings_ingested_at", "readings", ["ingested_at"])
    _create_index_if_missing("ix_predictions_is_anomaly", "predictions", ["is_anomaly"])
    _create_index_if_missing("ix_dlq_events_received_at", "dlq_events", ["received_at"])
    _create_index_if_missing("ix_outbox_events_published_at", "outbox_events", ["published_at"])
    _create_index_if_missing(
        "ix_drift_metrics_feature_computed_at",
        "drift_metrics",
        ["feature", "computed_at"],
    )


def downgrade() -> None:
    op.drop_table("drift_metrics")
    op.drop_table("outbox_events")
    op.drop_table("dlq_events")
    op.drop_table("predictions")
    op.drop_table("readings")
