"""Kafka helpers shared by producer and consumer."""

import logging

from confluent_kafka.admin import AdminClient, NewTopic

from common.retry import retry

log = logging.getLogger(__name__)


def ensure_topics(bootstrap_servers: str, topics: list[str]) -> None:
    """Create topics if missing (idempotent). Retries while the broker is starting."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    def _create() -> None:
        existing = admin.list_topics(timeout=5).topics
        missing = [
            NewTopic(t, num_partitions=1, replication_factor=1) for t in topics if t not in existing
        ]
        if not missing:
            return
        for topic, future in admin.create_topics(missing).items():
            future.result()  # raises if creation failed
            log.info("created topic", extra={"topic": topic})

    retry(_create, what="ensure_topics")
