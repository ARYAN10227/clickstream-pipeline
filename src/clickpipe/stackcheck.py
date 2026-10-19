"""Helpers for checking a running stack: consumer lag and per-run ClickHouse counts.

Shared by tools/verify.py and tools/crash_test.py.
"""

import time

import clickhouse_connect
from confluent_kafka import Consumer, TopicPartition

from clickpipe.config import Settings

PROGRESS_EVERY_S = 5


def partitions(consumer: Consumer, topic: str) -> list[TopicPartition]:
    """All partitions of `topic`, sorted by number."""
    meta = consumer.list_topics(topic, timeout=10).topics[topic]
    if meta.error is not None:
        raise RuntimeError(f"topic {topic}: {meta.error}")
    return [TopicPartition(topic, p) for p in sorted(meta.partitions)]


def total_lag(settings: Settings) -> int:
    """Uncommitted messages for the consumer group across every clicks partition."""
    reader = Consumer(
        {"bootstrap.servers": settings.kafka_bootstrap, "group.id": settings.consumer_group}
    )
    try:
        parts = partitions(reader, settings.clicks_topic)
        lag = 0
        for tp in reader.committed(parts, timeout=10):
            low, high = reader.get_watermark_offsets(tp, timeout=10)
            position = tp.offset if tp.offset >= 0 else low  # no commit yet: start of log
            lag += max(0, high - position)
        return lag
    finally:
        reader.close()


def wait_for_zero_lag(settings: Settings, timeout_s: float) -> int:
    """Poll until lag is 0 or the timeout passes; returns the last lag seen."""
    deadline = time.monotonic() + timeout_s
    next_report = 0.0
    while True:
        lag = total_lag(settings)
        now = time.monotonic()
        if lag == 0 or now >= deadline:
            print(f"consumer lag: {lag:,}" + ("" if lag == 0 else " (timed out)"))
            return lag
        if now >= next_report:
            print(f"waiting for consumer, lag: {lag:,}")
            next_report = now + PROGRESS_EVERY_S
        time.sleep(1)


def clickhouse_counts(settings: Settings, prefix: str) -> tuple[int, int, int]:
    """(raw count, FINAL count, uniqExact(event_id)) for rows of this run."""
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )
    try:
        where = "startsWith(short_code, {prefix:String})"
        params = {"prefix": prefix}
        raw = client.command(f"SELECT count() FROM analytics.clicks WHERE {where}", params)
        final = client.command(f"SELECT count() FROM analytics.clicks FINAL WHERE {where}", params)
        unique = client.command(
            f"SELECT uniqExact(event_id) FROM analytics.clicks WHERE {where}", params
        )
        return int(raw), int(final), int(unique)
    finally:
        client.close()
