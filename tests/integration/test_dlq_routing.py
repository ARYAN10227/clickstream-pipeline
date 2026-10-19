"""ClickConsumer end to end against real Redpanda + ClickHouse: valid rows vs DLQ routing."""

import random
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterator

import orjson
import pytest
from clickhouse_connect.driver.client import Client
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from helpers import wait_for

from clickpipe.config import Settings
from clickpipe.consumer import ClickConsumer
from clickpipe.dlq import DlqProducer
from clickpipe.fake import make_event
from clickpipe.sink import ClickHouseSink

pytestmark = pytest.mark.integration

N_VALID = 90


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _invalid_records(rng: random.Random) -> list[tuple[str, bytes]]:
    """Two records per DLQ reason, as (expected_reason, raw value)."""
    now = _now_ms()
    missing = make_event(rng, now)
    del missing["event_id"]
    missing2 = make_event(rng, now)
    del missing2["schema_version"]
    return [
        ("bad_json", b'{"event_id": '),
        ("bad_json", b"[1, 2, 3]"),
        ("missing_field", orjson.dumps(missing)),
        ("missing_field", orjson.dumps(missing2)),
        ("schema_invalid", orjson.dumps(make_event(rng, now) | {"short_code": "x" * 17})),
        ("schema_invalid", orjson.dumps(make_event(rng, now) | {"schema_version": 2})),
        # late/future are judged against produced_at: clicked at event_time, produced now
        ("late_event", orjson.dumps(make_event(rng, now - 400_000) | {"produced_at": now})),
        ("late_event", orjson.dumps(make_event(rng, now - 3_600_000) | {"produced_at": now})),
        ("future_event", orjson.dumps(make_event(rng, now + 120_000) | {"produced_at": now})),
        ("future_event", orjson.dumps(make_event(rng, now + 3_600_000) | {"produced_at": now})),
    ]


@pytest.fixture
def dlq_topic(kafka_bootstrap: str) -> Iterator[str]:
    """A second uniquely named topic to act as the DLQ."""
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    name = f"test-dlq-{uuid.uuid4().hex[:12]}"
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result(30)
    try:
        yield name
    finally:
        admin.delete_topics([name])[name].result(30)


def _read_all(bootstrap: str, topic: str, expected: int) -> list[dict[str, bytes]]:
    """Read `expected` records' headers from the start of a topic."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"test-reader-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    records: list[dict[str, bytes]] = []
    try:

        def read_more() -> bool:
            for msg in consumer.consume(num_messages=100, timeout=0.5):
                if msg.error() is None:
                    records.append(dict(msg.headers() or []))
            return len(records) >= expected

        wait_for(read_more, timeout_s=30, message=f"{expected} records on {topic}")
    finally:
        consumer.close()
    return records


def test_invalid_and_late_events_are_routed_to_dlq(
    kafka_bootstrap: str,
    fresh_topic: str,
    dlq_topic: str,
    ch_client: Client,
    fresh_table: str,
    new_ch_client: Callable[[], Client],
) -> None:
    rng = random.Random(5)
    invalid = _invalid_records(rng)
    producer = Producer({"bootstrap.servers": kafka_bootstrap})
    for _ in range(N_VALID):
        event = make_event(rng, _now_ms())
        producer.produce(fresh_topic, key=event["short_code"], value=orjson.dumps(event))
    for _, value in invalid:
        producer.produce(fresh_topic, key=b"bad", value=value, headers=[("run_id", b"dlq-test")])
    assert producer.flush(10) == 0

    settings = Settings(
        kafka_bootstrap=kafka_bootstrap,
        clicks_topic=fresh_topic,
        dlq_topic=dlq_topic,
        consumer_group=f"test-{uuid.uuid4().hex[:8]}",
        batch_max_wait_ms=200,
    )
    consumer = ClickConsumer(
        settings,
        ClickHouseSink(settings, table=fresh_table, client=new_ch_client()),
        DlqProducer(settings),
    )
    stop = threading.Event()
    errors: list[BaseException] = []

    def run() -> None:
        try:
            consumer.run(stop)
        except BaseException as exc:  # surface thread failures in the test
            errors.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    total = N_VALID + len(invalid)
    # Separate client in the same group, never subscribed: reads committed offsets without
    # touching the running consumer (one consumer object must not be shared across threads).
    offsets_reader = Consumer(
        {"bootstrap.servers": kafka_bootstrap, "group.id": settings.consumer_group}
    )
    try:

        def committed() -> int:
            assert not errors, errors
            parts = [TopicPartition(fresh_topic, p) for p in range(3)]
            return sum(max(tp.offset, 0) for tp in offsets_reader.committed(parts, timeout=5))

        wait_for(lambda: committed() == total, timeout_s=60, message=f"{total} committed offsets")
    finally:
        stop.set()
        thread.join(timeout=10)
        consumer.close()
        offsets_reader.close()
    assert not errors, errors

    assert ch_client.command(f"SELECT count() FROM {fresh_table} FINAL") == N_VALID

    records = _read_all(kafka_bootstrap, dlq_topic, expected=len(invalid))
    assert len(records) == len(invalid)
    assert Counter(r["dlq_reason"].decode() for r in records) == Counter(
        reason for reason, _ in invalid
    )
    for r in records:
        assert r["source_offset"].isdigit()
        assert r["source_topic"].decode() == fresh_topic
        assert r["run_id"] == b"dlq-test"
