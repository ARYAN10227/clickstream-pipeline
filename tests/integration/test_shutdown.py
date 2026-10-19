"""Graceful stop: everything consumed is inserted and committed before the loop exits."""

import random
import threading
import time
import uuid

import orjson
import pytest
from clickhouse_connect.driver.client import Client
from confluent_kafka import Consumer, Producer, TopicPartition
from helpers import wait_for

from clickpipe.config import Settings
from clickpipe.consumer import ClickConsumer
from clickpipe.dlq import DlqProducer
from clickpipe.fake import make_event
from clickpipe.sink import ClickHouseSink

pytestmark = pytest.mark.integration

N_EVENTS = 5000
PARTITIONS = 3  # fresh_topic is created with 3 partitions


def test_stop_leaves_nothing_uncommitted(
    kafka_bootstrap: str, fresh_topic: str, ch_client: Client, fresh_table: str
) -> None:
    settings = Settings(
        kafka_bootstrap=kafka_bootstrap,
        clicks_topic=fresh_topic,
        consumer_group=f"test-{uuid.uuid4().hex[:8]}",
        batch_max_size=500,
        batch_max_wait_ms=200,
    )
    consumer = ClickConsumer(
        settings,
        ClickHouseSink(settings, table=fresh_table, client=ch_client),
        DlqProducer(settings),
    )
    stop = threading.Event()
    errors: list[BaseException] = []

    def run() -> None:
        try:
            consumer.run(stop)
        except BaseException as exc:  # surface thread failures in the test
            errors.append(exc)
        finally:
            consumer.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()

    rng = random.Random(9)
    producer = Producer({"bootstrap.servers": kafka_bootstrap})
    for _ in range(N_EVENTS):
        event = make_event(rng, time.time_ns() // 1_000_000)
        producer.produce(fresh_topic, key=event["short_code"], value=orjson.dumps(event))
        producer.poll(0)
    assert producer.flush(10) == 0

    wait_for(
        lambda: (
            not errors and ch_client.command(f"SELECT count() FROM {fresh_table} FINAL") == N_EVENTS
        ),
        timeout_s=60,
        message=f"{N_EVENTS} rows in {fresh_table}",
    )
    stop.set()
    thread.join(timeout=30)
    assert not thread.is_alive(), "consumer did not stop within 30 s"
    assert not errors, errors

    reader = Consumer({"bootstrap.servers": kafka_bootstrap, "group.id": settings.consumer_group})
    try:
        parts = [TopicPartition(fresh_topic, p) for p in range(PARTITIONS)]
        committed = {tp.partition: tp.offset for tp in reader.committed(parts, timeout=10)}
        high = {p: reader.get_watermark_offsets(tp, timeout=10)[1] for p, tp in enumerate(parts)}
    finally:
        reader.close()
    assert committed == high, f"committed {committed} != high watermarks {high}"
    assert sum(high.values()) == N_EVENTS
    assert ch_client.command(f"SELECT count() FROM {fresh_table} FINAL") == N_EVENTS
