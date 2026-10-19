"""Smoke tests for the shared integration fixtures."""

import uuid
from datetime import UTC, datetime

import pytest
from clickhouse_connect.driver.client import Client
from confluent_kafka import Consumer, Message, Producer
from helpers import wait_for

pytestmark = pytest.mark.integration


def test_produce_and_consume_round_trip(kafka_bootstrap: str, fresh_topic: str) -> None:
    payload = f'{{"event_id": "{uuid.uuid4()}"}}'.encode()
    producer = Producer({"bootstrap.servers": kafka_bootstrap})
    producer.produce(fresh_topic, key=b"abc123", value=payload)
    assert producer.flush(10) == 0, "message was not delivered"

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([fresh_topic])
    try:

        def poll_one() -> Message | None:
            msg = consumer.poll(0.5)
            return msg if msg is not None and msg.error() is None else None

        msg = wait_for(poll_one, timeout_s=30, message=f"a message on {fresh_topic}")
    finally:
        consumer.close()

    assert msg.key() == b"abc123"
    assert msg.value() == payload


def test_insert_and_read_back(ch_client: Client, fresh_table: str) -> None:
    event_id = uuid.uuid4()
    now = datetime.now(UTC)
    row = [event_id, "abc123", now, now, now, "example.com", "mobile", "Chrome", "DE", 1, 0, 42]
    ch_client.insert(
        fresh_table,
        [row],
        column_names=[
            "event_id",
            "short_code",
            "event_time",
            "produced_at",
            "ingested_at",
            "referrer_domain",
            "device",
            "browser",
            "country",
            "schema_version",
            "kafka_partition",
            "kafka_offset",
        ],
    )

    result = ch_client.query(
        f"SELECT event_id, short_code, country, kafka_offset FROM {fresh_table}"
    ).result_rows
    assert result == [(event_id, "abc123", "DE", 42)]
