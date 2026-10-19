"""End-to-end: events produced to Redpanda land in ClickHouse via the running consumer.

Needs the full stack: run `make up` first, then `make test-e2e`.
"""

import itertools
import random
import string
from collections.abc import Iterator
from typing import Any

import clickhouse_connect
import pytest
from clickhouse_connect.driver.client import Client
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from helpers import wait_for

from clickpipe.config import Settings, get_settings
from clickpipe.fake import make_event
from clickpipe.producer import now_ms, send_events

pytestmark = pytest.mark.e2e

N_EVENTS = 20_000
REACH_TIMEOUT_S = 5


def _stack_or_skip(settings: Settings) -> Client:
    """Return a ClickHouse client, or skip if Redpanda or ClickHouse is not reachable."""
    hint = "start the stack with make up"
    try:
        AdminClient({"bootstrap.servers": settings.kafka_bootstrap, "log_level": 0}).list_topics(
            timeout=REACH_TIMEOUT_S
        )
    except Exception as exc:
        pytest.skip(f"Redpanda not reachable at {settings.kafka_bootstrap} ({exc}); {hint}")
    try:
        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_db,
            connect_timeout=REACH_TIMEOUT_S,
        )
        client.command("SELECT 1")
    except Exception as exc:
        address = f"{settings.clickhouse_host}:{settings.clickhouse_port}"
        pytest.skip(f"ClickHouse not reachable at {address} ({type(exc).__name__}); {hint}")
    return client


def test_produced_events_land_in_clickhouse() -> None:
    settings = get_settings()
    ch = _stack_or_skip(settings)
    rng = random.Random()
    prefix = "e" + "".join(rng.choices(string.ascii_lowercase, k=5))

    def events() -> Iterator[dict[str, Any]]:
        while True:
            yield make_event(rng, now_ms(), code_prefix=prefix)

    result = send_events(
        Producer(settings.producer_config()),
        settings.clicks_topic,
        itertools.islice(events(), N_EVENTS),
        rate=20_000,
    )
    assert (result.sent, result.delivery_errors) == (N_EVENTS, 0)

    where = f"startsWith(short_code, '{prefix}')"
    wait_for(
        lambda: (
            ch.command(f"SELECT uniqExact(event_id) FROM analytics.clicks WHERE {where}")
            == N_EVENTS
        ),
        timeout_s=90,
        message=f"{N_EVENTS} unique events with prefix {prefix!r} in analytics.clicks",
    )
    assert ch.command(f"SELECT count() FROM analytics.clicks FINAL WHERE {where}") == N_EVENTS
