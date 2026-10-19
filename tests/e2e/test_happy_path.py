"""End-to-end: events produced to Redpanda land in ClickHouse via the running consumer.

Needs the full stack: run `make up` first, then `make test-e2e`.
"""

import itertools
import random
import string
from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka import Producer
from helpers import require_stack, wait_for

from clickpipe.config import get_settings
from clickpipe.fake import make_event
from clickpipe.producer import now_ms, send_events

pytestmark = pytest.mark.e2e

N_EVENTS = 20_000


def test_produced_events_land_in_clickhouse() -> None:
    settings = get_settings()
    ch = require_stack(settings)
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
