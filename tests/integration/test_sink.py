"""Integration tests for ClickHouseSink against a real ClickHouse container."""

import random
import time

import orjson
import pytest
from clickhouse_connect.driver.client import Client

from clickpipe.config import Settings
from clickpipe.fake import make_event
from clickpipe.schema import parse_event, to_row
from clickpipe.sink import ClickHouseSink

pytestmark = pytest.mark.integration


def _rows(n: int) -> list[tuple[object, ...]]:
    rng = random.Random(11)
    now_ms = time.time_ns() // 1_000_000
    return [
        to_row(parse_event(orjson.dumps(make_event(rng, now_ms))), partition=0, offset=i)
        for i in range(n)
    ]


def test_insert_100_rows(ch_client: Client, fresh_table: str) -> None:
    sink = ClickHouseSink(Settings(), table=fresh_table, client=ch_client)

    assert sink.insert_rows(_rows(100)) == 100
    assert ch_client.command(f"SELECT count() FROM {fresh_table} FINAL") == 100


def test_ingested_at_filled_by_clickhouse(ch_client: Client, fresh_table: str) -> None:
    ClickHouseSink(Settings(), table=fresh_table, client=ch_client).insert_rows(_rows(10))

    fresh, stale = ch_client.query(
        f"SELECT countIf(ingested_at >= now64(3) - INTERVAL 1 MINUTE AND ingested_at <= now64(3)),"
        f" countIf(ingested_at < now64(3) - INTERVAL 1 MINUTE OR ingested_at > now64(3))"
        f" FROM {fresh_table}"
    ).result_rows[0]
    assert (fresh, stale) == (10, 0)


def test_empty_insert_is_noop(ch_client: Client, fresh_table: str) -> None:
    sink = ClickHouseSink(Settings(), table=fresh_table, client=ch_client)

    assert sink.insert_rows([]) == 0
    assert ch_client.command(f"SELECT count() FROM {fresh_table}") == 0
