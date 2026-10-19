"""Both dedup layers from ADR-0003 against a real ClickHouse table."""

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

N_ROWS = 500


def _rows() -> list[tuple[object, ...]]:
    rng = random.Random(21)
    now_ms = time.time_ns() // 1_000_000
    return [
        to_row(parse_event(orjson.dumps(make_event(rng, now_ms))), partition=0, offset=i)
        for i in range(N_ROWS)
    ]


def test_fresh_table_keeps_dedup_window(ch_client: Client, fresh_table: str) -> None:
    ddl = ch_client.command(f"SHOW CREATE TABLE {fresh_table}")
    assert "non_replicated_deduplication_window = 1000" in ddl


def test_same_token_skips_duplicate_insert(ch_client: Client, fresh_table: str) -> None:
    sink = ClickHouseSink(Settings(), table=fresh_table, client=ch_client)
    rows = _rows()

    sink.insert_rows(rows, dedup_token="clicks|p0:0-499")
    sink.insert_rows(rows, dedup_token="clicks|p0:0-499")  # retry of the identical batch

    # Plain count() on purpose: the second insert must never have been stored at all.
    assert ch_client.command(f"SELECT count() FROM {fresh_table}") == N_ROWS


def test_different_token_duplicates_removed_by_replacing_merge_tree(
    ch_client: Client, fresh_table: str
) -> None:
    sink = ClickHouseSink(Settings(), table=fresh_table, client=ch_client)
    rows = _rows()

    sink.insert_rows(rows, dedup_token="clicks|p0:0-499")
    sink.insert_rows(rows, dedup_token="clicks|p0:0-250|p1:0-10")  # new batch boundaries

    assert ch_client.command(f"SELECT count() FROM {fresh_table}") == 2 * N_ROWS  # both stored
    assert ch_client.command(f"SELECT count() FROM {fresh_table} FINAL") == N_ROWS
    assert ch_client.command(f"SELECT uniqExact(event_id) FROM {fresh_table}") == N_ROWS
