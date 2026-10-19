from typing import Any

from clickpipe.config import Settings
from clickpipe.schema import CLICK_COLUMNS
from clickpipe.sink import ClickHouseSink


class FakeClient:
    """Records insert() calls instead of talking to ClickHouse."""

    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[tuple[Any, ...]], dict[str, Any]]] = []
        self.closed = False

    def insert(self, table: str, rows: list[tuple[Any, ...]], **kwargs: Any) -> None:
        self.inserts.append((table, rows, kwargs))

    def close(self) -> None:
        self.closed = True


def _sink(client: FakeClient) -> ClickHouseSink:
    return ClickHouseSink(Settings(), table="analytics.t", client=client)  # type: ignore[arg-type]


def test_insert_called_once_with_click_columns() -> None:
    client = FakeClient()
    rows = [tuple(range(len(CLICK_COLUMNS))), tuple(range(len(CLICK_COLUMNS)))]

    sent = _sink(client).insert_rows(rows)

    assert sent == 2
    assert len(client.inserts) == 1
    table, inserted, kwargs = client.inserts[0]
    assert table == "analytics.t"
    assert inserted == rows
    assert kwargs["column_names"] == CLICK_COLUMNS
    assert kwargs["settings"] is None


def test_dedup_token_sets_both_clickhouse_settings() -> None:
    client = FakeClient()
    _sink(client).insert_rows([tuple(range(len(CLICK_COLUMNS)))], dedup_token="clicks|p0:1-1")

    assert client.inserts[0][2]["settings"] == {
        "insert_deduplicate": 1,
        "insert_deduplication_token": "clicks|p0:1-1",
    }


def test_empty_list_does_not_call_insert() -> None:
    client = FakeClient()
    assert _sink(client).insert_rows([]) == 0
    assert client.inserts == []


def test_close_closes_client() -> None:
    client = FakeClient()
    _sink(client).close()
    assert client.closed
