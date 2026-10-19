"""Batch sink that writes click rows to ClickHouse over one long-lived client."""

from collections.abc import Sequence
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from clickpipe.config import Settings
from clickpipe.schema import CLICK_COLUMNS


class ClickHouseSink:
    """Inserts batches of rows (in CLICK_COLUMNS order) into one ClickHouse table."""

    def __init__(
        self, settings: Settings, table: str = "analytics.clicks", client: Client | None = None
    ) -> None:
        """Use the injected client, or create exactly one from settings."""
        self.table = table
        self.client: Client = client or clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_db,
        )

    def insert_rows(self, rows: Sequence[tuple[Any, ...]], dedup_token: str | None = None) -> int:
        """Insert rows in one request and return how many were sent; empty input is a no-op.

        With a dedup_token, ClickHouse skips the insert if a block with the same token was
        already inserted (within the table's non_replicated_deduplication_window), so
        retrying the same batch is harmless (ADR-0003, layer 1).
        """
        if not rows:
            return 0
        settings = (
            {"insert_deduplicate": 1, "insert_deduplication_token": dedup_token}
            if dedup_token is not None
            else None
        )
        self.client.insert(self.table, rows, column_names=CLICK_COLUMNS, settings=settings)
        return len(rows)

    def close(self) -> None:
        """Close the underlying client."""
        self.client.close()
