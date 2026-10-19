"""Batch sink that writes click rows to ClickHouse over one long-lived client."""

import logging
import random
import time
from collections.abc import Callable, Sequence
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import Error as ClickHouseDriverError
from clickhouse_connect.driver.exceptions import OperationalError

from clickpipe.config import Settings
from clickpipe.schema import CLICK_COLUMNS

log = logging.getLogger(__name__)

# ClickHouse server error codes that describe a temporary condition worth retrying.
TRANSIENT_CLICKHOUSE_CODES = frozenset(
    {
        3,  # UNEXPECTED_END_OF_FILE
        159,  # TIMEOUT_EXCEEDED
        202,  # TOO_MANY_SIMULTANEOUS_QUERIES
        209,  # SOCKET_TIMEOUT
        210,  # NETWORK_ERROR
        241,  # MEMORY_LIMIT_EXCEEDED
        242,  # TABLE_IS_READ_ONLY
        252,  # TOO_MANY_PARTS
        319,  # UNKNOWN_STATUS_OF_INSERT
        425,  # SYSTEM_ERROR
        999,  # KEEPER_EXCEPTION
    }
)


def is_transient(exc: BaseException) -> bool:
    """True for connection/timeout/network failures and temporary ClickHouse server errors."""
    if isinstance(exc, ConnectionError | TimeoutError | OSError):
        return True
    if isinstance(exc, ClickHouseDriverError):
        if exc.code is None:
            # No server error code: a transport-level failure (refused, reset, timed out).
            return isinstance(exc, OperationalError)
        return exc.code in TRANSIENT_CLICKHOUSE_CODES
    return False


class ClickHouseSink:
    """Inserts batches of rows (in CLICK_COLUMNS order) into one ClickHouse table."""

    def __init__(
        self,
        settings: Settings,
        table: str = "analytics.clicks",
        client: Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        """Use the injected client, or create exactly one from settings.

        `sleep` and `rng` are injectable so tests can run the retry logic instantly.
        """
        self.table = table
        self.max_attempts = max(1, settings.insert_max_retries)
        self.retry_base_s = settings.retry_base_s
        self.retry_max_s = settings.retry_max_s
        self._sleep = sleep
        self._rng = rng or random.Random()
        self.client: Client = client or clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_db,
        )

    def backoff_s(self, attempt: int) -> float:
        """Delay before retry number `attempt` (0-based): capped exponential, 50-100% jitter."""
        return min(self.retry_max_s, self.retry_base_s * 2**attempt) * self._rng.uniform(0.5, 1.0)

    def insert_rows(self, rows: Sequence[tuple[Any, ...]], dedup_token: str | None = None) -> int:
        """Insert rows in one request and return how many were sent; empty input is a no-op.

        With a dedup_token, ClickHouse skips the insert if a block with the same token was
        already inserted (within the table's non_replicated_deduplication_window), so
        retrying the same batch is harmless (ADR-0003, layer 1).

        Transient failures are retried up to insert_max_retries attempts in total with
        exponential backoff; the last error is re-raised. Other errors are raised at once.
        """
        if not rows:
            return 0
        settings = (
            {"insert_deduplicate": 1, "insert_deduplication_token": dedup_token}
            if dedup_token is not None
            else None
        )
        for attempt in range(self.max_attempts):
            try:
                self.client.insert(self.table, rows, column_names=CLICK_COLUMNS, settings=settings)
                return len(rows)
            except Exception as exc:
                if not is_transient(exc) or attempt == self.max_attempts - 1:
                    raise
                delay = self.backoff_s(attempt)
                log.warning(
                    "insert attempt %d/%d failed (%s); retrying in %.2fs",
                    attempt + 1,
                    self.max_attempts,
                    type(exc).__name__,
                    delay,
                )
                self._sleep(delay)
        raise AssertionError("unreachable")  # the loop always returns or raises

    def close(self) -> None:
        """Close the underlying client."""
        self.client.close()
