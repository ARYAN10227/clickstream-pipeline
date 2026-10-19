"""Smoke-check the local stack: Redpanda topics and the ClickHouse clicks table.

Usage: uv run python tools/smoke.py   (exit code 0 only if every check passes)
"""

import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, ClusterMetadata

from clickpipe.config import Settings, get_settings

TIMEOUT_S = 10
CLICKS_PARTITIONS = 6

# Readable reasons for common low-level connection errors.
KNOWN_ERRORS = {
    "Connection refused": "connection refused",
    "Failed to establish a new connection": "connection refused",
    "timed out": "timed out",
    "Name or service not known": "unknown host",
    "Authentication failed": "authentication failed (check user/password)",
}


class CheckFailed(Exception):
    """A check failed for a known, human-readable reason."""


@dataclass
class Result:
    name: str
    passed: bool
    detail: str


def _short_error(exc: Exception) -> str:
    """Reduce an exception to its first line, without tracebacks or long URLs."""
    if isinstance(exc, KafkaException) and exc.args:
        err = exc.args[0]
        return getattr(err, "str", lambda: str(err))()
    text = str(exc).strip()
    for needle, reason in KNOWN_ERRORS.items():
        if needle in text:
            return reason
    return text.splitlines()[0][:160] if text else type(exc).__name__


def _run(name: str, check: Callable[[], str]) -> Result:
    """Run one check, turning any exception into a FAIL row."""
    try:
        return Result(name, True, check())
    except CheckFailed as exc:
        return Result(name, False, str(exc))
    except Exception as exc:  # every failure must become a readable row
        return Result(name, False, _short_error(exc))


def _kafka_metadata(settings: Settings) -> ClusterMetadata:
    admin = AdminClient(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "socket.timeout.ms": TIMEOUT_S * 1000,
            "log_level": 0,  # keep librdkafka connection noise out of the table
        }
    )
    try:
        return admin.list_topics(timeout=TIMEOUT_S)
    except KafkaException as exc:
        raise CheckFailed(
            f"cannot reach Redpanda at {settings.kafka_bootstrap} within {TIMEOUT_S}s "
            f"({_short_error(exc)})"
        ) from exc


def _clickhouse_client(settings: Settings) -> Client:
    try:
        return clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            connect_timeout=TIMEOUT_S,
            send_receive_timeout=TIMEOUT_S,
        )
    except Exception as exc:
        raise CheckFailed(
            f"cannot connect to ClickHouse at "
            f"{settings.clickhouse_host}:{settings.clickhouse_port} ({_short_error(exc)})"
        ) from exc


def run_checks(settings: Settings) -> list[Result]:
    """Run all checks in order; later checks report a skip when a dependency failed."""
    results: list[Result] = []
    meta: list[ClusterMetadata] = []
    ch: list[Client] = []

    def redpanda_reachable() -> str:
        meta.append(_kafka_metadata(settings))
        return f"{settings.kafka_bootstrap}, {len(meta[0].brokers)} broker(s)"

    def topic_check(topic: str, partitions: int | None) -> Callable[[], str]:
        def check() -> str:
            if not meta:
                raise CheckFailed("skipped: Redpanda is not reachable")
            info = meta[0].topics.get(topic)
            if info is None:
                raise CheckFailed(f"topic '{topic}' does not exist (run: make up)")
            count = len(info.partitions)
            if partitions is not None and count != partitions:
                raise CheckFailed(f"topic '{topic}' has {count} partitions, expected {partitions}")
            return f"{count} partition(s)"

        return check

    def clickhouse_select_1() -> str:
        ch.append(_clickhouse_client(settings))
        value = ch[0].command("SELECT 1")
        if str(value) != "1":
            raise CheckFailed(f"SELECT 1 returned {value!r}")
        address = f"{settings.clickhouse_host}:{settings.clickhouse_port}"
        return f"{address}, version {ch[0].server_version}"

    def clicks_table() -> str:
        if not ch:
            raise CheckFailed("skipped: ClickHouse is not reachable")
        table = f"{settings.clickhouse_db}.clicks"
        if str(ch[0].command(f"EXISTS TABLE {table}")) != "1":
            raise CheckFailed(f"table {table} does not exist (see sql/001_create_clicks.sql)")
        return table

    results.append(_run("Redpanda reachable", redpanda_reachable))
    results.append(
        _run(
            f"Topic {settings.clicks_topic}",
            topic_check(settings.clicks_topic, CLICKS_PARTITIONS),
        )
    )
    results.append(_run(f"Topic {settings.dlq_topic}", topic_check(settings.dlq_topic, None)))
    results.append(_run("ClickHouse SELECT 1", clickhouse_select_1))
    results.append(_run(f"Table {settings.clickhouse_db}.clicks", clicks_table))
    return results


def print_table(results: list[Result]) -> None:
    """Print results as an aligned check | status | detail table."""
    width = max(len("check"), *(len(r.name) for r in results))
    print(f"{'check':<{width}} | status | detail")
    print(f"{'-' * width}-+--------+-{'-' * 40}")
    for r in results:
        print(f"{r.name:<{width}} | {'PASS' if r.passed else 'FAIL':<6} | {r.detail}")


def main() -> int:
    """Run the smoke checks and return the process exit code."""
    logging.getLogger("clickhouse_connect").setLevel(logging.CRITICAL)
    results = run_checks(get_settings())
    print_table(results)
    failed = [r for r in results if not r.passed]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
