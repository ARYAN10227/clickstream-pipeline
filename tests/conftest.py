"""Shared fixtures: session-scoped Redpanda and ClickHouse containers for integration tests.

Image versions are read from docker-compose.yml so tests and the local stack never drift.
Without a working Docker daemon the container fixtures skip, so unit tests still run.
"""

import re
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import clickhouse_connect
import pytest
from clickhouse_connect.driver.client import Client
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "docker-compose.yml"
CREATE_CLICKS_SQL = ROOT / "sql" / "001_create_clicks.sql"
TIMEOUT_S = 30


def compose_image(repository: str) -> str:
    """Return the pinned image reference for `repository` from docker-compose.yml."""
    match = re.search(
        rf"^\s*image:\s*({re.escape(repository)}:\S+)\s*$", COMPOSE_FILE.read_text(), re.M
    )
    if match is None:
        raise RuntimeError(f"no image '{repository}:<tag>' found in {COMPOSE_FILE}")
    return match.group(1)


def _require_docker() -> None:
    """Skip the requesting test when no Docker daemon is reachable."""
    try:
        import docker

        docker.from_env().ping()
    except Exception as exc:
        pytest.skip(
            f"Docker is not available, skipping integration test ({exc.__class__.__name__})"
        )


def _connect(clickhouse: object) -> Client:
    """A new clickhouse-connect client for the session ClickHouse container."""
    return clickhouse_connect.get_client(
        host=clickhouse.get_container_host_ip(),  # type: ignore[attr-defined]
        port=int(clickhouse.get_exposed_port(8123)),  # type: ignore[attr-defined]
        username="clickpipe",
        password="clickpipe_test",
    )


def _sql_statements(path: Path) -> list[str]:
    """Split a SQL file into individual statements (no semicolons inside statements)."""
    return [stmt.strip() for stmt in path.read_text().split(";") if stmt.strip()]


@pytest.fixture(scope="session")
def redpanda() -> Iterator[object]:
    """Start one Redpanda container for the whole test session."""
    _require_docker()
    from testcontainers.community.kafka import RedpandaContainer

    container = RedpandaContainer(compose_image("redpandadata/redpanda"))
    container.start(timeout=60)
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def kafka_bootstrap(redpanda: object) -> str:
    """Bootstrap address (host:port) of the session Redpanda container."""
    return redpanda.get_bootstrap_server()  # type: ignore[attr-defined]


@pytest.fixture(scope="session")
def clickhouse() -> Iterator[object]:
    """Start one ClickHouse container for the whole test session."""
    _require_docker()
    from testcontainers.community.clickhouse import ClickHouseContainer

    container = ClickHouseContainer(
        compose_image("clickhouse/clickhouse-server"),
        username="clickpipe",
        password="clickpipe_test",
        dbname="analytics",
    )
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def ch_client(clickhouse: object) -> Client:
    """clickhouse-connect client with database analytics and table analytics.clicks created."""
    client = _connect(clickhouse)
    for statement in _sql_statements(CREATE_CLICKS_SQL):
        client.command(statement)
    return client


@pytest.fixture
def new_ch_client(ch_client: Client, clickhouse: object) -> Iterator[Callable[[], Client]]:
    """Factory for extra ClickHouse clients, all closed when the test ends.

    A clickhouse-connect client is one ClickHouse session and must not be used from two
    threads at once ("concurrent queries within the same session"). Give code that runs in
    a background thread (e.g. a ClickConsumer's sink) its own client from this factory.
    """
    clients: list[Client] = []

    def make() -> Client:
        client = _connect(clickhouse)
        clients.append(client)
        return client

    yield make
    for client in clients:
        client.close()


@pytest.fixture
def fresh_table(ch_client: Client) -> Iterator[str]:
    """Uniquely named empty copy of analytics.clicks; dropped after the test."""
    name = f"analytics.clicks_{uuid.uuid4().hex[:12]}"
    ch_client.command(f"CREATE TABLE {name} AS analytics.clicks")
    try:
        yield name
    finally:
        ch_client.command(f"DROP TABLE IF EXISTS {name} SYNC")


@pytest.fixture
def fresh_topic(kafka_bootstrap: str) -> Iterator[str]:
    """Uniquely named topic with 3 partitions; deleted after the test."""
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    name = f"test-{uuid.uuid4().hex[:12]}"
    admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1)])[name].result(
        timeout=TIMEOUT_S
    )
    try:
        yield name
    finally:
        admin.delete_topics([name])[name].result(timeout=TIMEOUT_S)
