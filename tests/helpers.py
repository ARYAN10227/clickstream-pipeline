"""Shared test helpers."""

import importlib.util
import sys
import time
from collections.abc import Callable
from pathlib import Path
from types import ModuleType

import clickhouse_connect
import pytest
from clickhouse_connect.driver.client import Client
from confluent_kafka.admin import AdminClient

from clickpipe.config import Settings

TOOLS_DIR = Path(__file__).resolve().parents[1] / "tools"
REACH_TIMEOUT_S = 5


def wait_for[T](
    predicate: Callable[[], T],
    timeout_s: float = 30,
    interval_s: float = 0.2,
    message: str = "",
) -> T:
    """Poll predicate() until it returns a truthy value and return that value.

    Raises AssertionError on timeout, including the last value or exception seen.
    """
    deadline = time.monotonic() + timeout_s
    last_value: object = None
    last_error: Exception | None = None
    while True:
        try:
            value = predicate()
            if value:
                return value
            last_value, last_error = value, None
        except Exception as exc:  # keep polling; report the last error on timeout
            last_error = exc
        if time.monotonic() >= deadline:
            detail = f"last error: {last_error!r}" if last_error else f"last value: {last_value!r}"
            raise AssertionError(
                f"timed out after {timeout_s}s waiting for {message or 'condition'} ({detail})"
            )
        time.sleep(interval_s)


def require_stack(settings: Settings) -> Client:
    """Return a ClickHouse client, or skip the test if Redpanda or ClickHouse is not reachable."""
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


def load_tool(name: str) -> ModuleType:
    """Import tools/<name>.py as a module (tools/ is not a package)."""
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, TOOLS_DIR / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
