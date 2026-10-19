"""Shared test helpers."""

import time
from collections.abc import Callable


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
