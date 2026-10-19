"""Consume click events from Kafka in batches and write them to ClickHouse.

Delivery is at-least-once (ADR-0002): offsets are committed only after the batch
is stored, so a crash causes redelivery, never loss.
"""

import logging
import threading
from collections.abc import Sequence
from typing import Any

import orjson
from confluent_kafka import Consumer, KafkaError, Message
from pydantic import ValidationError

from clickpipe.config import Settings
from clickpipe.schema import parse_event, to_row
from clickpipe.sink import ClickHouseSink

log = logging.getLogger(__name__)


def _short_error(exc: Exception) -> str:
    """Summarise a parse/validation failure in one line."""
    if isinstance(exc, orjson.JSONDecodeError):
        return f"bad JSON: {exc}"
    if isinstance(exc, ValidationError):
        first = exc.errors()[0]
        field = ".".join(str(part) for part in first["loc"]) or "<root>"
        more = f" (+{exc.error_count() - 1} more)" if exc.error_count() > 1 else ""
        return f"invalid {field}: {first['msg']}{more}"
    return f"{type(exc).__name__}: {exc}"


def process_batch(
    messages: Sequence[Message],
) -> tuple[list[tuple[Any, ...]], list[tuple[Message, str]]]:
    """Split a polled batch into ClickHouse rows and (message, reason) pairs for invalid ones."""
    rows: list[tuple[Any, ...]] = []
    invalid: list[tuple[Message, str]] = []
    for msg in messages:
        err = msg.error()
        if err is not None:
            if err.code() != KafkaError._PARTITION_EOF:
                log.warning("skipping Kafka error message: %s", err)
            continue
        try:
            event = parse_event(msg.value())
        except Exception as exc:  # any bad payload is reported, never fatal
            reason = _short_error(exc)
            log.warning(
                "invalid record %s[%d]@%d: %s", msg.topic(), msg.partition(), msg.offset(), reason
            )
            # TODO: route invalid records to the dead-letter topic (clicks-dlq) in a later step.
            invalid.append((msg, reason))
            continue
        rows.append(to_row(event, msg.partition(), msg.offset()))
    return rows, invalid


class ClickConsumer:
    """Batch loop: consume -> validate -> insert into ClickHouse -> commit offsets."""

    def __init__(
        self, settings: Settings, sink: ClickHouseSink, consumer: Consumer | None = None
    ) -> None:
        """Use the injected consumer, or create one from settings and subscribe to clicks."""
        self.settings = settings
        self.sink = sink
        if consumer is None:
            consumer = Consumer(settings.consumer_config())
            consumer.subscribe([settings.clicks_topic])
        self.consumer = consumer

    def run(self, stop_event: threading.Event) -> None:
        """Process batches until stop_event is set; re-raises if a batch cannot be stored."""
        while not stop_event.is_set():
            messages = self.consumer.consume(
                num_messages=self.settings.batch_max_size,
                timeout=self.settings.batch_max_wait_ms / 1000,
            )
            if not messages:
                continue
            rows, invalid = process_batch(messages)
            self.sink.insert_rows(rows)  # raises on failure -> no commit below
            # Commit only AFTER the insert succeeded. This gives at-least-once delivery:
            # if we crash between insert and commit, the batch is redelivered and
            # re-inserted, and idempotent storage (dedup tokens + ReplacingMergeTree on
            # event_id, ADR-0003) removes the duplicates. Committing first could lose
            # events that were acknowledged in Kafka but never stored.
            self.consumer.commit(asynchronous=False)
            log.debug("committed batch: %d rows, %d invalid", len(rows), len(invalid))

    def close(self) -> None:
        """Close the Kafka consumer (leaves the group cleanly)."""
        self.consumer.close()
