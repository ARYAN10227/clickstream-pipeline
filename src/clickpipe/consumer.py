"""Consume click events from Kafka in batches and write them to ClickHouse.

Delivery is at-least-once (ADR-0002): offsets are committed only after valid rows are
stored AND invalid records are confirmed in the dead-letter topic (ADR-0005), so a crash
causes redelivery, never loss.
"""

import logging
import threading
import time
from collections.abc import Sequence
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

from clickpipe.config import Settings
from clickpipe.dedup import build_dedup_token
from clickpipe.dlq import DlqProducer
from clickpipe.schema import DlqReason, Invalid, to_row, validate
from clickpipe.sink import ClickHouseSink

log = logging.getLogger(__name__)


def _describe(partitions: list[TopicPartition]) -> str:
    """Format partitions as 'topic[0,1,2]' for logs."""
    by_topic: dict[str, list[int]] = {}
    for tp in partitions:
        by_topic.setdefault(tp.topic, []).append(tp.partition)
    return ", ".join(f"{t}{sorted(ps)}" for t, ps in sorted(by_topic.items())) or "none"


def process_batch(
    messages: Sequence[Message], now_ms: int, late_threshold_s: int, future_skew_s: int
) -> tuple[list[tuple[Any, ...]], list[tuple[Message, DlqReason, str]]]:
    """Split a polled batch into ClickHouse rows and (message, reason, error) for invalid ones."""
    rows: list[tuple[Any, ...]] = []
    invalid: list[tuple[Message, DlqReason, str]] = []
    for msg in messages:
        err = msg.error()
        if err is not None:
            if err.code() != KafkaError._PARTITION_EOF:
                log.warning("skipping Kafka error message: %s", err)
            continue
        result = validate(msg.value() or b"", now_ms, late_threshold_s, future_skew_s)
        if isinstance(result, Invalid):
            log.warning(
                "invalid record %s[%d]@%d: %s: %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                result.reason.value,
                result.error,
            )
            invalid.append((msg, result.reason, result.error))
            continue
        rows.append(to_row(result.event, msg.partition(), msg.offset()))
    return rows, invalid


class ClickConsumer:
    """Batch loop: consume -> validate -> insert rows -> send + flush DLQ -> commit offsets."""

    def __init__(
        self,
        settings: Settings,
        sink: ClickHouseSink,
        dlq: DlqProducer,
        consumer: Consumer | None = None,
    ) -> None:
        """Use the injected consumer, or create one from settings and subscribe to clicks."""
        self.settings = settings
        self.sink = sink
        self.dlq = dlq
        if consumer is None:
            consumer = Consumer(settings.consumer_config())
            consumer.subscribe(
                [settings.clicks_topic], on_assign=self._on_assign, on_revoke=self._on_revoke
            )
        self.consumer = consumer

    # Rebalance safety: run() inserts, DLQ-flushes and commits each batch synchronously
    # before it polls again, and these callbacks only fire inside consume(). So when
    # partitions are revoked there is never a half-processed batch: no uninserted work to
    # lose and no offsets to commit. Nothing needs flushing here; we only log.
    @staticmethod
    def _on_assign(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("partitions assigned: %s", _describe(partitions))

    @staticmethod
    def _on_revoke(_consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("partitions revoked: %s", _describe(partitions))

    def run(self, stop_event: threading.Event) -> None:
        """Process batches until stop_event is set; re-raises if a batch cannot be finished.

        stop_event is only checked between batches, so a batch already polled is always
        finished (insert, DLQ flush, commit) before the loop exits.
        """
        while not stop_event.is_set():
            messages = self.consumer.consume(
                num_messages=self.settings.batch_max_size,
                timeout=self.settings.batch_max_wait_ms / 1000,
            )
            if not messages:
                continue
            rows, invalid = process_batch(
                messages,
                now_ms=time.time_ns() // 1_000_000,
                late_threshold_s=self.settings.late_threshold_s,
                future_skew_s=self.settings.future_skew_s,
            )
            # Any exception below propagates before the commit, so the batch is redelivered.
            if rows:
                # Token from the messages that produced rows: a retry of this exact batch
                # gets the same token and ClickHouse skips the duplicate insert (ADR-0003).
                invalid_ids = {id(msg) for msg, _, _ in invalid}
                valid = [m for m in messages if m.error() is None and id(m) not in invalid_ids]
                token = build_dedup_token(self.settings.clicks_topic, valid)
                self.sink.insert_rows(rows, dedup_token=token)
            for msg, reason, error in invalid:
                self.dlq.send(msg, reason, error)
            if invalid:
                self.dlq.flush()  # raises DlqFlushError unless every DLQ record is delivered
            # Commit only AFTER rows are stored and DLQ records are confirmed. This gives
            # at-least-once delivery: if we crash before the commit, the batch is redelivered
            # and re-processed; idempotent storage (dedup tokens + ReplacingMergeTree on
            # event_id, ADR-0003) removes duplicate rows, and a duplicated DLQ record is
            # acceptable for an error stream (ADR-0005). Committing first could lose events.
            try:
                self.consumer.commit(asynchronous=False)
            except KafkaException as exc:
                # E.g. the group rebalanced and these partitions moved to another consumer.
                # The rows and DLQ records are already stored, so this is safe to skip: the
                # batch will be delivered again and idempotent writes absorb the duplicates.
                log.warning("offset commit failed, batch will be redelivered: %s", exc)
                continue
            log.debug("committed batch: %d rows, %d to DLQ", len(rows), len(invalid))

    def close(self) -> None:
        """Close the Kafka consumer (leaves the group cleanly)."""
        self.consumer.close()
