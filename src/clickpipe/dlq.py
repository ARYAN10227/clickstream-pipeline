"""Dead-letter producer: forwards rejected records to the DLQ topic (ADR-0005).

The original value, key and headers are kept byte-for-byte so records can be replayed
after a fix; the failure reason and source position are added as headers.
"""

import time

from confluent_kafka import KafkaError, Message, Producer

from clickpipe.config import Settings
from clickpipe.schema import DlqReason


class DlqFlushError(Exception):
    """DLQ records could not be confirmed as delivered; offsets must not be committed."""


class DlqProducer:
    """Produces invalid records to the dead-letter topic and confirms delivery on flush."""

    def __init__(self, settings: Settings, producer: Producer | None = None) -> None:
        """Use the injected producer, or create one from settings.producer_config()."""
        self.topic = settings.dlq_topic
        self.producer = producer or Producer(settings.producer_config())
        self._failures: list[str] = []

    def _on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is not None:
            self._failures.append(str(err))

    def send(self, message: Message, reason: DlqReason, error: str) -> None:
        """Queue the original record for the DLQ with reason and source headers added."""
        headers: list[tuple[str, bytes]] = list(message.headers() or [])
        headers += [
            ("dlq_reason", reason.value.encode()),
            ("error", error.encode()),
            ("source_topic", message.topic().encode()),
            ("source_partition", str(message.partition()).encode()),
            ("source_offset", str(message.offset()).encode()),
            ("failed_at", str(time.time_ns() // 1_000_000).encode()),
        ]
        while True:
            try:
                self.producer.produce(
                    self.topic,
                    value=message.value(),
                    key=message.key(),
                    headers=headers,
                    on_delivery=self._on_delivery,
                )
                return
            except BufferError:
                self.producer.poll(0.1)  # local queue full: serve callbacks, then retry

    def flush(self, timeout_s: float = 10.0) -> None:
        """Wait for delivery; raise DlqFlushError if anything is unsent or failed."""
        remaining = self.producer.flush(timeout_s)
        failures, self._failures = self._failures, []
        if remaining:
            raise DlqFlushError(f"{remaining} DLQ message(s) still queued after {timeout_s}s")
        if failures:
            raise DlqFlushError(f"{len(failures)} DLQ delivery failure(s), first: {failures[0]}")

    def close(self) -> None:
        """Flush any queued DLQ records."""
        self.flush()
