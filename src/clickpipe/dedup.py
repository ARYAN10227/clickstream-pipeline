"""Deterministic insert deduplication tokens for consumer batches (ADR-0003, layer 1)."""

from collections.abc import Iterable
from typing import Protocol


class _HasPosition(Protocol):
    def partition(self) -> int: ...
    def offset(self) -> int: ...


def build_dedup_token(topic: str, messages: Iterable[_HasPosition]) -> str:
    """Return a token describing exactly which Kafka offsets a batch covers.

    Format: "<topic>|p<partition>:<min offset>-<max offset>|..." with one segment per
    partition, sorted by partition number, e.g. "clicks|p0:100-199|p3:5-80". The token
    depends only on the set of (partition, offset) positions, not on message order.

    Pass it to ClickHouse as the insert_deduplication_token setting:

    - Retry of the same batch: if an insert fails (or its result is unknown) and the
      identical batch is inserted again, it produces the same token, so ClickHouse
      recognises the block and skips the duplicate insert
      (needs non_replicated_deduplication_window on the table).
    - Restart or rebalance: the batch is re-consumed but may start or end at different
      offsets, which gives a different token, so ClickHouse inserts it. Those duplicate
      rows are removed later by ReplacingMergeTree on event_id (ADR-0003, layer 2);
      read counts with FINAL or uniqExact(event_id).

    Raises ValueError for an empty batch.
    """
    ranges: dict[int, tuple[int, int]] = {}
    for msg in messages:
        partition, offset = msg.partition(), msg.offset()
        low, high = ranges.get(partition, (offset, offset))
        ranges[partition] = (min(low, offset), max(high, offset))
    if not ranges:
        raise ValueError("cannot build a dedup token for an empty batch")
    segments = (f"p{p}:{low}-{high}" for p, (low, high) in sorted(ranges.items()))
    return "|".join((topic, *segments))
