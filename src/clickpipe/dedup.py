"""Deterministic insert deduplication tokens for consumer batches (ADR-0003, layer 1)."""

import hashlib
from collections.abc import Iterable
from typing import Protocol
from uuid import UUID


class _HasPosition(Protocol):
    def partition(self) -> int: ...
    def offset(self) -> int: ...


def build_dedup_token(
    topic: str, messages: Iterable[_HasPosition], event_ids: Iterable[UUID]
) -> str:
    """Return a token identifying exactly which messages and events a batch inserts.

    Format: "<topic>|p<partition>:<min offset>-<max offset>|...|h:<hash>" with one segment
    per partition, sorted by partition number, e.g. "clicks|p0:100-199|p3:5-80|h:9f2c...".
    The hash covers the batch's event_ids (sorted). The token depends only on which
    positions and events the batch holds, not on their order.

    Pass it to ClickHouse as the insert_deduplication_token setting:

    - Retry of the same batch: if an insert fails (or its result is unknown) and the
      identical batch is inserted again, it produces the same token, so ClickHouse
      recognises the block and skips the duplicate insert
      (needs non_replicated_deduplication_window on the table).
    - Restart or rebalance: the batch is re-consumed but may start or end at different
      offsets, which gives a different token, so ClickHouse inserts it. Those duplicate
      rows are removed later by ReplacingMergeTree on event_id (ADR-0003, layer 2);
      read counts with FINAL or uniqExact(event_id).
    - Offsets reused for different data (topic recreated, broker data lost): ClickHouse
      skips any insert whose token it has seen, whatever the rows contain. The event_id
      hash keeps such a batch's token different, so the new data is not silently dropped.

    Raises ValueError for an empty batch.
    """
    ranges: dict[int, tuple[int, int]] = {}
    for msg in messages:
        partition, offset = msg.partition(), msg.offset()
        low, high = ranges.get(partition, (offset, offset))
        ranges[partition] = (min(low, offset), max(high, offset))
    if not ranges:
        raise ValueError("cannot build a dedup token for an empty batch")
    digest = hashlib.blake2b(digest_size=16)
    for event_id in sorted(event_ids):
        digest.update(event_id.bytes)
    segments = (f"p{p}:{low}-{high}" for p, (low, high) in sorted(ranges.items()))
    return "|".join((topic, *segments, f"h:{digest.hexdigest()}"))
