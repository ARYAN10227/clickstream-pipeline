# ADR-0003: Two-layer deduplication

Status: accepted

## Context
At-least-once delivery (ADR-0002) means duplicates can reach ClickHouse: retried batches,
replays, producer retries and consumer restarts with different batch boundaries.

## Decision
1. Set insert_deduplication_token from each batch's partition/offset ranges plus a hash of
   the batch's event_ids, with non_replicated_deduplication_window enabled, so a retried
   identical batch is skipped. ClickHouse skips any insert whose token it has seen,
   whatever the rows contain; the event_id hash keeps new data at reused offsets (topic
   recreated, broker data lost) from being dropped silently.
2. Use ReplacingMergeTree with event_id in the sorting key, which collapses duplicates
   from replays, producer retries or restarts with different batch boundaries.

## Rejected alternative
A Redis or in-memory "seen IDs" set. It is lost on crash, grows without bound, and moves
correctness out of the storage layer.

## Consequences
- Dedup happens at merge time, so correct counts must use FINAL or uniqExact(event_id),
  never plain count().
