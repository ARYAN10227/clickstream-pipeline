# ADR-0005: Dead-letter topic

Status: accepted

## Context
Some records fail validation (bad_json, schema_invalid, missing_field) or the time rules
(late_event, future_event). They must not be silently lost and should be replayable once
the cause is fixed.

## Decision
Send invalid, late and future events to topic clicks-dlq with the original bytes and
headers dlq_reason, error, source_topic, source_partition, source_offset.
The DLQ producer is flushed BEFORE offsets are committed.

## Rejected alternative
- Log-and-drop: silent loss.
- A ClickHouse errors table: loses the original bytes and is harder to replay after a fix.

## Consequences
- Bad data is never lost and can be replayed.
- A crash between DLQ flush and commit may duplicate DLQ records, which is acceptable for
  an error stream.
