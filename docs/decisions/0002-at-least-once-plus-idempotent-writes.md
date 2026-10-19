# ADR-0002: At-least-once delivery plus idempotent writes

Status: accepted

## Context
The consumer must not lose events on crashes and must not double-count them on replays.
The sink is ClickHouse, an external system outside Kafka.

## Decision
Commit offsets manually, only after the ClickHouse insert and the DLQ flush both succeed.
A crash between insert and commit causes redelivery, which idempotent writes make
harmless.

## Rejected alternative
Kafka transactions (exactly-once). They cover Kafka-to-Kafka flows and cannot make an
external ClickHouse write atomic with the offset commit.

## Consequences
- No event is lost: uncommitted offsets are always redelivered.
- Duplicates can reach ClickHouse and must be removed by storage (ADR-0003).
