# ADR-0001: Custom Python consumer

Status: accepted

## Context
Click events in Redpanda topic `clicks` must reach ClickHouse table `analytics.clicks`.
The pipeline needs DLQ routing with per-record failure reasons, late/future event rules,
and offsets committed only after a successful insert. All of this must be testable.

## Decision
Write a custom Python consumer using confluent-kafka for consuming and producing and
clickhouse-connect for inserts.

## Rejected alternative
ClickHouse Kafka table engine + materialized view. It is faster to set up, but DLQ
routing, per-record failure reasons, late-event rules and commit-after-insert become
opaque settings that are hard to test.

## Consequences
- More code to own and maintain.
- Around 10x scale (200k events/s), Python validation is the ceiling. Scale out by
  running more consumer processes in group `clickpipe`, up to the partition count.
