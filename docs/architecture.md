# Architecture: delivery semantics and failure scenarios

The pipeline gives **at-least-once delivery with idempotent storage**. A message is never
acknowledged (offset committed) before its outcome is durable. Anything delivered twice is
removed by deduplication. ([ADR-0002](decisions/0002-at-least-once-plus-idempotent-writes.md),
[ADR-0003](decisions/0003-two-layer-deduplication.md))

## Data path

1. **Produce.** The URL shortener ([Node hook](../integrations/node-producer/clickProducer.js))
   and the tools ([loadgen](../tools/loadgen.py), [replay](../tools/replay.py)) send one JSON
   event per click to topic `clicks`, keyed by `short_code`.
   - Each event carries a producer-assigned UUID `event_id`, and `produced_at` is set right
     before sending.
   - Both producers are idempotent with `acks=all` (`enable.idempotence` in
     [config.py](../src/clickpipe/config.py), `idempotent: true` in the Node hook).
2. **Consume.** [`ClickConsumer.run`](../src/clickpipe/consumer.py) polls a batch (up to
   `BATCH_MAX_SIZE` messages or `BATCH_MAX_WAIT_MS`). Auto-commit is off.
3. **Validate.** [`validate()`](../src/clickpipe/schema.py) turns each message into either a
   valid event or an `Invalid(reason, error)` with one of `bad_json`, `missing_field`,
   `schema_invalid`, `late_event` or `future_event`. Kafka error events are skipped; partition
   EOF is ignored.
4. **Enrich.** [`to_row`](../src/clickpipe/schema.py) adds referrer domain, device, browser
   and country ([enrich.py](../src/clickpipe/enrich.py)). The IP is used only for the GeoIP
   lookup and is never stored.
5. **Insert.** [`ClickHouseSink.insert_rows`](../src/clickpipe/sink.py) writes the batch's
   valid rows into `analytics.clicks` in one insert.
   - The insert carries an `insert_deduplication_token` built from the partition and offset
     ranges of the messages that produced rows, plus a hash of those rows' `event_id`s
     ([dedup.py](../src/clickpipe/dedup.py)).
   - Transient errors are retried with capped exponential backoff and jitter.
6. **Dead-letter.** [`DlqProducer`](../src/clickpipe/dlq.py) sends each invalid record to
   `clicks-dlq` with the original value, key and headers, plus `dlq_reason`, `error`,
   `source_topic`, `source_partition`, `source_offset` and `failed_at`. It then flushes and
   raises if anything is undelivered.
7. **Commit.** Only after steps 5 and 6 have both succeeded does the consumer commit offsets
   synchronously.

The table is a ReplacingMergeTree ordered by `(short_code, event_time, event_id)`
([schema](../sql/init/001_create_clicks.sql)). Correct reads use `FINAL` or
`uniqExact(event_id)`.

## One batch

```mermaid
sequenceDiagram
    autonumber
    participant RP as Redpanda (clicks)
    participant C as ClickConsumer
    participant CH as ClickHouse
    participant DLQ as Redpanda (clicks-dlq)

    C->>RP: consume(BATCH_MAX_SIZE, BATCH_MAX_WAIT_MS)
    RP-->>C: messages
    C->>C: validate + enrich -> rows, invalid[(msg, reason, error)]
    alt rows not empty
        C->>CH: INSERT rows SETTINGS insert_deduplication_token = "clicks|p0:a-b|p3:c-d|h:<event_id hash>"
        Note over C,CH: transient error -> retry with backoff;<br/>still failing -> raise, no commit
        CH-->>C: ok (or skipped as duplicate token)
    end
    loop each invalid record
        C->>DLQ: produce(original bytes, original headers + dlq_reason, source_*)
    end
    opt invalid not empty
        C->>DLQ: flush()
        Note over C,DLQ: anything undelivered -> DlqFlushError, no commit
    end
    C->>RP: commit(asynchronous=False)
    Note over C,RP: commit fails (e.g. rebalance) -> log warning,<br/>batch is redelivered later
```

## Failure walkthroughs

### 1. Consumer crashes before the insert

- **What happens:** the batch was polled but nothing was written and nothing was committed.
  When the consumer restarts (or another group member takes the partitions), it re-reads
  from the last committed offset and processes the same messages again.
- **What protects the data:** offsets are committed only after a successful insert and DLQ
  flush, so the committed position never moves past unstored messages.
- **Proof:**
  - [tests/unit/test_consumer.py](../tests/unit/test_consumer.py)
    `test_run_order_is_insert_dlq_send_dlq_flush_commit` and
    `test_run_does_not_send_dlq_or_commit_when_insert_fails`
  - [tests/e2e/test_crash.py](../tests/e2e/test_crash.py) and `make crash-test`
    ([tools/crash_test.py](../tools/crash_test.py)): `SIGKILL` mid-stream, then zero loss.
    The kill lands wherever the consumer happens to be, so this proves the outcome but does
    not target this exact point.

### 2. Consumer crashes after the insert but before the commit

- **What happens:** the rows are in ClickHouse but the offsets are not committed, so after
  the restart the batch is delivered and inserted again.
- **What protects the data:** two layers.
  - **Same batch boundaries:** the retried batch has the same partition/offset ranges and
    the same events, so the same dedup token, and ClickHouse skips the insert. Plain
    `count()` stays correct.
  - **Different boundaries** (for example after a restart the poll returns a different
    slice): the token differs and the rows are stored twice. ReplacingMergeTree collapses
    them on `event_id` at merge time, and `FINAL` / `uniqExact(event_id)` are correct
    immediately.
  - The token window is limited by the table's `non_replicated_deduplication_window`. Older
    retries fall through to layer 2.
  - Because the token includes a hash of the batch's `event_id`s, new data that happens to
    reuse old offsets (topic recreated, broker data lost) gets a different token and is not
    skipped.
- **Proof:**
  - [tests/integration/test_idempotency.py](../tests/integration/test_idempotency.py)
    `test_same_token_skips_duplicate_insert` (plain `count()` unchanged),
    `test_different_token_duplicates_removed_by_replacing_merge_tree` and
    `test_reused_offsets_with_new_events_are_not_dropped`
  - [tests/unit/test_consumer.py](../tests/unit/test_consumer.py)
    `test_run_passes_dedup_token_from_valid_messages_only`
  - [tests/e2e/test_crash.py](../tests/e2e/test_crash.py): zero duplicates after kill -9

### 3. ClickHouse is down or erroring

- **What happens:**
  - The insert fails, and [`insert_rows`](../src/clickpipe/sink.py) retries transient
    errors with backoff: connection, timeout and transport errors, plus temporary
    ClickHouse codes such as memory limit or too many parts.
  - Non-transient errors (for example an unknown column) are raised immediately.
  - When retries run out, the exception propagates. The batch is **not committed**,
    [`main.py`](../src/clickpipe/main.py) logs it and exits with code 1, and the compose
    `restart: unless-stopped` policy restarts the consumer.
  - On restart the uncommitted messages are delivered again. If ClickHouse is still down at
    startup, the consumer exits again with a one-line error until it is back.
- **What protects the data:** no commit without a successful insert. Retrying an insert with
  the same token is harmless.
- **Proof:** [tests/unit/test_retry.py](../tests/unit/test_retry.py):
  - `test_transient_failures_are_retried_with_growing_delays`
  - `test_gives_up_after_insert_max_retries_attempts`
  - `test_non_transient_error_is_raised_immediately`
  - `test_consumer_never_commits_when_insert_finally_fails`

  Also [tests/unit/test_consumer.py](../tests/unit/test_consumer.py)
  `test_run_does_not_send_dlq_or_commit_when_insert_fails`.

### 4. Poison message (unparseable, invalid, late or future)

- **What happens:** the message does not block the partition. `validate()` classifies it,
  and the consumer sends the original bytes and headers to `clicks-dlq` with `dlq_reason`,
  a redacted, length-capped `error` and the source position. Valid messages in the same
  batch are inserted normally. The batch is committed only after the DLQ flush confirms
  delivery; if the flush fails, nothing is committed and the batch is retried. A crash
  between the DLQ flush and the commit can duplicate DLQ records, which is acceptable for
  an error stream ([ADR-0005](decisions/0005-dead-letter-topic.md)).
- **What protects the data:** the DLQ keeps every bad record and it is replayable. Error
  text never contains the event's IP.
- **Proof:**
  - [tests/unit/test_validate.py](../tests/unit/test_validate.py): every reason code,
    every missing required field, the late/future boundaries, and no IP in error text
  - [tests/unit/test_dlq.py](../tests/unit/test_dlq.py): original bytes and headers, all
    DLQ headers, and flush failures
  - [tests/unit/test_consumer.py](../tests/unit/test_consumer.py)
    `test_run_does_not_commit_when_dlq_flush_fails` and
    `test_run_only_invalid_batch_skips_insert_then_commits`
  - [tests/integration/test_dlq_routing.py](../tests/integration/test_dlq_routing.py): real
    brokers and ClickHouse, the right count per reason, and `source_offset` on every record
  - `make replay` ([tests/e2e/test_replay.py](../tests/e2e/test_replay.py))

### 5. Producer retry creates a duplicate

- **What happens:**
  - **Network-level retries are absorbed by Kafka.** If a produce request times out and the
    client retries it, the idempotent producer (`enable.idempotence`, `acks=all`) lets the
    broker drop the retried copy.
  - **Application-level resends are new messages.** If an application sends the same event
    again (for example a retried HTTP request or a replay), it is a new message with the
    **same `event_id`**.
- **What protects the data:** for application-level duplicates, ReplacingMergeTree keyed on
  `event_id`, read with `FINAL` or `uniqExact(event_id)`. These duplicates usually land in a
  different batch, so the insert token does not catch them.
- **Proof:**
  - `make replay` ([tools/replay.py](../tools/replay.py), [tools/verify.py](../tools/verify.py),
    [tests/e2e/test_replay.py](../tests/e2e/test_replay.py)) injects byte-identical
    duplicates, some right after the original and some much later, and checks that
    `count() FINAL` and `uniqExact(event_id)` equal the number of unique events
  - [tests/integration/test_idempotency.py](../tests/integration/test_idempotency.py)
    `test_different_token_duplicates_removed_by_replacing_merge_tree`

### 6. Consumer-group rebalance

- **What happens:** partitions move between consumers, for example when replicas are added
  or removed or a member times out.
  - Each batch is inserted, DLQ-flushed and committed **before the next poll**, and the
    assign/revoke callbacks only run inside a poll. So at revoke time there is no
    half-processed batch: nothing to flush, nothing uncommitted to lose.
  - If a commit fails because the group rebalanced in the meantime, the consumer logs a
    warning and continues. That batch's rows are already stored; the new owner re-reads it
    from the last committed offset, and the duplicates are absorbed as in scenario 2.
  - On a clean stop (SIGTERM), the current batch finishes and is committed before the
    consumer leaves the group.
- **What protects the data:** synchronous commit after each fully processed batch, plus
  idempotent storage for the batch whose commit was lost.
- **Proof:**
  - [tests/unit/test_consumer.py](../tests/unit/test_consumer.py)
    `test_commit_failure_is_logged_and_loop_continues` and
    `test_stop_during_processing_finishes_current_batch_then_exits`
  - [tests/integration/test_shutdown.py](../tests/integration/test_shutdown.py)
    `test_stop_leaves_nothing_uncommitted`: committed offsets equal the high watermarks
    after a stop
  - **Gap:** no automated test triggers a live rebalance in the middle of a batch. The
    3-replica benchmark ([scaling.md](results/scaling.md)) ran with a scaled group and
    stored every event, but the group was already stable when that run started.
