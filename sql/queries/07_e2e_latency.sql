-- Click Analytics: end-to-end latency (produced_at -> ingested_at) p50/p95/p99 in ms, per minute.
SELECT
  time,
  q[1] AS p50_ms,
  q[2] AS p95_ms,
  q[3] AS p99_ms
FROM
(
  SELECT
    toStartOfMinute(event_time) AS time,
    quantiles(0.5, 0.95, 0.99)(dateDiff('millisecond', produced_at, ingested_at)) AS q
  FROM analytics.clicks
  WHERE $__timeFilter(event_time)
    AND $__conditionalAll(short_code IN (${short_code:singlequote}), $short_code)
  GROUP BY time
)
ORDER BY time
