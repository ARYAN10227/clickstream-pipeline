-- Click Analytics: clicks per minute (Grafana ClickHouse datasource SQL; uses Grafana macros).
-- uniqExact(event_id) so duplicate rows not yet merged never inflate the count.
SELECT
  toStartOfMinute(event_time) AS time,
  uniqExact(event_id) AS clicks
FROM analytics.clicks
WHERE $__timeFilter(event_time)
  AND $__conditionalAll(short_code IN (${short_code:singlequote}), $short_code)
GROUP BY time
ORDER BY time
