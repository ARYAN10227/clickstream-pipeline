-- Click Analytics: top 10 short codes by unique clicks.
SELECT
  short_code,
  uniqExact(event_id) AS clicks
FROM analytics.clicks
WHERE $__timeFilter(event_time)
  AND $__conditionalAll(short_code IN (${short_code:singlequote}), $short_code)
GROUP BY short_code
ORDER BY clicks DESC
LIMIT 10
