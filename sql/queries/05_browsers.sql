-- Click Analytics: unique clicks by browser family.
SELECT
  browser,
  uniqExact(event_id) AS clicks
FROM analytics.clicks
WHERE $__timeFilter(event_time)
  AND $__conditionalAll(short_code IN (${short_code:singlequote}), $short_code)
GROUP BY browser
ORDER BY clicks DESC
LIMIT 15
