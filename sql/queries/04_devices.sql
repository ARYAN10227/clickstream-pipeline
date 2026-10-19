-- Click Analytics: unique clicks by device class.
SELECT
  device,
  uniqExact(event_id) AS clicks
FROM analytics.clicks
WHERE $__timeFilter(event_time)
  AND $__conditionalAll(short_code IN (${short_code:singlequote}), $short_code)
GROUP BY device
ORDER BY clicks DESC
