-- Click Analytics: unique clicks by country ('ZZ' = unknown / no GeoIP database).
SELECT
  country,
  uniqExact(event_id) AS clicks,
  round(100 * clicks / sum(clicks) OVER (), 2) AS share_pct
FROM analytics.clicks
WHERE $__timeFilter(event_time)
  AND $__conditionalAll(short_code IN (${short_code:singlequote}), $short_code)
GROUP BY country
ORDER BY clicks DESC
