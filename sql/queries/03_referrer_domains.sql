-- Click Analytics: unique clicks by referrer domain ('' = direct traffic).
SELECT
  if(referrer_domain = '', '(direct)', referrer_domain) AS referrer,
  uniqExact(event_id) AS clicks
FROM analytics.clicks
WHERE $__timeFilter(event_time)
  AND $__conditionalAll(short_code IN (${short_code:singlequote}), $short_code)
GROUP BY referrer
ORDER BY clicks DESC
LIMIT 20
