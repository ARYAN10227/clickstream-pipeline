-- Click Analytics: values for the $short_code template variable (200 busiest codes in range).
SELECT short_code
FROM analytics.clicks
WHERE $__timeFilter(event_time)
GROUP BY short_code
ORDER BY uniqExact(event_id) DESC
LIMIT 200
