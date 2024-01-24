-- 1. Dnevna promena broja pregleda po kategoriji

SELECT event_date, category_id,
    COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS daily_views,
    COUNT(CASE WHEN event_type = 'view' THEN 1 END) - LAG(COUNT(CASE WHEN event_type = 'view' THEN 1 END), 1, 0) 
    OVER (PARTITION BY category_id ORDER BY event_date) AS change_in_daily_views
FROM event_window 
WHERE event_type = 'view'
GROUP BY event_date, category_id
ORDER BY event_date, category_id