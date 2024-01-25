-- 4. Odnos pregleda i potrošenih para za svakog korisnika

SELECT 
    user_id, 
    category_id,
    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_spent_on_purchases,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS number_of_purchases,
    COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS number_of_views,
    (COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) / GREATEST(COUNT(CASE WHEN event_type = 'view' THEN 1 END), 1)) AS purchase_view_ratio
FROM user_behavior
GROUP BY user_id, category_id