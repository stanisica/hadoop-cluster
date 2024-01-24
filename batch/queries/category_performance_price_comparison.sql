-- 9. Poređenje uspešnosti kategorija spram cene

SELECT 
    CASE 
        WHEN price < 200 THEN 'Less than 200e'
        WHEN price BETWEEN 200 AND 400 THEN '200e-400e'
        ELSE 'More than 400e'
    END AS price_category,
    COUNT(*) AS number_of_sales
FROM event_window 
WHERE event_type = 'purchase'
GROUP BY 
    CASE 
        WHEN price < 200 THEN 'Less than 200e'
        WHEN price BETWEEN 200 AND 400 THEN '200e-400e'
        ELSE 'More than 400e'
    END