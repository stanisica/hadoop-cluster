-- 5. Prosečna cena proizvoda pre i posle promotivnog perioda po kategoriji 

SELECT 
    category_id,
    category_code,
    AVG(CASE WHEN event_time < 'start_promotion' THEN price ELSE NULL END) AS avg_price_before_promotion,
    AVG(CASE WHEN event_time BETWEEN 'start_promotion' AND 'end_promotion' THEN price ELSE NULL END) AS avg_price_during_promotion,
    AVG(CASE WHEN event_time > 'end_promotion' THEN price ELSE NULL END) AS avg_price_after_promotion
FROM price_variation
GROUP BY category_id, category_code