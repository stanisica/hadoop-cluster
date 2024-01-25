-- 7. Godišnji rast prodaje po kategoriji

SELECT 
    year, 
    category_id,
    SUM(price) AS total_sales,
    (SUM(price) - LAG(SUM(price), 1, 0) OVER (PARTITION BY category_id ORDER BY year)) / LAG(SUM(price), 1, 1) OVER (PARTITION BY category_id ORDER BY year) * 100 AS year_over_year_growth
FROM event_window
GROUP BY year, category_id
