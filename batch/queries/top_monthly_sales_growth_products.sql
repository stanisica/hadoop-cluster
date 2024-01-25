-- 6. Proizvodi sa najvećim rastom u prodaji u poslednjem mesecu

SELECT product_id,
       SUM(price) AS total_sales,
       LAG(SUM(price), 1) OVER (ORDER BY DATE_FORMAT(event_time, 'yyyy-MM')) AS previous_month_sales
FROM event_window 
WHERE DATE_FORMAT(event_time, 'yyyy-MM') = 'recent_month'
GROUP BY product_id