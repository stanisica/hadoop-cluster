-- 7. Rangiranje proizvoda po prodaji za svaki mesec

SELECT 
    ew.year, 
    ew.month, 
    ew.product_id, 
    SUM(ew.price) AS total_sales,
    RANK() OVER (PARTITION BY ew.year, ew.month ORDER BY SUM(ew.price) DESC) as sales_rank
FROM event_window ew
WHERE ew.event_type = 'purchase'
GROUP BY ew.year, ew.month, ew.product_id;
