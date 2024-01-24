-- 2. Mesečni Trend Prodaje po Kategoriji

SELECT year, month, category_id, SUM(price) AS total_sales, COUNT(DISTINCT product_id) AS unique_products_sold
FROM event_window 
GROUP BY year, month, category_id
ORDER BY year, month