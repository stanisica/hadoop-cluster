-- 8. Top 10 najprodavanijih proizvoda po kategoriji

SELECT category_id, product_id, SUM(price) AS total_sales
FROM price_variation
GROUP BY category_id, product_id
ORDER BY category_id, total_sales DESC
LIMIT 10
