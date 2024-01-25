-- 10. Top 50 proizvoda koji su najčešće dodavani u korpu bez kupovine

SELECT 
    ub.product_id,
    COUNT(CASE WHEN ub.event_type = 'cart' THEN 1 END) - COUNT(CASE WHEN ub.event_type = 'purchase' THEN 1 END) as cart_without_purchase
FROM user_behavior ub
GROUP BY ub.product_id
ORDER BY cart_without_purchase DESC
LIMIT 50;