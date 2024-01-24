-- 5 Analiza potrošǌe korisnika po kategorijama

SELECT 
    ub.category_id,
    ub.user_id,
    SUM(ub.price) AS total_spent
FROM user_behavior ub
GROUP BY ub.category_id, ub.user_id
ORDER BY total_spent DESC;