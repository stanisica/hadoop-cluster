-- 3. Korisnici koji su najviše potrošili u svakoj kategoriji

SELECT category_id, user_id, RANK() OVER (PARTITION BY category_id ORDER BY SUM(price) DESC) as rank
FROM user_behavior
GROUP BY category_id, user_id