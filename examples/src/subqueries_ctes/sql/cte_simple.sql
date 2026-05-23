-- Simple CTE (Common Table Expression) using WITH.
-- Useful for breaking complex queries into named, readable pieces.
WITH active_users AS (
    SELECT
        id,
        name
    FROM users
    WHERE active = 1
)

SELECT
    au.name,
    COUNT(p.id) AS post_count
FROM active_users AS au
LEFT JOIN posts AS p ON au.id = p.user_id
GROUP BY au.id, au.name
