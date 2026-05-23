-- COUNT(*) with GROUP BY: count posts per user.
-- COUNT infers as IntType (non-nullable).
SELECT
    u.id,
    u.name,
    COUNT(*) AS post_count
FROM users AS u
LEFT JOIN posts AS p ON u.id = p.user_id
GROUP BY u.id, u.name
