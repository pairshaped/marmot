-- FILTER (WHERE condition) on aggregate functions.
-- Counts only rows matching the filter within each group.
SELECT
    u.id,
    u.name,
    COUNT(*) AS total_posts,
    COUNT(*) FILTER (WHERE p.published_at IS NOT NULL) AS published_posts
FROM users AS u
LEFT JOIN posts AS p ON u.id = p.user_id
GROUP BY u.id, u.name
