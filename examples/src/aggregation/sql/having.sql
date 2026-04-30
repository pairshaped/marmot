-- GROUP BY with HAVING: filter groups after aggregation.
-- HAVING parameters are inferred from usage context.
SELECT u.id, u.name, COUNT(*) AS post_count
FROM users u
LEFT JOIN posts p ON u.id = p.user_id
GROUP BY u.id, u.name
HAVING COUNT(*) >= @min_posts
