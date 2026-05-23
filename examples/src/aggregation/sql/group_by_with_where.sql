-- GROUP BY with WHERE: filter before aggregation.
SELECT
    user_id,
    COUNT(*) AS post_count
FROM posts
WHERE published_at IS NOT NULL
GROUP BY user_id
