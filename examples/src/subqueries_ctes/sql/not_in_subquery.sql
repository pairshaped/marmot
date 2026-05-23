-- NOT IN (subquery): exclude rows matching the subquery result.
SELECT
    id,
    name
FROM users
WHERE id NOT IN (
    SELECT DISTINCT user_id FROM posts
    WHERE published_at IS NULL
)
