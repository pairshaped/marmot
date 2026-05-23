-- WHERE with IN (subquery).
SELECT
    id,
    title
FROM posts
WHERE user_id IN (
    SELECT id FROM users
    WHERE active = @active
)
