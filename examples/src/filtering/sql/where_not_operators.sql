-- WHERE with NOT operators: NOT LIKE, NOT IN, NOT BETWEEN.
SELECT
    id,
    name
FROM users
WHERE
    name NOT LIKE
    @pattern
    AND id NOT IN (
        SELECT user_id FROM posts
        WHERE published_at IS NULL
    )
