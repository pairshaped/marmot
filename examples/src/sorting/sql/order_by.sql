-- ORDER BY: sort results ascending (ASC, the default) or descending (DESC).
SELECT
    id,
    title,
    published_at
FROM posts
ORDER BY published_at DESC
