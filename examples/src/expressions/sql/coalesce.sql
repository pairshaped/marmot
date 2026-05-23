-- COALESCE: return the first non-NULL argument.
-- If all arguments are NULL, returns NULL (makes the column nullable).
-- COALESCE with a literal fallback makes the column non-nullable.
SELECT
    id,
    title,
    COALESCE(published_at, 0) AS published_at_or_zero,
    COALESCE(body, '(no content)') AS body_or_placeholder
FROM posts
