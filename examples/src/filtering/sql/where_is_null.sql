-- WHERE with IS NULL and IS NOT NULL.
-- Useful for finding rows with missing optional data.
SELECT id, title
FROM posts
WHERE published_at IS NULL

-- Also works with NOT:
-- SELECT id, title FROM posts WHERE body IS NOT NULL
