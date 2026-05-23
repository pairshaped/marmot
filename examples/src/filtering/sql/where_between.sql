-- WHERE with BETWEEN x AND y (inclusive range).
-- Both bounds become Int-typed parameters.
SELECT
    id,
    title,
    published_at
FROM posts
WHERE
    published_at BETWEEN
    @from AND @to
