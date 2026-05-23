-- ROW_NUMBER(): assigns sequential numbers to rows within a partition.
-- ROW_NUMBER() infers as IntType (non-nullable).
SELECT
    ROW_NUMBER() OVER (ORDER BY published_at DESC) AS rank_order,
    id,
    title,
    published_at
FROM posts
