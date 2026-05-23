-- ROW_NUMBER(): assigns sequential numbers to rows within a partition.
-- ROW_NUMBER() infers as IntType (non-nullable).
SELECT
    id,
    title,
    published_at,
    ROW_NUMBER() OVER (ORDER BY published_at DESC) AS rank_order
FROM posts
