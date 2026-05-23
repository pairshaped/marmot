-- RANK() and DENSE_RANK(): assign ranks with gaps (RANK) or without (DENSE_RANK).
-- Both infer as IntType (non-nullable).
SELECT
    id,
    title,
    RANK() OVER (ORDER BY published_at DESC) AS rank,
    DENSE_RANK() OVER (ORDER BY published_at DESC) AS dense_rank
FROM posts
WHERE published_at IS NOT NULL
