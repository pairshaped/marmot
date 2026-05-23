-- UNION ALL: combine results from two SELECTs, keeping duplicates.
-- Faster than UNION since no deduplication step is needed.
SELECT id, title, published_at
FROM posts
WHERE published_at IS NOT NULL
UNION ALL
SELECT id, title, published_at
FROM posts
WHERE user_id =
@user_id
