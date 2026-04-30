-- Nested CASE with COALESCE: expressions can be composed.
SELECT
  id,
  title,
  COALESCE(
    CASE
      WHEN published_at IS NULL THEN 'unpublished'
      ELSE NULL
    END,
    'unknown'
  ) AS status
FROM posts
