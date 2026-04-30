-- Searched CASE: evaluates conditions in order, returns the THEN value
-- for the first true condition. Type is inferred from THEN/ELSE branches.
-- NULL branches or missing ELSE make the result nullable.
SELECT
  id,
  title,
  published_at,
  CASE
    WHEN published_at IS NULL THEN 'draft'
    WHEN published_at > @cutoff THEN 'recent'
    ELSE 'archived'
  END AS post_status
FROM posts
