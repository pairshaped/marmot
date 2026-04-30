-- Subquery wrapping a compound query.
SELECT name FROM (
  SELECT name FROM users WHERE active = 1
  UNION ALL
  SELECT name FROM users WHERE created_at > @since
)
ORDER BY name
