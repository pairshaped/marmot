-- EXISTS(subquery): returns 1 if the subquery returns any rows, 0 otherwise.
-- EXISTS infers as IntType (non-nullable).
SELECT id, name
FROM users u
WHERE EXISTS (
  SELECT 1 FROM posts p WHERE p.user_id = u.id AND p.published_at IS NOT NULL
)
