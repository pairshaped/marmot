-- Subquery in the WHERE clause using IN.
SELECT id, title, body
FROM posts
WHERE user_id IN (
  SELECT id FROM users WHERE active = @active
)
