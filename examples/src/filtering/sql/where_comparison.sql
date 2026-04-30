-- WHERE with comparison operators: =, !=, <, >, <=, >=
SELECT id, title, published_at
FROM posts
WHERE published_at >= @since
  AND published_at <= @until
  AND user_id != @exclude_user_id
