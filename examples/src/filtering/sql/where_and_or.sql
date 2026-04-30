-- WHERE with AND / OR combinators and parenthesized conditions.
SELECT id, title, published_at
FROM posts
WHERE (published_at >= @since OR published_at IS NULL)
  AND (user_id = @user_id OR user_id IN (SELECT id FROM users WHERE active = 1))
