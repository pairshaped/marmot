-- Named parameters using @name, :name, and $name syntax.
-- All three forms are equivalent and produce the same argument name.
-- Named parameters can appear multiple times: Marmot deduplicates them.
SELECT
    id,
    title,
    body
FROM posts
WHERE
    user_id
    = @user_id
    AND published_at >= :since
    AND title LIKE $pattern
