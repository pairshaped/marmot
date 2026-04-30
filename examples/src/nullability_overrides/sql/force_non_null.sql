-- Force non-nullable with ! suffix on a column alias.
-- Useful when you know a LEFT JOIN column will always have a value.
-- Marmot would normally mark comment_body as Option(String) from the LEFT JOIN,
-- but the ! override forces it to String.
SELECT p.id, p.title, c.body! AS comment_body
FROM posts p
LEFT JOIN comments c ON p.id = c.post_id
