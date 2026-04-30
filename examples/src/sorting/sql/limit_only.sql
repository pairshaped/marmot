-- LIMIT with anonymous ? parameter (also forced to IntType).
SELECT id, title
FROM posts
ORDER BY id DESC
LIMIT ?
