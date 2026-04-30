-- Multiple chained joins across three tables.
-- posts -> users (author), posts -> comments -> users (commenter)
SELECT p.title, u.name AS author, c.body AS comment, cu.name AS commenter
FROM posts p
INNER JOIN users u ON p.user_id = u.id
LEFT JOIN comments c ON p.id = c.post_id
LEFT JOIN users cu ON c.user_id = cu.id
