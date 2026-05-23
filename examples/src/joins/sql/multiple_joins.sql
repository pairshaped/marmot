-- Multiple chained joins across three tables.
-- posts -> users (author), posts -> comments -> users (commenter)
SELECT
    p.title,
    u.name AS author,
    c.body AS comment,
    cu.name AS commenter
FROM posts AS p
INNER JOIN users AS u ON p.user_id = u.id
LEFT JOIN comments AS c ON p.id = c.post_id
LEFT JOIN users AS cu ON c.user_id = cu.id
