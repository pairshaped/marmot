-- RIGHT JOIN: left-side columns become nullable.
SELECT c.id, c.body, p.title
FROM posts p
RIGHT JOIN comments c ON p.id = c.post_id
