-- RIGHT JOIN: left-side columns become nullable.
SELECT
    c.id,
    c.body,
    p.title
FROM posts AS p
RIGHT JOIN comments AS c ON p.id = c.post_id
