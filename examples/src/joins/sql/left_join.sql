-- LEFT JOIN: right-side columns become nullable (Option types in Gleam).
-- A post with no comments still appears, with comment columns as None.
SELECT
    p.id,
    p.title,
    c.body AS comment_body
FROM posts AS p
LEFT JOIN comments AS c ON p.id = c.post_id
