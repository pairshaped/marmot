-- JOIN ... USING: shorthand when both tables share the same column name.
-- comments and post_tags both have post_id, so USING works here.
SELECT
    c.body,
    pt.tag_id
FROM comments AS c
INNER JOIN post_tags AS pt ON c.post_id = pt.post_id
