-- JOIN ... USING: shorthand when both tables share the same column name.
-- comments and post_tags both have post_id, so USING works here.
SELECT c.body, pt.tag_id
FROM comments c
JOIN post_tags pt USING (post_id)
