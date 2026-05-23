-- LEFT JOIN with a WHERE condition on the right-side column.
-- The WHERE reduces nullability: Marmot detects this and the generated
-- column type is non-optional despite being on the right side of a LEFT JOIN.
SELECT p.id, p.title, c.body
FROM posts p
LEFT JOIN comments c ON p.id = c.post_id
WHERE c.id =
@comment_id
