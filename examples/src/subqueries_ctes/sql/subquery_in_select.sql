-- Scalar subquery in the SELECT list, wrapped in CAST for correct type inference.
-- Each row gets the count of comments for that post.
SELECT
    p.id,
    p.title,
    CAST((
        SELECT COUNT(*) FROM comments AS c
        WHERE c.post_id = p.id
    ) AS INTEGER)
        AS comment_count
FROM posts AS p
