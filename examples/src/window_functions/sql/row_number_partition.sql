-- ROW_NUMBER() with PARTITION BY: numbering resets per group.
SELECT
    user_id,
    id,
    title,
    ROW_NUMBER()
        OVER (PARTITION BY user_id ORDER BY published_at DESC)
        AS post_num
FROM posts
