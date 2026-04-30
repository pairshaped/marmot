-- ROW_NUMBER() with PARTITION BY: numbering resets per group.
SELECT
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY published_at DESC) AS post_num,
  user_id,
  id,
  title
FROM posts
