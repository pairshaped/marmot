-- INNER JOIN: returns only rows with matches in both tables.
-- Generated function: inner_join_posts_with_users(db:) -> Result(List(InnerJoinPostsWithUsersRow), ...)
SELECT p.id, p.title, u.name AS author_name
FROM posts p
INNER JOIN users u ON p.user_id = u.id
