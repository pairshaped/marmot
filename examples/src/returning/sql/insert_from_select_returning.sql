-- INSERT from SELECT with RETURNING: insert rows from a subquery and
-- return the inserted rows.
INSERT INTO archived_posts (id, title, user_id)
SELECT id, title, user_id FROM posts WHERE published_at <
@cutoff
RETURNING id, title
