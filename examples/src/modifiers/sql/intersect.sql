-- INTERSECT: return rows that appear in both SELECTs.
SELECT user_id FROM posts
INTERSECT
SELECT id FROM users WHERE active = 1
