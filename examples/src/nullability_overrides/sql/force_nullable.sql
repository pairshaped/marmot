-- Force nullable with ? suffix on a column alias.
-- Useful when you know an INNER JOIN column might actually be NULL
-- (e.g., due to application-level soft references).
SELECT u.id, u.name, p.title? AS latest_post_title
FROM users u
INNER JOIN posts p ON u.id = p.user_id
