-- CROSS JOIN: cartesian product of both tables (no ON clause).
SELECT
    u.name,
    t.name AS tag_name
FROM users AS u
CROSS JOIN tags AS t
