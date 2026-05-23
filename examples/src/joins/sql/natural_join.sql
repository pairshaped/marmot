-- NATURAL JOIN: auto-joins on all columns with matching names.
SELECT
    u.name,
    p.title
FROM users AS u
NATURAL JOIN posts AS p
