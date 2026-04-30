-- NATURAL JOIN: auto-joins on all columns with matching names.
SELECT u.name, p.title
FROM users u
NATURAL JOIN posts p
