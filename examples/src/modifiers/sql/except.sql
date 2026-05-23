-- EXCEPT: return rows from the first SELECT that don't appear in the second.
SELECT
    id,
    name
FROM users
EXCEPT
SELECT
    u.id,
    u.name
FROM users AS u INNER JOIN posts AS p ON u.id = p.user_id
