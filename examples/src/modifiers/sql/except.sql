-- EXCEPT: return rows from the first SELECT that don't appear in the second.
SELECT id, name FROM users
EXCEPT
SELECT u.id, u.name FROM users u INNER JOIN posts p ON u.id = p.user_id
