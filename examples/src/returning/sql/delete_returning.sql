-- DELETE with RETURNING: delete and return the removed rows.
DELETE FROM users
WHERE active = 0
RETURNING id, name, email
