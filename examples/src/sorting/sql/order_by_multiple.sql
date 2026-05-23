-- ORDER BY with multiple columns.
SELECT
    id,
    name,
    email
FROM users
ORDER BY name ASC, created_at DESC
