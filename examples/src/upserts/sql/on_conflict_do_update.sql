-- ON CONFLICT DO UPDATE SET: update specific columns on constraint violation.
-- The WHERE clause on the DO UPDATE is optional and limits when the update fires.
INSERT INTO users (id, name, email, created_at)
VALUES (@id, @name, @email, @created_at)
ON CONFLICT (id) DO UPDATE SET
    name = @name,
    email = @email
WHERE users.active = 1
