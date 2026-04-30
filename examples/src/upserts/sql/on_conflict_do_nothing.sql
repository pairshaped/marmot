-- ON CONFLICT DO NOTHING: skip insert on constraint violation.
INSERT INTO users (id, name, email, created_at)
VALUES (@id, @name, @email, @created_at)
ON CONFLICT (id) DO NOTHING
