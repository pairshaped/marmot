-- INSERT with RETURNING *: returns all columns from the inserted row.
-- Marmot expands * to all table columns via PRAGMA table_info.
INSERT INTO users (name, email, created_at)
VALUES (@name, @email, @created_at)
RETURNING *
